"""
Main DataValidator class that orchestrates validation operations.
"""

from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from .config import ValidationConfig, ValidationRule
from .settings import load_config
from .state import PipelineState
from .engines import ValidationEngine, ValidationSummary, create_engine


class DataValidator:
    """
    Main data validation orchestrator.

    This class provides a high-level interface for validating data using
    YAML configuration files and multiple compute engines.
    """

    def __init__(
        self,
        config: Union[str, Path, Dict[str, Any], ValidationConfig],
        *,
        env_prefix: str = "VALIDATOR",
        use_widgets: bool = False,
    ):
        """Initialize DataValidator with configuration.

        Parameters
        ----------
        config:
            Path to YAML configuration file or existing ``ValidationConfig`` object.
        env_prefix:
            Prefix for environment variable overrides when loading configuration.
        use_widgets:
            If ``True`` attempt to merge configuration values from Databricks widgets.
        """

        if isinstance(config, (str, Path)):
            self.config = load_config(
                str(config), env_prefix=env_prefix, use_widgets=use_widgets
            )
        elif isinstance(config, dict):
            # Deprecated path: still supported for backwards compatibility
            self.config = ValidationConfig.from_dict(config)
        elif isinstance(config, ValidationConfig):
            self.config = config
        elif isinstance(config, (str, Path)) or config is None:
            self.config = load_config(str(config) if config else None)
        else:
            raise ValueError(f"Unsupported config type: {type(config)}")

        if self.config.require_sql_rules:
            self.config.validate_sql_snippets()

        self._engine: Optional[ValidationEngine] = None
        self._dqx_enabled = self.config.dqx.enabled
        self._state: Optional[PipelineState] = None
        if self.config.pipeline.state_file:
            self._state = PipelineState.load(self.config.pipeline.state_file)

    @property
    def engine(self) -> ValidationEngine:
        """Get or create validation engine."""
        if self._engine is None:
            self._engine = create_engine(self.config.engine)
        return self._engine

    def validate_table(
        self, data: Any, table_name: str, rules: Optional[List[ValidationRule]] = None
    ) -> ValidationSummary:
        """
        Validate a table using configured rules.

        Args:
            data: Data to validate (DataFrame, table name, or file path)
            table_name: Name of the table being validated
            rules: Optional list of rules to use (defaults to config rules)

        Returns:
            ValidationSummary with results
        """
        if rules is None:
            rules = self.config.get_enabled_rules(table_name)

        # Check if table is already completed and get cached results
        if self._state:
            if self._state.is_completed(table_name):
                cached_results = self._state.get_cached_results(table_name)
                if cached_results:
                    # Reconstruct ValidationSummary from cached results
                    return self._reconstruct_summary_from_cache(cached_results, table_name)
            else:
                # Mark table as started
                self._state.mark_table_started(table_name)

        with self.engine as eng:
            loaded_data = eng.load_data_with_retry(data)
            summary = eng.execute_rules(loaded_data, rules, table_name, self._state)

        # Integrate with DQX if enabled
        if self._dqx_enabled:
            summary = self._integrate_with_dqx(summary, table_name)

        # Store results and mark as completed
        if self._state:
            summary_dict = self._summary_to_dict(summary)
            self._state.store_results(table_name, summary_dict)

        return summary

    def validate_all_tables(
        self, data_sources: Dict[str, Any]
    ) -> Dict[str, ValidationSummary]:
        """
        Validate multiple tables with enhanced resumption capabilities.

        Args:
            data_sources: Dictionary mapping table names to data sources

        Returns:
            Dictionary mapping table names to validation summaries
        """
        results = {}

        with self.engine as eng:
            for table_name, data_source in data_sources.items():
                # Check if table is already completed
                if self._state and self._state.is_completed(table_name):
                    cached_results = self._state.get_cached_results(table_name)
                    if cached_results:
                        # Use cached results for completed tables
                        summary = self._reconstruct_summary_from_cache(cached_results, table_name)
                        results[table_name] = summary
                        continue
                
                # Mark table as started if using state management
                if self._state:
                    self._state.mark_table_started(table_name)
                
                try:
                    rules = self.config.get_enabled_rules(table_name)
                    loaded_data = eng.load_data_with_retry(data_source)
                    summary = eng.execute_rules(loaded_data, rules, table_name, self._state)

                    # Integrate with DQX if enabled
                    if self._dqx_enabled:
                        summary = self._integrate_with_dqx(summary, table_name)

                    results[table_name] = summary
                    
                    # Store results and mark as completed
                    if self._state:
                        summary_dict = self._summary_to_dict(summary)
                        self._state.store_results(table_name, summary_dict)
                        
                except Exception as e:
                    # Log error but continue with other tables
                    print(f"Error validating table {table_name}: {str(e)}")
                    # Don't mark as completed on error - allow retry
                    continue

        return results

    def apply_filters(
        self, data: Any, table_name: str, rules: Optional[List[ValidationRule]] = None
    ) -> Any:
        """
        Apply validation rules as filters to clean data.

        Args:
            data: Data to filter
            table_name: Name of the table being filtered
            rules: Optional list of rules to use (defaults to config rules)

        Returns:
            Filtered data
        """
        if rules is None:
            rules = self.config.get_enabled_rules(table_name)

        with self.engine as eng:
            filtered_data = eng.load_data(data)

            for rule in rules:
                if rule.enabled:
                    filtered_data = eng.apply_filter(filtered_data, rule)

            # For DuckDB engine, convert back to DataFrame
            if self.config.engine.type == "duckdb" and isinstance(filtered_data, str):
                # Get the DataFrame from the table
                filtered_data = eng.get_dataframe(filtered_data)

            return filtered_data

    def validate_with_dlt(
        self, data: Any, table_name: str, dlt_expectations: bool = True
    ) -> ValidationSummary:
        """
        Validate data in Delta Live Tables context.

        Args:
            data: Data to validate
            table_name: Name of the table
            dlt_expectations: Whether to create DLT expectations

        Returns:
            ValidationSummary
        """
        summary = self.validate_table(data, table_name)

        if dlt_expectations:
            self._create_dlt_expectations(summary, table_name)

        return summary

    def get_validation_report(
        self, summaries: Union[ValidationSummary, Dict[str, ValidationSummary]]
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report.

        Args:
            summaries: Single summary or dictionary of summaries

        Returns:
            Formatted report dictionary
        """
        if isinstance(summaries, ValidationSummary):
            summaries = {"single_table": summaries}

        report = {
            "validation_timestamp": self._get_timestamp(),
            "engine_type": self.config.engine.type,
            "total_tables": len(summaries),
            "overall_stats": self._calculate_overall_stats(summaries),
            "table_results": {},
        }

        for table_name, summary in summaries.items():
            report["table_results"][table_name] = {
                "total_rules": summary.total_rules,
                "passed_rules": summary.passed_rules,
                "failed_rules": summary.failed_rules,
                "success_rate": summary.overall_success_rate,
                "execution_time_ms": summary.total_execution_time_ms,
                "rules": [
                    {
                        "name": result.rule_name,
                        "type": result.rule_type,
                        "passed": result.passed,
                        "success_rate": result.success_rate,
                        "message": result.message,
                        "severity": result.severity,
                    }
                    for result in summary.results
                ],
            }

        return report

    def _integrate_with_dqx(
        self, summary: ValidationSummary, table_name: str
    ) -> ValidationSummary:
        """Integrate validation results with DQX."""
        try:
            # This is a placeholder for DQX integration
            # In a real implementation, you would use the DQX package here

            # Store metrics if configured
            if self.config.dqx.metrics_table:
                self._store_dqx_metrics(summary, table_name)

            # Store quarantined records if configured
            if self.config.dqx.quarantine_table:
                self._store_quarantined_records(summary, table_name)

        except Exception as e:
            # Don't fail validation if DQX integration fails
            print(f"Warning: DQX integration failed: {str(e)}")

        return summary

    def _create_dlt_expectations(
        self, summary: ValidationSummary, table_name: str
    ) -> None:
        """Create Delta Live Tables expectations from validation results."""
        # This is a placeholder for DLT integration
        # In a real implementation, you would create DLT expectations here
        for result in summary.results:
            if not result.passed and result.severity == "error":
                print(
                    f"DLT Expectation: {result.rule_name} failed for table {table_name}"
                )

    def _store_dqx_metrics(self, summary: ValidationSummary, table_name: str) -> None:
        """Store DQX metrics to configured table."""
        # Placeholder for storing metrics
        pass

    def _store_quarantined_records(
        self, summary: ValidationSummary, table_name: str
    ) -> None:
        """Store quarantined records to configured table."""
        # Placeholder for storing quarantined records
        pass

    def _calculate_overall_stats(
        self, summaries: Dict[str, ValidationSummary]
    ) -> Dict[str, Any]:
        """Calculate overall statistics across all tables."""
        total_rules = sum(s.total_rules for s in summaries.values())
        total_passed = sum(s.passed_rules for s in summaries.values())
        total_failed = sum(s.failed_rules for s in summaries.values())
        total_execution_time = sum(
            s.total_execution_time_ms for s in summaries.values()
        )

        return {
            "total_rules": total_rules,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "overall_success_rate": (
                total_passed / total_rules if total_rules > 0 else 1.0
            ),
            "total_execution_time_ms": total_execution_time,
        }

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime

        return datetime.now().isoformat()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._engine:
            self._engine.disconnect()

    def reset_state(self) -> None:
        """Clear persisted pipeline state."""
        if self._state:
            self._state.reset()

    def reset_table_state(self, table_name: str) -> None:
        """Clear state for a specific table to force re-validation."""
        if self._state:
            self._state.reset_table(table_name)

    def reset_rule_state(self, table_name: str, rule_name: str) -> None:
        """Clear state for a specific rule to force re-execution."""
        if self._state:
            self._state.reset_rule(table_name, rule_name)

    def _summary_to_dict(self, summary: ValidationSummary) -> Dict[str, Any]:
        """Convert ValidationSummary to dictionary for caching."""
        return {
            "table_name": summary.table_name,
            "total_rules": summary.total_rules,
            "passed_rules": summary.passed_rules,
            "failed_rules": summary.failed_rules,
            "warning_rules": summary.warning_rules,
            "error_rules": summary.error_rules,
            "overall_success_rate": summary.overall_success_rate,
            "total_execution_time_ms": summary.total_execution_time_ms,
            "results": [
                {
                    "rule_name": result.rule_name,
                    "rule_type": result.rule_type,
                    "passed": result.passed,
                    "failed_count": result.failed_count,
                    "total_count": result.total_count,
                    "success_rate": result.success_rate,
                    "message": result.message,
                    "severity": result.severity,
                    "execution_time_ms": result.execution_time_ms,
                    "metadata": result.metadata
                }
                for result in summary.results
            ]
        }

    def _reconstruct_summary_from_cache(self, cached_data: Dict[str, Any], table_name: str) -> ValidationSummary:
        """Reconstruct ValidationSummary from cached dictionary data."""
        from .engines import ValidationResult, ValidationSummary
        
        results = []
        for result_data in cached_data.get("results", []):
            result = ValidationResult(
                rule_name=result_data.get("rule_name", ""),
                rule_type=result_data.get("rule_type", ""),
                passed=result_data.get("passed", False),
                failed_count=result_data.get("failed_count", 0),
                total_count=result_data.get("total_count", 0),
                success_rate=result_data.get("success_rate", 0.0),
                message=result_data.get("message", ""),
                severity=result_data.get("severity", "error"),
                execution_time_ms=result_data.get("execution_time_ms", 0),
                metadata=result_data.get("metadata", {})
            )
            results.append(result)

        return ValidationSummary(
            table_name=cached_data.get("table_name", table_name),
            total_rules=cached_data.get("total_rules", 0),
            passed_rules=cached_data.get("passed_rules", 0),
            failed_rules=cached_data.get("failed_rules", 0),
            warning_rules=cached_data.get("warning_rules", 0),
            error_rules=cached_data.get("error_rules", 0),
            overall_success_rate=cached_data.get("overall_success_rate", 0.0),
            total_execution_time_ms=cached_data.get("total_execution_time_ms", 0),
            results=results
        )
