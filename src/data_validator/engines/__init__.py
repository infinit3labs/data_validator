"""
Abstract engine interface and implementations for different compute engines.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from ..config import ValidationRule, EngineConfig


@dataclass
class ValidationResult:
    """Result of a validation rule execution."""
    
    rule_name: str
    rule_type: str
    passed: bool
    failed_count: int
    total_count: int
    success_rate: float
    message: str
    severity: str
    execution_time_ms: float
    metadata: Dict[str, Any]


@dataclass
class ValidationSummary:
    """Summary of all validation results."""
    
    table_name: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    error_rules: int
    overall_success_rate: float
    total_execution_time_ms: float
    results: List[ValidationResult]


class ValidationEngine(ABC):
    """Abstract base class for validation engines."""
    
    def __init__(self, config: EngineConfig):
        self.config = config
        self._connection = None
        self._max_retries = config.options.get('max_retries', 3)
        self._retry_delay = config.options.get('retry_delay', 1.0)
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the compute engine."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the compute engine."""
        pass
    
    @abstractmethod
    def load_data(self, source: Union[str, Any]) -> Any:
        """Load data from source (table name, file path, or DataFrame)."""
        pass
    
    @abstractmethod
    def execute_rule(self, data: Any, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule against the data."""
        pass
    
    @abstractmethod
    def apply_filter(self, data: Any, rule: ValidationRule) -> Any:
        """Apply validation rule as a filter and return filtered data."""
        pass

    def _is_transient_error(self, error: Exception) -> bool:
        """Check if an error is transient and should be retried."""
        # Default implementation for common transient errors
        error_str = str(error).lower()
        transient_patterns = [
            'connection', 'timeout', 'network', 'temporary', 'transient',
            'unavailable', 'overloaded', 'throttled', 'rate limit'
        ]
        return any(pattern in error_str for pattern in transient_patterns)

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute an operation with retry logic for transient failures."""
        import time
        
        last_exception = None
        for attempt in range(self._max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self._max_retries and self._is_transient_error(e):
                    time.sleep(self._retry_delay * (2 ** attempt))  # Exponential backoff
                    # Try to reconnect for connection-related errors
                    if 'connection' in str(e).lower():
                        try:
                            self.disconnect()
                            self.connect()
                        except:
                            pass  # Ignore reconnection errors, let the retry handle it
                    continue
                else:
                    raise e
        
        raise last_exception

    def connect_with_retry(self) -> None:
        """Connect with retry logic for transient failures."""
        self._execute_with_retry(self.connect)
    
    def load_data_with_retry(self, source: Union[str, Any]) -> Any:
        """Load data with retry logic for transient failures."""
        return self._execute_with_retry(self.load_data, source)
    
    def execute_rule_with_retry(self, data: Any, rule: ValidationRule) -> ValidationResult:
        """Execute rule with retry logic for transient failures."""
        return self._execute_with_retry(self.execute_rule, data, rule)
    
    def execute_rules(self, data: Any, rules: List[ValidationRule], table_name: str = "unknown", 
                     state_manager=None) -> ValidationSummary:
        """Execute multiple validation rules against the data with state-based resumption."""
        import time
        
        start_time = time.time()
        results = []
        
        for rule in rules:
            if not rule.enabled:
                continue
                
            # Check if rule is already completed (for resumption)
            if state_manager and state_manager.is_rule_completed(table_name, rule.name):
                # Try to get cached result
                cached_rule_result = None
                table_state = state_manager.state.get(table_name, {})
                rule_state = table_state.get("rules", {}).get(rule.name, {})
                cached_rule_result = rule_state.get("result")
                
                if cached_rule_result:
                    # Reconstruct ValidationResult from cached data
                    result = ValidationResult(
                        rule_name=cached_rule_result.get("rule_name", rule.name),
                        rule_type=cached_rule_result.get("rule_type", rule.rule_type),
                        passed=cached_rule_result.get("passed", False),
                        failed_count=cached_rule_result.get("failed_count", 0),
                        total_count=cached_rule_result.get("total_count", 0),
                        success_rate=cached_rule_result.get("success_rate", 0.0),
                        message=cached_rule_result.get("message", ""),
                        severity=cached_rule_result.get("severity", rule.severity),
                        execution_time_ms=cached_rule_result.get("execution_time_ms", 0),
                        metadata=cached_rule_result.get("metadata", {})
                    )
                    results.append(result)
                    continue
            
            # Execute the rule with retry logic
            try:
                result = self.execute_rule_with_retry(data, rule)
                results.append(result)
                
                # Cache the result for recovery
                if state_manager:
                    result_dict = {
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
                    state_manager.mark_rule_completed(table_name, rule.name, result_dict)
                    
            except Exception as e:
                # Create a failed result for consistency
                failed_result = ValidationResult(
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    passed=False,
                    failed_count=1,
                    total_count=1,
                    success_rate=0.0,
                    message=f"Rule execution failed: {str(e)}",
                    severity=rule.severity,
                    execution_time_ms=0,
                    metadata={"error": str(e), "error_type": type(e).__name__}
                )
                results.append(failed_result)
                
                # Don't cache failed results - they should be retried
                continue
        
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Calculate summary statistics
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        warning_rules = sum(1 for r in results if r.severity == "warning" and not r.passed)
        error_rules = sum(1 for r in results if r.severity == "error" and not r.passed)
        overall_success_rate = passed_rules / total_rules if total_rules > 0 else 1.0
        
        return ValidationSummary(
            table_name=table_name,
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            warning_rules=warning_rules,
            error_rules=error_rules,
            overall_success_rate=overall_success_rate,
            total_execution_time_ms=execution_time,
            results=results
        )
    
    def __enter__(self):
        self.connect_with_retry()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.disconnect()
        except:
            # Ignore errors during cleanup
            pass


def create_engine(config: EngineConfig) -> ValidationEngine:
    """Factory function to create appropriate validation engine."""
    if config.type == "pyspark":
        from .pyspark_engine import PySparkValidationEngine
        return PySparkValidationEngine(config)
    elif config.type == "databricks":
        from .databricks_engine import DatabricksValidationEngine
        return DatabricksValidationEngine(config)
    elif config.type == "duckdb":
        from .duckdb_engine import DuckDBValidationEngine
        return DuckDBValidationEngine(config)
    elif config.type == "polars":
        from .polars_engine import PolarsValidationEngine
        return PolarsValidationEngine(config)
    else:
        raise ValueError(f"Unsupported engine type: {config.type}")