"""
Tests for the main DataValidator class.
"""

import pytest
import tempfile
import yaml
from pathlib import Path

from data_validator import DataValidator, ValidationConfig
from data_validator.config import ValidationRule, TableConfig, EngineConfig


def create_test_config():
    """Create a test configuration."""
    rule = ValidationRule(
        name="test_completeness",
        rule_type="completeness",
        column="id",
        severity="error",
    )

    table_config = TableConfig(name="test_table", rules=[rule])

    engine_config = EngineConfig(type="duckdb")  # Use DuckDB instead of PySpark

    return ValidationConfig(engine=engine_config, tables=[table_config])


def test_data_validator_creation_with_config_object():
    """Test creating DataValidator with config object."""
    config = create_test_config()
    validator = DataValidator(config)

    assert validator.config.engine.type == "duckdb"
    assert len(validator.config.tables) == 1


def test_data_validator_creation_with_dict():
    """Test creating DataValidator with dictionary config."""
    config_dict = {
        "version": "1.0",
        "engine": {"type": "duckdb"},  # Use DuckDB instead of PySpark
        "tables": [
            {
                "name": "test_table",
                "rules": [
                    {
                        "name": "test_rule",
                        "rule_type": "completeness",
                        "column": "id",
                        "severity": "error",
                    }
                ],
            }
        ],
    }

    validator = DataValidator(config_dict)
    assert validator.config.engine.type == "duckdb"


def test_data_validator_creation_with_yaml_file():
    """Test creating DataValidator with YAML file."""
    config_data = {
        "version": "1.0",
        "engine": {"type": "duckdb"},  # Use DuckDB instead of PySpark
        "tables": [
            {
                "name": "test_table",
                "rules": [
                    {
                        "name": "test_rule",
                        "rule_type": "completeness",
                        "column": "id",
                        "severity": "error",
                    }
                ],
            }
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        validator = DataValidator(temp_path)
        assert validator.config.engine.type == "duckdb"
    finally:
        Path(temp_path).unlink()


def test_data_validator_invalid_config_type():
    """Test that invalid config type raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported config type"):
        DataValidator(123)


def test_data_validator_engine_property():
    """Test that engine property creates engine on first access."""
    config = create_test_config()
    validator = DataValidator(config)

    # Engine should be None initially
    assert validator._engine is None

    # Accessing engine property should create it
    engine = validator.engine
    assert engine is not None
    assert validator._engine is not None


def test_data_validator_context_manager():
    """Test DataValidator as context manager."""
    config = create_test_config()

    with DataValidator(config) as validator:
        assert validator is not None
        assert validator.config.engine.type == "duckdb"


def test_get_validation_report_single_summary():
    """Test generating validation report from single summary."""
    from data_validator.engines import ValidationResult, ValidationSummary

    config = create_test_config()
    validator = DataValidator(config)

    # Create a mock validation result
    result = ValidationResult(
        rule_name="test_rule",
        rule_type="completeness",
        passed=True,
        failed_count=0,
        total_count=100,
        success_rate=1.0,
        message="All records passed",
        severity="error",
        execution_time_ms=50.0,
        metadata={"engine": "test"},
    )

    summary = ValidationSummary(
        table_name="test_table",
        total_rules=1,
        passed_rules=1,
        failed_rules=0,
        warning_rules=0,
        error_rules=0,
        overall_success_rate=1.0,
        total_execution_time_ms=50.0,
        results=[result],
    )

    report = validator.get_validation_report(summary)

    assert report["total_tables"] == 1
    assert report["overall_stats"]["total_rules"] == 1
    assert report["overall_stats"]["total_passed"] == 1
    assert report["overall_stats"]["overall_success_rate"] == 1.0
    assert (
        "single_table" in report["table_results"]
    )  # Changed from "test_table" to "single_table"


def test_get_validation_report_multiple_summaries():
    """Test generating validation report from multiple summaries."""
    from data_validator.engines import ValidationResult, ValidationSummary

    config = create_test_config()
    validator = DataValidator(config)

    # Create mock validation results
    result1 = ValidationResult(
        rule_name="rule1",
        rule_type="completeness",
        passed=True,
        failed_count=0,
        total_count=100,
        success_rate=1.0,
        message="All records passed",
        severity="error",
        execution_time_ms=30.0,
        metadata={"engine": "test"},
    )

    result2 = ValidationResult(
        rule_name="rule2",
        rule_type="uniqueness",
        passed=False,
        failed_count=5,
        total_count=100,
        success_rate=0.95,
        message="5 duplicate records found",
        severity="warning",
        execution_time_ms=20.0,
        metadata={"engine": "test"},
    )

    summary1 = ValidationSummary(
        table_name="table1",
        total_rules=1,
        passed_rules=1,
        failed_rules=0,
        warning_rules=0,
        error_rules=0,
        overall_success_rate=1.0,
        total_execution_time_ms=30.0,
        results=[result1],
    )

    summary2 = ValidationSummary(
        table_name="table2",
        total_rules=1,
        passed_rules=0,
        failed_rules=1,
        warning_rules=1,
        error_rules=0,
        overall_success_rate=0.0,
        total_execution_time_ms=20.0,
        results=[result2],
    )

    summaries = {"table1": summary1, "table2": summary2}
    report = validator.get_validation_report(summaries)

    assert report["total_tables"] == 2
    assert report["overall_stats"]["total_rules"] == 2
    assert report["overall_stats"]["total_passed"] == 1
    assert report["overall_stats"]["total_failed"] == 1
    assert report["overall_stats"]["overall_success_rate"] == 0.5
    assert "table1" in report["table_results"]
    assert "table2" in report["table_results"]


def test_validate_configured_tables(tmp_path):
    """Validate tables using data sources from configuration."""
    import pandas as pd

    data = pd.DataFrame({"id": [1, None, 3]})
    csv_path = tmp_path / "data.csv"
    data.to_csv(csv_path, index=False)

    config = {
        "version": "1.0",
        "engine": {"type": "duckdb", "connection_params": {"database": ":memory:"}},
        "tables": [
            {
                "name": "test_table",
                "data_source": str(csv_path),
                "rules": [
                    {
                        "name": "id_not_null",
                        "rule_type": "completeness",
                        "column": "id",
                        "severity": "error",
                    }
                ],
            }
        ],
    }

    validator = DataValidator(config)
    summaries = validator.validate_configured_tables()

    assert "test_table" in summaries
    summary = summaries["test_table"]
    assert summary.total_rules == 1
