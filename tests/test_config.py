"""
Tests for the data validation configuration module.
"""

import pytest
import tempfile
import yaml
from pathlib import Path

from data_validator.config import (
    ValidationConfig,
    ValidationRule,
    TableConfig,
    EngineConfig,
)


def test_validation_rule_creation():
    """Test creating a validation rule."""
    rule = ValidationRule(
        name="test_rule",
        rule_type="completeness",
        column="test_column",
        severity="error",
    )

    assert rule.name == "test_rule"
    assert rule.rule_type == "completeness"
    assert rule.column == "test_column"
    assert rule.severity == "error"
    assert rule.enabled is True
    assert rule.threshold is None


def test_validation_rule_with_threshold():
    """Test creating a validation rule with threshold."""
    rule = ValidationRule(
        name="test_rule",
        rule_type="completeness",
        column="test_column",
        threshold=0.95,
        severity="warning",
    )

    assert rule.threshold == 0.95
    assert rule.severity == "warning"


def test_validation_rule_invalid_severity():
    """Test that invalid severity raises ValueError."""
    with pytest.raises(ValueError, match="Severity must be one of"):
        ValidationRule(
            name="test_rule",
            rule_type="completeness",
            column="test_column",
            severity="invalid",
        )


def test_validation_rule_invalid_threshold():
    """Test that invalid threshold raises ValueError."""
    with pytest.raises(ValueError, match="Threshold must be between 0.0 and 1.0"):
        ValidationRule(
            name="test_rule",
            rule_type="completeness",
            column="test_column",
            threshold=1.5,
        )


def test_table_config_creation():
    """Test creating a table configuration."""
    rule = ValidationRule(
        name="test_rule",
        rule_type="completeness",
        column="test_column",
        severity="error",
    )

    table_config = TableConfig(
        name="test_table",
        description="Test table",
        data_source="/path/to/table.csv",
        rules=[rule],
    )

    assert table_config.name == "test_table"
    assert len(table_config.rules) == 1
    assert table_config.rules[0].name == "test_rule"
    assert table_config.data_source == "/path/to/table.csv"


def test_engine_config_creation():
    """Test creating an engine configuration."""
    engine_config = EngineConfig(
        type="pyspark",
        connection_params={"spark.sql.adaptive.enabled": "true"},
        options={"key": "value"},
    )

    assert engine_config.type == "pyspark"
    assert engine_config.connection_params["spark.sql.adaptive.enabled"] == "true"
    assert engine_config.options["key"] == "value"


def test_engine_config_invalid_type():
    """Test that invalid engine type raises ValueError."""
    with pytest.raises(ValueError, match="Engine type must be one of"):
        EngineConfig(type="invalid_engine")


def test_validation_config_creation():
    """Test creating a complete validation configuration."""
    rule = ValidationRule(
        name="test_rule",
        rule_type="completeness",
        column="test_column",
        severity="error",
    )

    table_config = TableConfig(
        name="test_table", data_source="/tmp/table.csv", rules=[rule]
    )

    engine_config = EngineConfig(type="pyspark")

    config = ValidationConfig(engine=engine_config, tables=[table_config])

    assert config.version == "1.0"
    assert config.engine.type == "pyspark"
    assert len(config.tables) == 1
    assert config.tables[0].name == "test_table"
    assert config.tables[0].data_source == "/tmp/table.csv"


def test_validation_config_from_yaml():
    """Test loading configuration from YAML file."""
    config_data = {
        "version": "1.0",
        "engine": {"type": "pyspark", "connection_params": {}, "options": {}},
        "tables": [
            {
                "name": "test_table",
                "data_source": "/tmp/table.csv",
                "rules": [
                    {
                        "name": "test_rule",
                        "rule_type": "completeness",
                        "column": "test_column",
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
        config = ValidationConfig.from_yaml(temp_path)
        assert config.version == "1.0"
        assert config.engine.type == "pyspark"
        assert len(config.tables) == 1
        assert config.tables[0].name == "test_table"
        assert config.tables[0].data_source == "/tmp/table.csv"
    finally:
        Path(temp_path).unlink()


def test_validation_config_get_table_config():
    """Test getting table configuration by name."""
    rule = ValidationRule(
        name="test_rule",
        rule_type="completeness",
        column="test_column",
        severity="error",
    )

    table_config = TableConfig(name="test_table", rules=[rule])

    engine_config = EngineConfig(type="pyspark")

    config = ValidationConfig(engine=engine_config, tables=[table_config])

    found_table = config.get_table_config("test_table")
    assert found_table is not None
    assert found_table.name == "test_table"

    not_found = config.get_table_config("nonexistent")
    assert not_found is None


def test_validation_config_get_enabled_rules():
    """Test getting enabled rules for a table."""
    rule1 = ValidationRule(
        name="rule1",
        rule_type="completeness",
        column="col1",
        severity="error",
        enabled=True,
    )

    rule2 = ValidationRule(
        name="rule2",
        rule_type="uniqueness",
        column="col2",
        severity="warning",
        enabled=False,
    )

    global_rule = ValidationRule(
        name="global_rule",
        rule_type="completeness",
        column="global_col",
        severity="error",
        enabled=True,
    )

    table_config = TableConfig(name="test_table", rules=[rule1, rule2])

    engine_config = EngineConfig(type="pyspark")

    config = ValidationConfig(
        engine=engine_config, tables=[table_config], global_rules=[global_rule]
    )

    enabled_rules = config.get_enabled_rules("test_table")
    assert len(enabled_rules) == 2  # 1 table rule + 1 global rule
    assert enabled_rules[0].name == "global_rule"
    assert enabled_rules[1].name == "rule1"
