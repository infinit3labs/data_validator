import pytest
from data_validator.config import (
    ValidationConfig,
    ValidationRule,
    TableConfig,
    EngineConfig,
)
from data_validator import DataValidator


def test_sql_snippet_validation():
    rule = ValidationRule(
        name="id_not_null",
        rule_type="custom",
        expression="SELECT COUNT(*) FROM {table} WHERE id IS NULL",
        severity="error",
    )
    table = TableConfig(name="t", rules=[rule])
    config = ValidationConfig(
        require_sql_rules=True,
        engine=EngineConfig(type="duckdb"),
        tables=[table],
    )

    # Should not raise
    DataValidator(config)


def test_sql_snippet_missing_expression():
    rule = ValidationRule(name="bad", rule_type="custom", severity="error")
    table = TableConfig(name="t", rules=[rule])
    config = ValidationConfig(
        require_sql_rules=True,
        engine=EngineConfig(type="duckdb"),
        tables=[table],
    )

    with pytest.raises(ValueError):
        DataValidator(config)
