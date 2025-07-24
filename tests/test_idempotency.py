"""Tests for idempotency and recovery features."""

import json
import pandas as pd
import pytest
import yaml
from pathlib import Path
from unittest.mock import patch

from data_validator import DataValidator
from data_validator.state import PipelineState
from data_validator.config import ValidationConfig


def test_pipeline_state_rule_level_tracking(tmp_path: Path) -> None:
    """Test rule-level completion tracking in pipeline state."""
    state_path = tmp_path / "state.json"
    state = PipelineState.load(state_path)
    
    # Test rule completion tracking
    assert not state.is_rule_completed("table1", "rule1")
    state.mark_rule_completed("table1", "rule1", {"passed": True})
    assert state.is_rule_completed("table1", "rule1")
    
    # Test persistence
    state2 = PipelineState.load(state_path)
    assert state2.is_rule_completed("table1", "rule1")
    
    # Test completed rules list
    completed_rules = state2.get_completed_rules("table1")
    assert "rule1" in completed_rules


def test_pipeline_state_result_caching(tmp_path: Path) -> None:
    """Test result caching and retrieval."""
    state_path = tmp_path / "state.json"
    state = PipelineState.load(state_path)
    
    # Store results
    test_results = {
        "table_name": "test_table",
        "total_rules": 2,
        "passed_rules": 1,
        "failed_rules": 1
    }
    state.store_results("test_table", test_results)
    
    # Retrieve cached results
    cached = state.get_cached_results("test_table")
    assert cached == test_results
    assert state.is_completed("test_table")


def test_pipeline_state_migration(tmp_path: Path) -> None:
    """Test migration from old state format to new format."""
    state_path = tmp_path / "state.json"
    
    # Create old format state file
    old_format = {"table1": "completed", "table2": "in_progress"}
    with open(state_path, "w") as f:
        json.dump(old_format, f)
    
    # Load and verify migration
    state = PipelineState.load(state_path)
    assert state.is_completed("table1")
    assert not state.is_completed("table2")  # in_progress != completed


def test_validator_rule_level_resumption(tmp_path: Path) -> None:
    """Test rule-level resumption in validation."""
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "state.json")},
        "tables": [
            {
                "name": "test_table",
                "rules": [
                    {
                        "name": "rule1",
                        "rule_type": "completeness",
                        "column": "col1",
                        "severity": "error",
                    },
                    {
                        "name": "rule2", 
                        "rule_type": "completeness",
                        "column": "col2",
                        "severity": "error",
                    }
                ],
            }
        ],
    }
    
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    
    # Create validator and manually mark one rule as completed
    validator = DataValidator(str(cfg_path))
    validator._state.mark_rule_completed("test_table", "rule1", {
        "rule_name": "rule1",
        "rule_type": "completeness",
        "passed": True,
        "failed_count": 0,
        "total_count": 3,
        "success_rate": 1.0,
        "message": "Cached result",
        "severity": "error",
        "execution_time_ms": 0,
        "metadata": {}
    })
    
    # Validate - should use cached result for rule1 and execute rule2
    summary = validator.validate_table(df, "test_table")
    
    # Verify both rules are present in results
    assert summary.total_rules == 2
    rule_names = [r.rule_name for r in summary.results]
    assert "rule1" in rule_names
    assert "rule2" in rule_names
    
    # Verify rule1 has cached message
    rule1_result = next(r for r in summary.results if r.rule_name == "rule1")
    assert rule1_result.message == "Cached result"


def test_validator_table_level_caching(tmp_path: Path) -> None:
    """Test complete table result caching."""
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "state.json")},
        "tables": [
            {
                "name": "test_table",
                "rules": [
                    {
                        "name": "rule1",
                        "rule_type": "completeness",
                        "column": "col1",
                        "severity": "error",
                    }
                ],
            }
        ],
    }
    
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    
    df = pd.DataFrame({"col1": [1, 2, 3]})
    
    validator = DataValidator(str(cfg_path))
    
    # First validation
    summary1 = validator.validate_table(df, "test_table")
    assert summary1.total_rules == 1
    
    # Second validation should use cached results
    summary2 = validator.validate_table(df, "test_table")
    assert summary2.total_rules == 1
    assert summary2.table_name == summary1.table_name


def test_validate_all_tables_idempotency(tmp_path: Path) -> None:
    """Test that validate_all_tables is idempotent."""
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "state.json")},
        "tables": [
            {
                "name": "table1",
                "rules": [{"name": "rule1", "rule_type": "completeness", "column": "col1"}]
            },
            {
                "name": "table2", 
                "rules": [{"name": "rule2", "rule_type": "completeness", "column": "col2"}]
            }
        ],
    }
    
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    
    df1 = pd.DataFrame({"col1": [1, 2]})
    df2 = pd.DataFrame({"col2": [3, 4]})
    sources = {"table1": df1, "table2": df2}
    
    validator = DataValidator(str(cfg_path))
    
    # First run
    results1 = validator.validate_all_tables(sources)
    assert len(results1) == 2
    
    # Second run should return cached results
    results2 = validator.validate_all_tables(sources)
    assert len(results2) == 2
    
    # Results should be equivalent
    for table_name in results1:
        assert table_name in results2
        assert results1[table_name].total_rules == results2[table_name].total_rules


def test_state_reset_operations(tmp_path: Path) -> None:
    """Test various state reset operations."""
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "state.json")},
        "tables": [
            {
                "name": "table1",
                "rules": [
                    {"name": "rule1", "rule_type": "completeness", "column": "col1"},
                    {"name": "rule2", "rule_type": "completeness", "column": "col2"}
                ]
            }
        ],
    }
    
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    
    validator = DataValidator(str(cfg_path))
    
    # Complete validation
    validator.validate_table(df, "table1")
    assert validator._state.is_completed("table1")
    
    # Test rule-level reset
    validator.reset_rule_state("table1", "rule1")
    assert not validator._state.is_rule_completed("table1", "rule1")
    assert validator._state.is_rule_completed("table1", "rule2")
    
    # Test table-level reset
    validator.reset_table_state("table1")
    assert not validator._state.is_completed("table1")
    
    # Test full reset
    validator.validate_table(df, "table1")  # Complete again
    validator.reset_state()
    assert not validator._state.is_completed("table1")


def test_engine_retry_logic_simulation(tmp_path: Path) -> None:
    """Test engine retry logic with simulated failures."""
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "state.json")},
        "tables": [
            {
                "name": "test_table",
                "rules": [{"name": "rule1", "rule_type": "completeness", "column": "col1"}]
            }
        ],
    }
    
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    
    df = pd.DataFrame({"col1": [1, 2, 3]})
    
    validator = DataValidator(str(cfg_path))
    
    # Mock a transient error followed by success
    original_load_data = validator.engine.load_data
    call_count = 0
    
    def mock_load_data_with_retry(source):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Temporary connection failure")
        return original_load_data(source)
    
    with patch.object(validator.engine, 'load_data', side_effect=mock_load_data_with_retry):
        # This should succeed on retry
        summary = validator.validate_table(df, "test_table")
        assert summary.total_rules == 1
        assert call_count >= 2  # Should have retried


def test_databricks_job_recovery(tmp_path: Path) -> None:
    """Test databricks job recovery functionality."""
    from data_validator.databricks_job import run_job
    
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "job_state.json")},
        "tables": [
            {
                "name": "customers",
                "rules": [{"name": "id_check", "rule_type": "completeness", "column": "id"}]
            }
        ],
    }
    
    sources = {
        "customers": str(tmp_path / "customers.csv")
    }
    
    # Create test data
    customers_df = pd.DataFrame({"id": [1, 2, 3]})
    customers_df.to_csv(tmp_path / "customers.csv", index=False)
    
    config_path = tmp_path / "config.yaml" 
    sources_path = tmp_path / "sources.yaml"
    output_path = tmp_path / "output.json"
    
    config_path.write_text(yaml.dump(config))
    sources_path.write_text(yaml.dump(sources))
    
    # Run job - should complete successfully
    run_job(str(config_path), str(sources_path), str(output_path))
    
    # Verify output exists and is valid
    assert output_path.exists()
    with open(output_path) as f:
        report = json.load(f)
    
    assert "table_results" in report
    assert "customers" in report["table_results"]
    
    # Run again - should be idempotent (use cached results)
    run_job(str(config_path), str(sources_path), str(output_path))
    
    # Output should still be valid
    with open(output_path) as f:
        report2 = json.load(f)
    
    assert report2["table_results"]["customers"]["total_rules"] == 1


if __name__ == "__main__":
    pytest.main([__file__])