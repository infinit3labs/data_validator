import json
from pathlib import Path
import pandas as pd
import yaml

from data_validator import DataValidator
from data_validator.state import PipelineState


def test_pipeline_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state = PipelineState.load(state_path)
    assert not state.is_completed("table1")
    state.mark_completed("table1")
    assert state.is_completed("table1")

    state2 = PipelineState.load(state_path)
    assert state2.is_completed("table1")

    state.reset()
    assert not state.is_completed("table1")


def test_validator_uses_state(tmp_path: Path) -> None:
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb"},
        "pipeline": {"state_file": str(tmp_path / "pipe.json")},
        "tables": [
            {
                "name": "t1",
                "rules": [
                    {
                        "name": "r",
                        "rule_type": "completeness",
                        "column": "a",
                        "severity": "error",
                    }
                ],
            },
            {
                "name": "t2",
                "rules": [
                    {
                        "name": "r",
                        "rule_type": "completeness",
                        "column": "b",
                        "severity": "error",
                    }
                ],
            },
        ],
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))

    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"b": [1]})

    validator = DataValidator(str(cfg_path))
    res1 = validator.validate_all_tables({"t1": df1, "t2": df2})
    assert set(res1.keys()) == {"t1", "t2"}

    res2 = validator.validate_all_tables({"t1": df1, "t2": df2})
    # New behavior: returns cached results instead of empty dict
    assert set(res2.keys()) == {"t1", "t2"}
    # Verify the results are equivalent (from cache)
    assert res2["t1"].total_rules == res1["t1"].total_rules
    assert res2["t2"].total_rules == res1["t2"].total_rules
