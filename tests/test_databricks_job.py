import json
import tempfile
from pathlib import Path
import yaml
import pandas as pd
from data_validator.databricks_job import run_job


def create_config(tmp: Path) -> Path:
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb", "connection_params": {"database": ":memory:"}},
        "tables": [
            {
                "name": "customers",
                "rules": [
                    {
                        "name": "id_completeness",
                        "rule_type": "completeness",
                        "column": "id",
                        "severity": "error",
                    }
                ],
            }
        ],
    }
    path = tmp / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return path


def create_sources(tmp: Path) -> tuple[Path, Path]:
    data_path = tmp / "customers.csv"
    pd.DataFrame({"id": [1, None, 3]}).to_csv(data_path, index=False)
    sources_yaml = tmp / "sources.yaml"
    with open(sources_yaml, "w") as f:
        yaml.dump({"customers": str(data_path)}, f)
    return data_path, sources_yaml


def test_run_job(tmp_path: Path) -> None:
    config_path = create_config(tmp_path)
    _, sources_path = create_sources(tmp_path)
    output_path = tmp_path / "report.json"

    run_job(str(config_path), str(sources_path), str(output_path))

    assert output_path.exists()
    report = json.loads(output_path.read_text())
    assert report["total_tables"] == 1
    assert "customers" in report["table_results"]
