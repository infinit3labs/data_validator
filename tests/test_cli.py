import json
import subprocess
from pathlib import Path

import pandas as pd
import yaml


def create_config(tmp: Path) -> Path:
    config = {
        "version": "1.0",
        "engine": {"type": "duckdb", "connection_params": {"database": ":memory:"}},
        "tables": [
            {
                "name": "customers",
                "rules": [
                    {"name": "id_not_null", "rule_type": "completeness", "column": "id", "severity": "error"}
                ],
            }
        ],
    }
    path = tmp / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return path


def create_sources(tmp: Path) -> Path:
    data_path = tmp / "customers.csv"
    pd.DataFrame({"id": [1, None]}).to_csv(data_path, index=False)
    sources_yaml = tmp / "sources.yaml"
    with open(sources_yaml, "w") as f:
        yaml.dump({"customers": str(data_path)}, f)
    return sources_yaml


def test_cli(tmp_path: Path) -> None:
    config = create_config(tmp_path)
    sources = create_sources(tmp_path)
    output = tmp_path / "report.json"

    subprocess.check_call([
        "data-validator",
        "--config",
        str(config),
        "--sources",
        str(sources),
        "--output",
        str(output),
    ])

    assert output.exists()
    report = json.loads(output.read_text())
    assert report["total_tables"] == 1
    assert "customers" in report["table_results"]
