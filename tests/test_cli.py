import json
from pathlib import Path
from subprocess import run, PIPE

import pandas as pd


def test_cli_runner(tmp_path):
    data = pd.DataFrame({"id": [1, None]})
    csv_path = tmp_path / "data.csv"
    data.to_csv(csv_path, index=False)

    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
version: '1.0'
engine:
  type: duckdb
  connection_params:
    database: ':memory:'
tables:
  - name: test_table
    data_source: {csv_path}
    rules:
      - name: id_not_null
        rule_type: completeness
        column: id
        severity: error
"""
    )

    report_path = tmp_path / "report.json"

    result = run(
        [
            "python",
            "-m",
            "data_validator.cli",
            "--config",
            str(config),
            "--report-path",
            str(report_path),
        ],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
        check=True,
    )

    assert report_path.exists()
    report = json.loads(report_path.read_text())
    assert report["total_tables"] == 1
