"""Utilities to run data validation as a Databricks job."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import yaml

from . import DataValidator


def run_job(config_path: str, sources_path: str, output_path: str) -> None:
    """Run data validation using configuration and data sources.

    Parameters
    ----------
    config_path: str
        Path to YAML validation configuration.
    sources_path: str
        Path to YAML file mapping table names to data sources.
    output_path: str
        File path where the validation report will be written as JSON.
    """
    validator = DataValidator(config_path)

    with open(sources_path, "r", encoding="utf-8") as f:
        sources = yaml.safe_load(f) or {}

    summaries = validator.validate_all_tables(sources)
    report = validator.get_validation_report(summaries)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data validation job on Databricks"
    )
    parser.add_argument(
        "--config", required=True, help="Path to validation YAML config"
    )
    parser.add_argument(
        "--sources", required=True, help="YAML file with table source mapping"
    )
    parser.add_argument(
        "--output", required=True, help="Path to write JSON validation report"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    run_job(args.config, args.sources, args.output)


if __name__ == "__main__":
    main()
