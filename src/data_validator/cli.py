"""Command line interface for running data validation jobs.

This CLI is designed for execution on Databricks clusters or locally.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict

from . import DataValidator


def _parse_overrides(values: list[str]) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"Invalid override '{item}'. Use table=source format."
            )
        table, source = item.split("=", 1)
        overrides[table] = source
    return overrides


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run data validation job")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument(
        "--report-path",
        help="Optional path to write validation report as JSON",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        help="Override data source mapping using table=path. Can be specified multiple times.",
    )

    args = parser.parse_args(argv)
    overrides = _parse_overrides(args.override)

    validator = DataValidator(args.config)

    if overrides:
        data_sources = {
            table.name: overrides.get(table.name, table.data_source)
            for table in validator.config.tables
        }
        missing = [name for name, src in data_sources.items() if src is None]
        if missing:
            raise ValueError("Data source missing for tables: " + ", ".join(missing))
        summaries = validator.validate_all_tables(data_sources)
    else:
        summaries = validator.validate_configured_tables()

    report = validator.get_validation_report(summaries)

    if args.report_path:
        path = Path(args.report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2))
    else:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":  # pragma: no cover - manual invocation only
    main()
