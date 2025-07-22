"""Command line interface for running data validation locally."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from . import DataValidator


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data validation using a YAML configuration file",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML validation configuration",
    )
    parser.add_argument(
        "--sources",
        help="Optional YAML file mapping table names to data sources",
    )
    parser.add_argument(
        "--table",
        help="Validate only a specific table from the configuration",
    )
    parser.add_argument(
        "--output",
        help="Path to write JSON validation report (prints to stdout if omitted)",
    )
    return parser.parse_args(argv)


def run_cli(config_path: str, sources_path: Optional[str], table: Optional[str], output_path: Optional[str]) -> None:
    """Execute validation based on CLI arguments."""
    validator = DataValidator(config_path)

    summaries: Dict[str, Any] = {}
    if sources_path:
        with open(sources_path, "r", encoding="utf-8") as f:
            sources = json.load(f) if sources_path.endswith(".json") else yaml.safe_load(f)
        if table:
            data = sources.get(table)
            if data is None:
                raise ValueError(f"Table '{table}' not found in sources file")
            summaries[table] = validator.validate_table(data, table)
        else:
            summaries = validator.validate_all_tables(sources)
    else:
        if table is None:
            raise ValueError("--table must be provided when no sources file is given")
        summaries[table] = validator.validate_table(table, table)

    report = validator.get_validation_report(summaries)
    output_data = json.dumps(report, indent=2)

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(output_data)
    else:
        print(output_data)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    run_cli(args.config, args.sources, args.table, args.output)


if __name__ == "__main__":  # pragma: no cover - entry point
    main()
