"""Utilities to run data validation as a Databricks job."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import yaml

from . import DataValidator


def run_job(config_path: str, sources_path: str, output_path: str, resume: bool = True) -> None:
    """Run data validation using configuration and data sources with recovery support.

    Parameters
    ----------
    config_path: str
        Path to YAML validation configuration.
    sources_path: str
        Path to YAML file mapping table names to data sources.
    output_path: str
        File path where the validation report will be written as JSON.
    resume: bool
        Whether to resume from previous execution state (default: True).
    """
    output_file = Path(output_path)
    temp_output = output_file.with_suffix('.tmp')
    
    # Check if there's a previous partial result to resume from
    partial_results = {}
    if resume and temp_output.exists():
        try:
            with open(temp_output, "r", encoding="utf-8") as f:
                partial_results = json.load(f)
            print(f"Resuming from partial results: {temp_output}")
        except (json.JSONDecodeError, FileNotFoundError):
            # Ignore corrupted or missing partial results
            partial_results = {}

    validator = DataValidator(config_path)

    # Load sources
    try:
        with open(sources_path, "r", encoding="utf-8") as f:
            sources = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise FileNotFoundError(f"Sources file not found: {sources_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in sources file: {e}")

    # Validate all tables (with automatic resumption via state management)
    summaries = {}
    try:
        summaries = validator.validate_all_tables(sources)
        
        # Generate final report
        report = validator.get_validation_report(summaries)
        
        # Write final report atomically
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        
        # Atomic move to final location
        temp_output.replace(output_file)
        
        # Clean up any temporary files
        if temp_output.exists():
            temp_output.unlink()
            
        print(f"Validation completed successfully. Report written to: {output_file}")
        
    except Exception as e:
        # Save partial results for potential recovery
        if summaries:
            partial_report = validator.get_validation_report(summaries)
            with open(temp_output, "w", encoding="utf-8") as f:
                json.dump(partial_report, f, indent=2)
            print(f"Partial results saved to: {temp_output}")
        
        print(f"Validation failed: {str(e)}")
        raise


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data validation job on Databricks with recovery support"
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
    parser.add_argument(
        "--no-resume", action="store_true", 
        help="Disable resumption from previous execution state"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    run_job(args.config, args.sources, args.output, resume=not args.no_resume)


if __name__ == "__main__":
    main()
