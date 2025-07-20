"""Demo script showing how to run the CLI on a Databricks cluster."""

import os
from pathlib import Path

# Path to sample configuration located in the examples directory
CONFIG_PATH = Path(__file__).parent / "sample_config.yaml"

# Output path on DBFS or local filesystem
OUTPUT_PATH = Path(os.environ.get("VALIDATION_OUTPUT", "./validation_report.json"))

if __name__ == "__main__":
    from data_validator.cli import main

    main(
        [
            "--config",
            str(CONFIG_PATH),
            "--report-path",
            str(OUTPUT_PATH),
        ]
    )
    print(f"Report written to {OUTPUT_PATH}")
