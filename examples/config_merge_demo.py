"""Demo showing configuration merging from YAML, environment variables, and Databricks widgets."""

import os
from pathlib import Path

from data_validator.settings import load_config
from data_validator import DataValidator

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "sample_config.yaml"


def main() -> None:
    """Load configuration from multiple sources and run a simple validation."""
    # Override the engine type using an environment variable
    os.environ["VALIDATOR_ENGINE__TYPE"] = "duckdb"

    config = load_config(str(CONFIG_PATH))
    print(f"Merged engine type: {config.engine.type}")

    validator = DataValidator(config)
    print(f"Loaded {len(validator.config.tables)} table configurations")


if __name__ == "__main__":
    main()
