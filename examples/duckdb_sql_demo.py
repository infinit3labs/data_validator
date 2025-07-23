"""Demo script showcasing validation using in-memory DuckDB with SQL rules."""

from pathlib import Path

import pandas as pd
from data_validator import DataValidator

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "sql_rules_config.yaml"
DATA_PATH = HERE / "customers.csv"


def main() -> None:
    data = pd.read_csv(DATA_PATH)
    validator = DataValidator(CONFIG_PATH)
    summary = validator.validate_table(data, "customers")
    report = validator.get_validation_report(summary)
    print(report)


if __name__ == "__main__":
    main()
