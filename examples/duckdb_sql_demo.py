"""Demo script showcasing validation using in-memory DuckDB with SQL rules."""

from pathlib import Path

import pandas as pd
from data_validator import DataValidator

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "sql_rules_config.yaml"
DATA_PATH = HERE / "customers.csv"


# Ensure the customers.csv file exists with sample data
if not DATA_PATH.exists():
    sample_data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "age": [25, 30, 35]
    })
    sample_data.to_csv(DATA_PATH, index=False)
def main() -> None:
    data = pd.read_csv(DATA_PATH)
    validator = DataValidator(CONFIG_PATH)
    summary = validator.validate_table(data, "customers")
    report = validator.get_validation_report(summary)
    print(report)


if __name__ == "__main__":
    main()
