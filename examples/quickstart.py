"""Quickstart demo for the data_validator package."""

from pathlib import Path

import pandas as pd

from data_validator import DataValidator

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "sample_config.yaml"


def main() -> None:
    """Run a simple validation using the sample configuration."""
    customers = pd.DataFrame(
        {
            "customer_id": [1, 2, 3, None],
            "email": ["a@example.com", "b@example.com", "bad-email", "d@example.com"],
        }
    )

    validator = DataValidator(CONFIG_PATH)
    summary = validator.validate_table(customers, "customers")
    print("Success rate:", summary.overall_success_rate)

    clean_df = validator.apply_filters(customers, "customers")
    print("Rows after filtering:", len(clean_df))


if __name__ == "__main__":
    main()
