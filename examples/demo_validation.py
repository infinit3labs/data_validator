"""Demonstration script for the data_validator package.

This script loads configuration from ``demo_config.yaml`` and validates a
sample Pandas DataFrame. It prints a short summary and returns the
``ValidationSummary`` instance for programmatic use.
"""
from pathlib import Path
import pandas as pd

from data_validator import DataValidator


DATA = pd.DataFrame(
    {
        "id": [1, 2, 3, None, 5],
        "email": ["a@example.com", "invalid", "c@example.com", "d@example.com", None],
        "age": [25, 130, 35, 40, 28],
    }
)


def run_demo() -> "data_validator.engines.ValidationSummary":
    """Execute the demo validation run."""
    config_path = Path(__file__).parent / "demo_config.yaml"
    validator = DataValidator(config_path)
    summary = validator.validate_table(DATA, "users")
    report = validator.get_validation_report(summary)
    print(
        f"Validation success rate: {report['overall_stats']['overall_success_rate']:.1%}"
    )
    return summary


if __name__ == "__main__":
    run_demo()
