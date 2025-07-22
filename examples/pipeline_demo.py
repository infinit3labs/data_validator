"""Demonstrate pipeline state management."""

from pathlib import Path
import pandas as pd
from data_validator import DataValidator

HERE = Path(__file__).parent
CONFIG = HERE / "pipeline_config.yaml"
STATE_FILE = HERE / "pipeline_state.json"


def main() -> None:
    customers = pd.DataFrame({"customer_id": [1, 2, None]})
    orders = pd.DataFrame({"order_id": [10, 11]})

    validator = DataValidator(CONFIG)
    summaries = validator.validate_all_tables(
        {"customers": customers, "orders": orders}
    )
    report = validator.get_validation_report(summaries)
    print(report)

    # Running again shows idempotent behaviour
    summaries = validator.validate_all_tables(
        {"customers": customers, "orders": orders}
    )
    print("Second run skipped tables:", summaries)

    # Reset state to rerun
    validator.reset_state()
    validator.validate_all_tables({"customers": customers, "orders": orders})


if __name__ == "__main__":
    main()
