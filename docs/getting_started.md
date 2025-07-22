# Getting Started

This guide walks through the basic steps required to validate data using the
`data_validator` package. It includes sample configuration files and reusable
Python snippets so you can quickly integrate the validator in your own
projects.

## 1. Install the package

```bash
pip install data-validator[duckdb]  # install with optional engine
```

The project uses a configuration driven approach. Validation behavior is
defined entirely in YAML files.

## 2. Create a configuration file

Below is a minimal template that can be used as a starting point. Save this as
`config.yaml`.

```yaml
version: "1.0"
engine:
  type: "duckdb"  # or pyspark / polars

tables:
  - name: "customers"
    rules:
      - name: "customer_id_not_null"
        rule_type: "completeness"
        column: "customer_id"
        severity: "error"
```

Additional rule types and options can be added. See the [examples](../examples)
folder for more comprehensive configurations.

## 3. Validate data

```python
from data_validator import DataValidator
import pandas as pd

# Sample data
customers = pd.read_csv("customers.csv")

validator = DataValidator("config.yaml")
summary = validator.validate_table(customers, "customers")
print(summary.overall_success_rate)
```

## 4. Clean data using filters

```python
clean_df = validator.apply_filters(customers, "customers")
```

## 5. Validate multiple tables

```python
sources = {
    "customers": customers,
    "orders": pd.read_csv("orders.csv"),
}

results = validator.validate_all_tables(sources)
```

For a runnable example see [`examples/quickstart.py`](../examples/quickstart.py).

## Configuration Merge Demo
Run `python examples/config_merge_demo.py` to see environment overrides merged with YAML.
