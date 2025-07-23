# Data Validator User Guide

This guide provides a quick introduction to the `data_validator` package. All functionality is configuration driven through YAML files. Examples below demonstrate how to create configurations and execute validations using different compute engines.

## Installation with Poetry

```bash
poetry install
```

To run the examples in this repository use Poetry's run command:

```bash
poetry run python examples/demo_validation.py
```

## Example YAML Configuration

```yaml
version: "1.0"
engine:
  type: "duckdb"
  connection_params:
    database: ":memory:"
tables:
  - name: "users"
    rules:
      - name: "id_completeness"
        rule_type: "completeness"
        column: "id"
        severity: "error"
      - name: "email_format"
        rule_type: "pattern"
        column: "email"
        parameters:
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        threshold: 0.8
        severity: "warning"
      - name: "age_range"
        rule_type: "range"
        column: "age"
        parameters:
          min_value: 0
          max_value: 120
        threshold: 0.9
        severity: "error"
```

Save the above as `demo_config.yaml` and use it with the `DataValidator` as shown below.

## Basic Usage

```python
from data_validator import DataValidator
import pandas as pd

# Load configuration from YAML
validator = DataValidator("demo_config.yaml")

# Create sample data
data = pd.DataFrame({
    "id": [1, 2, 3, None, 5],
    "email": ["a@example.com", "invalid", "c@example.com", "d@example.com", None],
    "age": [25, 130, 35, 40, 28],
})

# Run validation and generate report
summary = validator.validate_table(data, "users")
report = validator.get_validation_report(summary)
print(report)
```

## Template Code Blocks

### Validate Multiple Tables

```python
sources = {
    "users": users_df,
    "orders": orders_df,
}
results = validator.validate_all_tables(sources)
```

### Apply Filters

```python
clean_df = validator.apply_filters(data, "users")
```

### Generate Validation Report

```python
report = validator.get_validation_report(summary)
```

For more examples see the `examples/` directory.
