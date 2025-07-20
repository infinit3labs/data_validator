# Data Validator

A flexible data validation module that uses YAML configuration to apply data quality rules against Spark DataFrames in Delta Live Tables. The module supports multiple compute engines including PySpark, DuckDB, and Polars, and integrates with Databricks labs DQX package.

## Features

- **YAML Configuration**: Define data quality rules using intuitive YAML configuration files
- **Multi-Engine Support**: Execute validation rules using PySpark, DuckDB, or Polars
- **Delta Live Tables Integration**: Native support for Delta Live Tables workflows
- **Databricks DQX Integration**: Leverage DQX for advanced data quality monitoring
- **Flexible Rule Types**: Support for completeness, uniqueness, range, pattern, and custom validation rules
- **Filter Mode**: Apply validation rules as filters to clean data
- **Comprehensive Reporting**: Generate detailed validation reports with metrics and insights

## Installation

```bash
# Basic installation
pip install data-validator

# With specific engine dependencies
pip install data-validator[spark]     # For PySpark
pip install data-validator[duckdb]    # For DuckDB
pip install data-validator[polars]    # For Polars
pip install data-validator[all]       # All engines

# Development installation
pip install data-validator[dev]       # Includes testing and linting tools
```

## Quick Start

### 1. Create a YAML Configuration

```yaml
# config.yaml
version: "1.0"

engine:
  type: "pyspark"
  connection_params:
    spark.sql.adaptive.enabled: "true"

dqx:
  enabled: true
  metrics_table: "data_quality.metrics"

tables:
  - name: "customers"
    rules:
      - name: "customer_id_not_null"
        rule_type: "completeness"
        column: "customer_id"
        severity: "error"
      
      - name: "email_format"
        rule_type: "pattern"
        column: "email"
        parameters:
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        threshold: 0.95
        severity: "warning"
```

### 2. Validate Your Data

```python
from data_validator import DataValidator

# Initialize validator with configuration
validator = DataValidator("config.yaml")

# Validate a table
summary = validator.validate_table(df, "customers")

# Generate report
report = validator.get_validation_report(summary)
print(f"Success rate: {summary.overall_success_rate:.2%}")
```

### 3. Use as Data Filter

```python
# Apply rules as filters to clean data
clean_df = validator.apply_filters(df, "customers")
```

## Configuration Reference

### Engine Configuration

#### PySpark Engine
```yaml
engine:
  type: "pyspark"
  connection_params:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
  options:
    spark.sql.execution.arrow.pyspark.enabled: "true"
```

#### DuckDB Engine
```yaml
engine:
  type: "duckdb"
  connection_params:
    database: ":memory:"  # or path to database file
  options:
    threads: "4"
    memory_limit: "2GB"
```

#### Polars Engine
```yaml
engine:
  type: "polars"
  options:
    streaming: true
```

### Validation Rules

#### Completeness Rule
```yaml
- name: "field_completeness"
  rule_type: "completeness"
  column: "field_name"
  threshold: 0.95  # 95% of records must have non-null values
  severity: "error"
```

#### Uniqueness Rule
```yaml
- name: "id_uniqueness"
  rule_type: "uniqueness"
  column: "id"
  severity: "error"
```

#### Range Rule
```yaml
- name: "age_range"
  rule_type: "range"
  column: "age"
  parameters:
    min_value: 0
    max_value: 150
  threshold: 0.99
  severity: "warning"
```

#### Pattern Rule
```yaml
- name: "phone_pattern"
  rule_type: "pattern"
  column: "phone"
  parameters:
    pattern: "^\\+?[1-9]\\d{1,14}$"
  threshold: 0.90
  severity: "warning"
```

#### Custom Rule
```yaml
- name: "custom_business_rule"
  rule_type: "custom"
  expression: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
  threshold: 0.01  # Less than 1% negative amounts allowed
  severity: "error"
```

## Delta Live Tables Integration

```python
from data_validator import DataValidator

# In your DLT pipeline
validator = DataValidator("dlt_config.yaml")

@dlt.table(
    name="validated_customers",
    comment="Customer data with quality validation"
)
def validated_customers():
    df = spark.table("raw_customers")
    
    # Validate data
    summary = validator.validate_with_dlt(df, "customers", dlt_expectations=True)
    
    # Apply filters for clean data
    return validator.apply_filters(df, "customers")
```

## Multi-Engine Usage

### PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataValidation").getOrCreate()
df = spark.table("my_table")

validator = DataValidator("pyspark_config.yaml")
summary = validator.validate_table(df, "my_table")
```

### DuckDB
```python
import duckdb
import pandas as pd

df = pd.read_csv("data.csv")
validator = DataValidator("duckdb_config.yaml")
summary = validator.validate_table(df, "my_table")
```

### Polars
```python
import polars as pl

df = pl.read_csv("data.csv")
validator = DataValidator("polars_config.yaml")
summary = validator.validate_table(df, "my_table")
```

## Advanced Features

### Batch Validation
```python
# Validate multiple tables at once
data_sources = {
    "customers": customer_df,
    "orders": order_df,
    "products": product_df
}

results = validator.validate_all_tables(data_sources)
report = validator.get_validation_report(results)
```

### DQX Integration
```yaml
dqx:
  enabled: true
  output_path: "/dbfs/data_quality/results"
  metrics_table: "data_quality.validation_metrics"
  quarantine_table: "data_quality.quarantined_records"
```

### Custom Rule Extensions
```python
from data_validator.engines import ValidationEngine, ValidationResult

class CustomValidationEngine(ValidationEngine):
    def execute_rule(self, data, rule):
        # Implement custom validation logic
        pass
```

## Running on Databricks

You can execute validations as part of a Databricks job using the bundled CLI.

```bash
# On a Databricks cluster
pip install data-validator[all]  # install engine dependencies
data-validator-job --config /dbfs/path/to/config.yaml \
                   --report-path /dbfs/path/to/output/report.json
```

The CLI reads table data sources from the YAML configuration. You can override
any table source at runtime:

```bash
data-validator-job --config config.yaml \
                   --override customers=/dbfs/data/customers.csv
```

## API Reference

### DataValidator Class

#### Methods
- `validate_table(data, table_name, rules=None)`: Validate a single table
- `validate_all_tables(data_sources)`: Validate multiple tables
- `apply_filters(data, table_name, rules=None)`: Apply rules as filters
- `validate_with_dlt(data, table_name, dlt_expectations=True)`: DLT integration
- `get_validation_report(summaries)`: Generate comprehensive report

### ValidationConfig Class

#### Methods
- `from_yaml(yaml_path)`: Load configuration from YAML file
- `from_dict(data)`: Load configuration from dictionary
- `get_table_config(table_name)`: Get configuration for specific table
- `get_enabled_rules(table_name)`: Get enabled rules for table

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run tests: `pytest`
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- GitHub Issues: [Report bugs and request features](https://github.com/infinit3labs/data_validator/issues)
- Documentation: [Full documentation](https://github.com/infinit3labs/data_validator/wiki)