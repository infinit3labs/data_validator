# Data Validator

A flexible data validation module that uses YAML configuration to apply data quality rules against Spark DataFrames in Delta Live Tables. The module supports multiple compute engines including PySpark, Databricks, DuckDB, and Polars, and integrates with Databricks labs DQX package.

## Features

- **YAML Configuration**: Define data quality rules using intuitive YAML configuration files
- **Multi-Engine Support**: Execute validation rules using PySpark, Databricks, DuckDB, or Polars
- **Databricks Integration**: Native support for Databricks clusters, Unity Catalog, and Delta Lake
- **Delta Live Tables Integration**: Native support for Delta Live Tables workflows
- **Databricks DQX Integration**: Leverage DQX for advanced data quality monitoring
- **Flexible Rule Types**: Support for completeness, uniqueness, range, pattern, and custom validation rules
- **Filter Mode**: Apply validation rules as filters to clean data
- **Environment Overrides**: Configure any option using `DV_` environment variables or Databricks widgets
- **Comprehensive Reporting**: Generate detailed validation reports with metrics and insights
- **Job Management**: Built-in utilities for deploying validation jobs in Databricks

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

For a guided tutorial see [docs/getting_started.md](docs/getting_started.md).

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

### 2.1 Override with Environment Variables

```bash
export DV_ENGINE__TYPE=duckdb
export DV_ENGINE__CONNECTION_PARAMS__DATABASE=":memory:"
validator = DataValidator("config.yaml")
```

### 3. Use as Data Filter

```python
# Apply rules as filters to clean data
clean_df = validator.apply_filters(df, "customers")
```

### 4. Environment Overrides and Widgets

Configuration values can be overridden using environment variables or Databricks
widgets when running on Databricks. Environment variable names mirror the YAML
structure using double underscores. For example:

```bash
export VALIDATOR_ENGINE__TYPE=duckdb
export VALIDATOR_DQX__ENABLED=false
```

You can also provide a widget named `engine` in Databricks to override the
engine type.

See [docs/config_merge.md](docs/config_merge.md) for more examples.


## Configuration Reference

### Engine Configuration

#### Databricks Engine
```yaml
engine:
  type: "databricks"
  connection_params:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.databricks.delta.preview.enabled: "true"
  options:
    databricks.runtime.version: "13.3.x-scala2.12"
    unity_catalog.enabled: "true"
```

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

#### Databricks-Specific Rules

```yaml
# Unity Catalog lineage validation
- name: "lineage_validation"
  rule_type: "unity_catalog_lineage"
  parameters:
    databricks:
      catalog: "main"
      schema: "customer_data"
      check_lineage: true
  severity: "info"

# Delta Lake quality validation
- name: "delta_quality"
  rule_type: "delta_quality"
  parameters:
    databricks:
      check_delta_log: true
      check_file_stats: true
      validate_partitioning: true
  severity: "info"
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

## Databricks Cluster Usage

The data validator provides native support for Databricks environments with enhanced features for Unity Catalog, Delta Lake, and cluster management.

### Basic Databricks Usage

```python
from data_validator import DataValidator

# Initialize validator with Databricks configuration
validator = DataValidator("databricks_config.yaml")

# Validate Unity Catalog table
df = spark.table("main.customer_data.customers")
summary = validator.validate_table(df, "customers")

# Get cluster information
cluster_info = validator.engine.get_cluster_info()
print(f"Runtime: {cluster_info['runtime_version']}")
```

### Unity Catalog Integration

```python
# Load data from Unity Catalog
unity_catalog_source = {
    "type": "unity_catalog",
    "catalog": "main",
    "schema": "customer_data",
    "table": "customers"
}

df = validator.engine.load_data(unity_catalog_source)
summary = validator.validate_table(df, "customers")
```

### Delta Lake Validation

```yaml
# Databricks-specific validation rules
tables:
  - name: "delta_table"
    rules:
      - name: "delta_quality_check"
        rule_type: "delta_quality"
        parameters:
          databricks:
            check_delta_log: true
            check_file_stats: true
            validate_partitioning: true
        severity: "info"
```

### Databricks Job Deployment

```python
from data_validator.databricks_utils import DatabricksJobManager

# Create job manager
manager = DatabricksJobManager()

# Create batch validation job
job_config = manager.create_validation_job(
    "data-validation-batch",
    "/dbfs/config/validation_config.yaml"
)

# Create streaming validation job
streaming_config = {
    "source_table": "streaming_events",
    "checkpoint_location": "/mnt/checkpoints",
    "trigger_interval": 60
}

streaming_job = manager.create_streaming_validation_job(
    "data-validation-streaming",
    "/dbfs/config/streaming_config.yaml",
    streaming_config
)
```

### Widget and Secret Management

```python
# Access Databricks widgets
table_name = validator.engine.get_widget_value("table_name", "default_table")

# Access Databricks secrets
api_key = validator.engine.get_secret("my-scope", "api-key")
```

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

## Databricks Job Usage

The package includes a helper module to execute validation in a Databricks job.
Prepare a YAML file listing your data sources and run:

```bash
python -m data_validator.databricks_job \
  --config /dbfs/path/to/config.yaml \
  --sources /dbfs/path/to/sources.yaml \
  --output /dbfs/path/to/report.json
```

Refer to `docs/databricks_job.md` for more details.

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
- `load_config(path=None)`: Merge YAML, widget, and environment sources
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
- Local Docs: [docs/getting_started.md](docs/getting_started.md)
