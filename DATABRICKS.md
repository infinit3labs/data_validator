# Databricks Cluster Mode Documentation

This document provides comprehensive guidance on using the data_validator package in Databricks cluster environments.

## Overview

The data_validator package now includes a dedicated `databricks` engine that extends the PySpark engine with Databricks-specific features:

- **Unity Catalog Integration**: Native support for three-part table names and catalog operations
- **Delta Lake Validation**: Specialized validation rules for Delta Lake tables
- **Cluster Integration**: Automatic detection of Databricks runtime and cluster information
- **Widget & Secret Management**: Access to Databricks widgets and secret scopes
- **Job Management**: Utilities for creating and managing Databricks validation jobs
- **Streaming Support**: Built-in support for streaming data validation

## Quick Start

### 1. Basic Configuration

Create a YAML configuration file with the Databricks engine:

```yaml
version: "1.0"

engine:
  type: "databricks"
  connection_params:
    spark.sql.adaptive.enabled: "true"
    spark.databricks.delta.preview.enabled: "true"
  options:
    unity_catalog.enabled: "true"

tables:
  - name: "customers"
    rules:
      - name: "customer_id_not_null"
        rule_type: "completeness"
        column: "customer_id"
        severity: "error"
```

### 2. Basic Usage

```python
from data_validator import DataValidator

# Initialize with Databricks configuration
validator = DataValidator("databricks_config.yaml")

# Validate Unity Catalog table
df = spark.table("main.customer_data.customers")
summary = validator.validate_table(df, "customers")

print(f"Success rate: {summary.overall_success_rate:.2%}")
```

## Unity Catalog Integration

### Table Reference Formats

The Databricks engine supports multiple ways to reference tables:

```python
# Three-part table name
df = validator.engine.load_data("main.customer_data.customers")

# Unity Catalog configuration
unity_source = {
    "type": "unity_catalog",
    "catalog": "main",
    "schema": "customer_data",
    "table": "customers"
}
df = validator.engine.load_data(unity_source)

# Delta table path
delta_source = {
    "type": "delta",
    "path": "/delta/path/to/table"
}
df = validator.engine.load_data(delta_source)

# Unity Catalog Volume
volume_source = {
    "type": "volume",
    "catalog": "main",
    "schema": "data_ingestion",
    "volume": "raw_files",
    "file_path": "customers.csv",
    "format": "csv"
}
df = validator.engine.load_data(volume_source)
```

## Databricks-Specific Validation Rules

### Unity Catalog Lineage Validation

```yaml
- name: "lineage_check"
  rule_type: "unity_catalog_lineage"
  parameters:
    databricks:
      catalog: "main"
      schema: "customer_data"
      check_lineage: true
  severity: "info"
```

### Delta Lake Quality Validation

```yaml
- name: "delta_quality"
  rule_type: "delta_quality"
  parameters:
    databricks:
      check_delta_log: true
      check_file_stats: true
      validate_partitioning: true
  severity: "info"
```

### Workspace Permission Validation

```yaml
- name: "permissions_check"
  rule_type: "workspace_permission"
  parameters:
    databricks:
      required_permissions: ["SELECT", "USE_SCHEMA"]
      check_catalog_access: true
  severity: "info"
```

## Widget and Secret Management

### Accessing Databricks Widgets

```python
# Get widget value with default
table_name = validator.engine.get_widget_value("table_name", "default_table")

# Use widget values in validation
config_path = validator.engine.get_widget_value("config_path", "/dbfs/config.yaml")
```

### Accessing Databricks Secrets

```python
# Get secret from scope
api_key = validator.engine.get_secret("my-scope", "api-key")

# Use secrets in configuration
database_password = validator.engine.get_secret("db-secrets", "password")
```

## Job Deployment

### Using the Job Manager

```python
from data_validator.databricks_utils import DatabricksJobManager

# Create job manager
manager = DatabricksJobManager(
    workspace_url="https://your-workspace.cloud.databricks.com",
    token="your-access-token"
)

# Create batch validation job
job_config = manager.create_validation_job(
    job_name="daily-data-validation",
    config_path="/dbfs/config/validation_config.yaml",
    schedule={
        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
        "timezone_id": "UTC"
    }
)

# Deploy using Databricks CLI or API
# databricks jobs create --json-file job_config.json
```

### Streaming Validation Job

```python
# Create streaming validation job
stream_config = {
    "source_table": "streaming_events",
    "checkpoint_location": "/mnt/checkpoints/validation",
    "trigger_interval": 60  # seconds
}

streaming_job = manager.create_streaming_validation_job(
    "realtime-validation",
    "/dbfs/config/streaming_config.yaml",
    stream_config
)
```

### Delta Live Tables Integration

```python
# Create DLT pipeline
dlt_config = manager.create_dlt_pipeline_config(
    "data-quality-pipeline",
    {
        "target_database": "data_quality",
        "storage_location": "/mnt/dlt-storage",
        "config_path": "/dbfs/config/dlt_config.yaml"
    }
)
```

## Cluster Information and Monitoring

### Getting Cluster Information

```python
# Get cluster details
cluster_info = validator.engine.get_cluster_info()

print(f"Runtime version: {cluster_info['runtime_version']}")
print(f"Spark version: {cluster_info['spark_version']}")
print(f"Is Databricks runtime: {cluster_info['is_databricks_runtime']}")
```

### Databricks Logging

```python
# Log messages to Databricks logs
validator.engine.log_to_databricks("Validation started", "INFO")
validator.engine.log_to_databricks("Critical validation failure", "ERROR")
```

## Complete Example: Production Pipeline

### 1. Configuration File (`databricks_production_config.yaml`)

```yaml
version: "1.0"
metadata:
  description: "Production data validation for customer data"
  environment: "production"

engine:
  type: "databricks"
  connection_params:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.databricks.delta.preview.enabled: "true"
  options:
    unity_catalog.enabled: "true"

dqx:
  enabled: true
  output_path: "/mnt/data-quality/results"
  metrics_table: "data_quality.validation_metrics"
  quarantine_table: "data_quality.quarantined_records"

tables:
  - name: "customer_data"
    rules:
      - name: "customer_id_completeness"
        rule_type: "completeness"
        column: "customer_id"
        threshold: 0.99
        severity: "error"
      
      - name: "email_format"
        rule_type: "pattern"
        column: "email"
        parameters:
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        threshold: 0.95
        severity: "warning"
      
      - name: "unity_catalog_lineage"
        rule_type: "unity_catalog_lineage"
        parameters:
          databricks:
            catalog: "main"
            schema: "customer_data"
            check_lineage: true
        severity: "info"

global_rules:
  - name: "workspace_permissions"
    rule_type: "workspace_permission"
    parameters:
      databricks:
        required_permissions: ["SELECT", "USE_SCHEMA"]
    severity: "info"
```

### 2. Validation Script (`validate_production_data.py`)

```python
#!/usr/bin/env python3
"""
Production data validation script for Databricks.
"""

import sys
from data_validator import DataValidator

def main():
    """Main validation function."""
    # Get configuration from widget
    config_path = dbutils.widgets.get("config_path")
    table_name = dbutils.widgets.get("table_name")
    
    print(f"Starting validation with config: {config_path}")
    
    # Initialize validator
    validator = DataValidator(config_path)
    
    # Get cluster information
    cluster_info = validator.engine.get_cluster_info()
    validator.engine.log_to_databricks(
        f"Validation started on cluster {cluster_info.get('app_name', 'unknown')}"
    )
    
    try:
        if table_name:
            # Validate specific table
            df = spark.table(table_name)
            summary = validator.validate_table(df, table_name)
            
            # Log results
            message = f"Validation completed for {table_name}: {summary.passed_rules}/{summary.total_rules} rules passed"
            validator.engine.log_to_databricks(message)
            
            # Fail job if validation fails
            if summary.failed_rules > 0:
                error_msg = f"Validation failed for {table_name}: {summary.failed_rules} rules failed"
                validator.engine.log_to_databricks(error_msg, "ERROR")
                raise Exception(error_msg)
        
        else:
            # Validate all configured tables
            results = {}
            for table_config in validator.config.tables:
                table_name = table_config.name
                df = spark.table(f"main.customer_data.{table_name}")
                summary = validator.validate_table(df, table_name)
                results[table_name] = summary
            
            # Generate overall report
            report = validator.get_validation_report(results)
            overall_success = report['overall_stats']['overall_success_rate']
            
            message = f"Overall validation success rate: {overall_success:.2%}"
            validator.engine.log_to_databricks(message)
            
            # Check for failures
            failed_tables = [
                name for name, summary in results.items() 
                if summary.failed_rules > 0
            ]
            
            if failed_tables:
                error_msg = f"Validation failed for tables: {', '.join(failed_tables)}"
                validator.engine.log_to_databricks(error_msg, "ERROR")
                raise Exception(error_msg)
    
    except Exception as e:
        validator.engine.log_to_databricks(f"Validation error: {str(e)}", "ERROR")
        raise
    
    print("✅ All validations completed successfully")

if __name__ == "__main__":
    main()
```

### 3. Job Configuration

```python
from data_validator.databricks_utils import DatabricksJobManager

manager = DatabricksJobManager()

# Production job configuration
job_config = manager.create_validation_job(
    job_name="production-data-validation",
    config_path="/dbfs/config/databricks_production_config.yaml",
    cluster_config={
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 3,
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.preview.enabled": "true"
        }
    },
    schedule={
        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
        "timezone_id": "UTC"
    }
)

# Save job configuration
import json
with open("production_validation_job.json", "w") as f:
    json.dump(job_config, f, indent=2)

print("Job configuration saved to production_validation_job.json")
```

## Best Practices

### 1. Configuration Management
- Store configurations in Unity Catalog Volumes for version control
- Use environment-specific configurations (dev, staging, prod)
- Leverage Databricks widgets for parameterization

### 2. Resource Management
- Use appropriate cluster sizes for your data volume
- Enable adaptive query execution for better performance
- Consider spot instances for cost optimization

### 3. Monitoring and Alerting
- Set up email notifications for job failures
- Use DQX integration for advanced monitoring
- Log validation results to Delta tables for analysis

### 4. Security
- Use Databricks secrets for sensitive information
- Implement proper Unity Catalog permissions
- Follow least-privilege access principles

### 5. Error Handling
- Implement retry logic for transient failures
- Separate validation failures from system errors
- Provide meaningful error messages and logging

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure PySpark is available in the cluster
2. **Permission Errors**: Check Unity Catalog and workspace permissions
3. **Configuration Errors**: Validate YAML syntax and required fields
4. **Memory Issues**: Adjust cluster configuration for large datasets

### Performance Optimization

1. **Caching**: Cache frequently accessed DataFrames
2. **Partitioning**: Use appropriate partitioning strategies
3. **Adaptive Query Execution**: Enable Spark AQE features
4. **Resource Tuning**: Monitor and optimize cluster resources

## Support and Documentation

- **Package Documentation**: See main README.md for general usage
- **Databricks Documentation**: Refer to Databricks official documentation for platform-specific features
- **Examples**: Check the `examples/` directory for additional usage patterns
- **Tests**: Review `tests/test_databricks.py` for implementation details

For issues and questions, please refer to the project's issue tracker or documentation.