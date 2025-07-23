"""
Databricks job deployment utilities for data validation.

This module provides utilities to deploy and manage data validation jobs
in Databricks environments including job creation, scheduling, and monitoring.
"""

import json
import os
from typing import Dict, Any, List, Optional
from pathlib import Path


class DatabricksJobManager:
    """Manager for Databricks data validation jobs."""
    
    def __init__(self, workspace_url: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize Databricks job manager.
        
        Args:
            workspace_url: Databricks workspace URL (optional, can use environment variable)
            token: Databricks personal access token (optional, can use environment variable)
        """
        self.workspace_url = workspace_url or os.environ.get('DATABRICKS_HOST')
        self.token = token or os.environ.get('DATABRICKS_TOKEN')
        
        if not self.workspace_url or not self.token:
            print("Warning: Databricks credentials not fully configured. "
                  "Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
    
    def create_validation_job(self, 
                            job_name: str,
                            config_path: str,
                            cluster_config: Optional[Dict[str, Any]] = None,
                            schedule: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a Databricks job for data validation.
        
        Args:
            job_name: Name of the job
            config_path: Path to validation configuration file
            cluster_config: Optional cluster configuration (uses default if not provided)
            schedule: Optional schedule configuration
        
        Returns:
            Job configuration dictionary
        """
        default_cluster = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.databricks.delta.preview.enabled": "true"
            },
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 50
            }
        }
        
        job_config = {
            "name": job_name,
            "new_cluster": cluster_config or default_cluster,
            "spark_python_task": {
                "python_file": "dbfs:/databricks/scripts/run_validation.py",
                "parameters": [
                    "--config", config_path,
                    "--engine", "databricks"
                ]
            },
            "libraries": [
                {
                    "pypi": {
                        "package": "data-validator[spark]"
                    }
                }
            ],
            "timeout_seconds": 3600,
            "max_retries": 2,
            "retry_on_timeout": True,
            "email_notifications": {
                "on_failure": ["data-team@company.com"],
                "on_success": ["data-team@company.com"]
            }
        }
        
        if schedule:
            job_config["schedule"] = schedule
        
        return job_config
    
    def create_streaming_validation_job(self,
                                      job_name: str,
                                      config_path: str,
                                      stream_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Databricks job for streaming data validation.
        
        Args:
            job_name: Name of the streaming job
            config_path: Path to validation configuration
            stream_config: Streaming-specific configuration
        
        Returns:
            Streaming job configuration
        """
        cluster_config = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 3,
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
                "spark.databricks.delta.preview.enabled": "true"
            }
        }
        
        job_config = {
            "name": job_name,
            "new_cluster": cluster_config,
            "spark_python_task": {
                "python_file": "dbfs:/databricks/scripts/run_streaming_validation.py",
                "parameters": [
                    "--config", config_path,
                    "--stream-source", stream_config.get("source_table"),
                    "--checkpoint-location", stream_config.get("checkpoint_location"),
                    "--trigger-interval", str(stream_config.get("trigger_interval", 60))
                ]
            },
            "libraries": [
                {
                    "pypi": {
                        "package": "data-validator[spark]"
                    }
                }
            ],
            "timeout_seconds": 0,  # Streaming jobs run indefinitely
            "max_retries": 5,
            "email_notifications": {
                "on_failure": ["data-team@company.com"]
            }
        }
        
        return job_config
    
    def create_dlt_pipeline_config(self,
                                 pipeline_name: str,
                                 source_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create Delta Live Tables pipeline configuration with data validation.
        
        Args:
            pipeline_name: Name of the DLT pipeline
            source_config: Source configuration for the pipeline
        
        Returns:
            DLT pipeline configuration
        """
        pipeline_config = {
            "name": pipeline_name,
            "target": source_config.get("target_database", "data_quality"),
            "storage": source_config.get("storage_location", "/mnt/dlt-storage"),
            "configuration": {
                "bundle.sourcePath": "/Workspace/Repos/data-validation",
                "data_validator.config_path": source_config.get("config_path")
            },
            "libraries": [
                {
                    "notebook": {
                        "path": "/Repos/data-validation/notebooks/dlt_validation_pipeline"
                    }
                }
            ],
            "clusters": [
                {
                    "label": "default",
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.databricks.delta.preview.enabled": "true"
                    }
                }
            ],
            "development": True,
            "continuous": False
        }
        
        return pipeline_config
    
    def generate_job_script(self, script_type: str = "batch") -> str:
        """
        Generate Python script for Databricks job execution.
        
        Args:
            script_type: Type of script ('batch' or 'streaming')
        
        Returns:
            Python script content
        """
        if script_type == "batch":
            return self._generate_batch_script()
        elif script_type == "streaming":
            return self._generate_streaming_script()
        else:
            raise ValueError(f"Unsupported script type: {script_type}")
    
    def _generate_batch_script(self) -> str:
        """Generate batch validation script."""
        return '''
# Databricks notebook source
# MAGIC %md
# MAGIC # Data Validation Job for Databricks
# MAGIC 
# MAGIC This notebook runs data validation using the data_validator package in a Databricks environment.

# COMMAND ----------

# MAGIC %pip install data-validator[spark]

# COMMAND ----------

import sys
import argparse
from data_validator import DataValidator
from pyspark.sql import SparkSession

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("DataValidator-Databricks").getOrCreate()

# COMMAND ----------

def parse_arguments():
    """Parse command line arguments or use widgets."""
    try:
        # Try to get parameters from Databricks widgets
        dbutils.widgets.text("config", "/dbfs/config/databricks_config.yaml", "Configuration Path")
        dbutils.widgets.text("table_name", "", "Table Name (optional)")
        dbutils.widgets.text("engine", "databricks", "Engine Type")
        
        config_path = dbutils.widgets.get("config")
        table_name = dbutils.widgets.get("table_name")
        engine_type = dbutils.widgets.get("engine")
        
        return config_path, table_name, engine_type
    
    except:
        # Fallback to command line arguments
        parser = argparse.ArgumentParser(description="Run data validation in Databricks")
        parser.add_argument("--config", required=True, help="Path to validation config")
        parser.add_argument("--table", help="Specific table to validate")
        parser.add_argument("--engine", default="databricks", help="Engine type")
        
        args = parser.parse_args()
        return args.config, args.table, args.engine

# COMMAND ----------

def main():
    """Main validation function."""
    config_path, table_name, engine_type = parse_arguments()
    
    print(f"Starting data validation with config: {config_path}")
    
    # Load configuration
    validator = DataValidator(config_path)
    
    # Get cluster information
    if hasattr(validator.engine, 'get_cluster_info'):
        cluster_info = validator.engine.get_cluster_info()
        print("Cluster Information:")
        for key, value in cluster_info.items():
            print(f"  {key}: {value}")
    
    validation_results = {}
    
    if table_name:
        # Validate specific table
        print(f"Validating table: {table_name}")
        try:
            df = validator.engine.load_data(table_name)
            summary = validator.validate_table(df, table_name)
            validation_results[table_name] = summary
            
            print(f"Validation completed for {table_name}")
            print(f"  Success rate: {summary.overall_success_rate:.2%}")
            print(f"  Rules passed: {summary.passed_rules}/{summary.total_rules}")
            
        except Exception as e:
            print(f"Error validating table {table_name}: {str(e)}")
            raise
    else:
        # Validate all tables in configuration
        print("Validating all configured tables")
        for table_config in validator.config.tables:
            table_name = table_config.name
            print(f"Validating table: {table_name}")
            
            try:
                df = validator.engine.load_data(table_name)
                summary = validator.validate_table(df, table_name)
                validation_results[table_name] = summary
                
                print(f"  Success rate: {summary.overall_success_rate:.2%}")
                print(f"  Rules passed: {summary.passed_rules}/{summary.total_rules}")
                
            except Exception as e:
                print(f"Error validating table {table_name}: {str(e)}")
                # Continue with other tables
    
    # Generate overall report
    if validation_results:
        report = validator.get_validation_report(validation_results)
        print("\\n=== Validation Report ===")
        print(f"Total tables validated: {report['total_tables']}")
        print(f"Overall success rate: {report['overall_stats']['overall_success_rate']:.2%}")
        
        # Check if any validations failed
        failed_tables = []
        for table_name, summary in validation_results.items():
            if summary.failed_rules > 0:
                failed_tables.append(table_name)
        
        if failed_tables:
            print(f"\\nTables with failed validations: {', '.join(failed_tables)}")
            raise Exception(f"Data validation failed for {len(failed_tables)} table(s)")
        else:
            print("\\n✅ All validations passed successfully!")
    
    else:
        print("No tables were validated")

# COMMAND ----------

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Completion
# MAGIC 
# MAGIC The data validation job has completed. Check the output above for validation results.
'''
    
    def _generate_streaming_script(self) -> str:
        """Generate streaming validation script."""
        return '''
# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Data Validation Job for Databricks
# MAGIC 
# MAGIC This notebook runs continuous data validation on streaming data.

# COMMAND ----------

# MAGIC %pip install data-validator[spark]

# COMMAND ----------

import sys
from data_validator import DataValidator
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def parse_streaming_arguments():
    """Parse streaming job arguments."""
    try:
        dbutils.widgets.text("config", "/dbfs/config/streaming_config.yaml", "Configuration Path")
        dbutils.widgets.text("source_table", "", "Source Table/Stream")
        dbutils.widgets.text("checkpoint_location", "/tmp/checkpoints", "Checkpoint Location")
        dbutils.widgets.text("trigger_interval", "60", "Trigger Interval (seconds)")
        
        config_path = dbutils.widgets.get("config")
        source_table = dbutils.widgets.get("source_table")
        checkpoint_location = dbutils.widgets.get("checkpoint_location")
        trigger_interval = int(dbutils.widgets.get("trigger_interval"))
        
        return config_path, source_table, checkpoint_location, trigger_interval
    
    except:
        # Default values for testing
        return "/dbfs/config/streaming_config.yaml", "streaming_events", "/tmp/checkpoints", 60

# COMMAND ----------

def validate_streaming_data(df, validator, table_name):
    """Validate streaming DataFrame."""
    # Add processing timestamp
    df = df.withColumn("validation_timestamp", current_timestamp())
    
    # Run validation on micro-batch
    summary = validator.validate_table(df, table_name)
    
    print(f"Streaming validation for {table_name}:")
    print(f"  Success rate: {summary.overall_success_rate:.2%}")
    print(f"  Rules passed: {summary.passed_rules}/{summary.total_rules}")
    
    # Apply filters to get clean data
    clean_df = validator.apply_filters(df, table_name)
    
    return clean_df

# COMMAND ----------

def main():
    """Main streaming validation function."""
    config_path, source_table, checkpoint_location, trigger_interval = parse_streaming_arguments()
    
    print(f"Starting streaming validation with config: {config_path}")
    print(f"Source: {source_table}")
    print(f"Trigger interval: {trigger_interval} seconds")
    
    # Load configuration
    validator = DataValidator(config_path)
    
    # Read streaming data
    df = spark.readStream.table(source_table)
    
    # Define validation function for each micro-batch
    def process_batch(batch_df, batch_id):
        print(f"Processing batch {batch_id}")
        
        if batch_df.count() > 0:
            clean_df = validate_streaming_data(batch_df, validator, source_table)
            
            # Write validated data to target table
            (clean_df.write
             .mode("append")
             .option("mergeSchema", "true")
             .saveAsTable(f"{source_table}_validated"))
        
        print(f"Batch {batch_id} processed successfully")
    
    # Start streaming query
    query = (df.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpoint_location)
             .trigger(processingTime=f"{trigger_interval} seconds")
             .start())
    
    print("Streaming validation started. Waiting for termination...")
    query.awaitTermination()

# COMMAND ----------

if __name__ == "__main__":
    main()
'''
    
    def save_configuration_examples(self, output_dir: str = "/tmp/databricks_configs"):
        """Save example configurations to files."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Save batch job configuration
        batch_job = self.create_validation_job(
            "data-validation-batch",
            "/dbfs/config/databricks_config.yaml"
        )
        
        with open(output_path / "batch_job_config.json", "w") as f:
            json.dump(batch_job, f, indent=2)
        
        # Save streaming job configuration
        streaming_job = self.create_streaming_validation_job(
            "data-validation-streaming",
            "/dbfs/config/streaming_config.yaml",
            {
                "source_table": "streaming_events",
                "checkpoint_location": "/mnt/checkpoints/validation",
                "trigger_interval": 60
            }
        )
        
        with open(output_path / "streaming_job_config.json", "w") as f:
            json.dump(streaming_job, f, indent=2)
        
        # Save DLT pipeline configuration
        dlt_pipeline = self.create_dlt_pipeline_config(
            "data-validation-dlt",
            {
                "target_database": "data_quality",
                "storage_location": "/mnt/dlt-storage/validation",
                "config_path": "/dbfs/config/dlt_config.yaml"
            }
        )
        
        with open(output_path / "dlt_pipeline_config.json", "w") as f:
            json.dump(dlt_pipeline, f, indent=2)
        
        # Save job scripts
        with open(output_path / "batch_validation_script.py", "w") as f:
            f.write(self.generate_job_script("batch"))
        
        with open(output_path / "streaming_validation_script.py", "w") as f:
            f.write(self.generate_job_script("streaming"))
        
        print(f"Configuration examples saved to {output_path}")
        return output_path


def create_databricks_deployment_guide() -> str:
    """Create a deployment guide for Databricks data validation."""
    return '''
# Databricks Data Validation Deployment Guide

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace with appropriate permissions
2. **Unity Catalog**: (Optional) Unity Catalog enabled for advanced features
3. **Cluster Permissions**: Ability to create and manage clusters
4. **Storage Access**: Access to DBFS, cloud storage, or Unity Catalog Volumes

## Installation Steps

### 1. Install data-validator Package

```python
%pip install data-validator[spark]
```

### 2. Upload Configuration Files

Upload your validation configuration to one of:
- DBFS: `/dbfs/config/validation_config.yaml`
- Unity Catalog Volume: `/Volumes/catalog/schema/volume/config.yaml`
- Cloud Storage: `s3://bucket/config/validation_config.yaml`

### 3. Create Validation Scripts

Upload the generated validation scripts to:
- Batch validation: `/dbfs/databricks/scripts/run_validation.py`
- Streaming validation: `/dbfs/databricks/scripts/run_streaming_validation.py`

### 4. Configure Jobs

Use the generated job configurations to create Databricks jobs:

```bash
# Using Databricks CLI
databricks jobs create --json-file batch_job_config.json
databricks jobs create --json-file streaming_job_config.json
```

### 5. Set Up Monitoring

1. Configure email notifications in job settings
2. Set up DQX integration for advanced monitoring
3. Create alerts for validation failures

## Usage Patterns

### Batch Validation

```python
from data_validator import DataValidator

# Load configuration
validator = DataValidator("/dbfs/config/databricks_config.yaml")

# Validate Unity Catalog table
df = spark.table("main.customer_data.customers")
summary = validator.validate_table(df, "customers")

print(f"Success rate: {summary.overall_success_rate:.2%}")
```

### Streaming Validation

```python
# Read streaming data
stream_df = spark.readStream.table("streaming_events")

# Validate each micro-batch
def validate_batch(batch_df, batch_id):
    summary = validator.validate_table(batch_df, "streaming_events")
    clean_df = validator.apply_filters(batch_df, "streaming_events")
    
    # Write to validated table
    clean_df.write.mode("append").saveAsTable("streaming_events_validated")

# Start streaming
(stream_df.writeStream
 .foreachBatch(validate_batch)
 .start())
```

### Delta Live Tables Integration

```python
import dlt
from data_validator import DataValidator

validator = DataValidator("/dbfs/config/dlt_config.yaml")

@dlt.table
def validated_customers():
    df = dlt.read("raw_customers")
    summary = validator.validate_with_dlt(df, "customers")
    return validator.apply_filters(df, "customers")
```

## Best Practices

1. **Resource Management**: Use appropriate cluster sizes for your data volume
2. **Error Handling**: Implement proper error handling and retry logic
3. **Monitoring**: Set up comprehensive monitoring and alerting
4. **Testing**: Test validation rules in development environment first
5. **Documentation**: Document your validation rules and their business logic

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure data-validator is installed with spark extras
2. **Permission Errors**: Check Unity Catalog and workspace permissions
3. **Configuration Errors**: Validate YAML configuration syntax
4. **Memory Issues**: Adjust cluster configuration for large datasets

### Performance Optimization

1. **Caching**: Cache frequently accessed DataFrames
2. **Partitioning**: Use appropriate partitioning strategies
3. **Adaptive Query Execution**: Enable Spark AQE for better performance
4. **Resource Tuning**: Optimize cluster configuration based on workload

## Support

For issues and questions:
- Check the data-validator documentation
- Review Databricks-specific logs and metrics
- Contact your data platform team for workspace-specific guidance
'''


if __name__ == "__main__":
    # Example usage
    manager = DatabricksJobManager()
    
    # Create example configurations
    output_dir = manager.save_configuration_examples()
    print(f"Databricks configurations created in: {output_dir}")
    
    # Print deployment guide
    guide = create_databricks_deployment_guide()
    print("\\n" + guide)