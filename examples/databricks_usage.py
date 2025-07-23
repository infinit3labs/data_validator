#!/usr/bin/env python3
"""
Example script demonstrating data_validator usage in Databricks cluster environment.

This example shows how to use the data validator with Databricks-specific features including:
- Unity Catalog integration
- Delta Lake validation
- Databricks widgets and secrets
- Cluster information and monitoring
- Job configuration for Databricks workflows
"""

import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import data_validator
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_validator import DataValidator, ValidationConfig
from data_validator.config import ValidationRule, TableConfig, EngineConfig, DQXConfig


def create_databricks_sample_config():
    """Create a sample Databricks configuration programmatically."""
    print("=== Creating Databricks Configuration ===")
    
    # Create Databricks-specific validation rules
    completeness_rule = ValidationRule(
        name="customer_id_completeness",
        description="Customer ID should not be null in Databricks",
        rule_type="completeness",
        column="customer_id",
        threshold=0.99,
        severity="error"
    )
    
    unity_catalog_rule = ValidationRule(
        name="unity_catalog_lineage",
        description="Validate Unity Catalog lineage",
        rule_type="unity_catalog_lineage",
        parameters={
            "databricks": {
                "catalog": "main",
                "schema": "customer_data",
                "check_lineage": True
            }
        },
        severity="info"
    )
    
    delta_quality_rule = ValidationRule(
        name="delta_table_quality",
        description="Validate Delta Lake specific metrics",
        rule_type="delta_quality",
        parameters={
            "databricks": {
                "check_delta_log": True,
                "check_file_stats": True,
                "validate_partitioning": True
            }
        },
        severity="info"
    )
    
    # Create table configuration
    table_config = TableConfig(
        name="customer_data",
        description="Customer data validation in Databricks",
        rules=[completeness_rule, unity_catalog_rule, delta_quality_rule]
    )
    
    # Create Databricks engine configuration
    engine_config = EngineConfig(
        type="databricks",
        connection_params={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.preview.enabled": "true"
        },
        options={
            "databricks.runtime.version": "13.3.x-scala2.12",
            "unity_catalog.enabled": "true"
        }
    )
    
    # Create DQX configuration for Databricks
    dqx_config = DQXConfig(
        enabled=True,
        output_path="/dbfs/mnt/data-quality/results",
        metrics_table="data_quality.databricks_validation_metrics",
        quarantine_table="data_quality.databricks_quarantined_records"
    )
    
    # Create complete configuration
    config = ValidationConfig(
        version="1.0",
        metadata={
            "description": "Databricks cluster validation config",
            "environment": "production"
        },
        engine=engine_config,
        dqx=dqx_config,
        tables=[table_config]
    )
    
    print(f"Created Databricks configuration with {len(config.tables)} table(s)")
    print(f"Engine type: {config.engine.type}")
    print(f"DQX enabled: {config.dqx.enabled}")
    
    return config


def demonstrate_databricks_features():
    """Demonstrate Databricks-specific features."""
    print("\n=== Databricks Features Demonstration ===")
    
    config = create_databricks_sample_config()
    
    try:
        # Create a Databricks validator
        validator = DataValidator(config)
        
        # Access the Databricks engine for special features
        if hasattr(validator.engine, 'get_cluster_info'):
            cluster_info = validator.engine.get_cluster_info()
            print("Cluster Information:")
            for key, value in cluster_info.items():
                print(f"  {key}: {value}")
        
        # Demonstrate widget access
        if hasattr(validator.engine, 'get_widget_value'):
            table_name = validator.engine.get_widget_value("table_name", "default_table")
            print(f"Widget value for 'table_name': {table_name}")
        
        # Demonstrate secret access
        if hasattr(validator.engine, 'get_secret'):
            # This would normally access a real secret scope
            secret_value = validator.engine.get_secret("my-scope", "api-key")
            print(f"Secret access result: {'Found' if secret_value else 'Not found'}")
        
        # Create a Databricks job configuration
        if hasattr(validator.engine, 'create_databricks_job_config'):
            job_config = validator.engine.create_databricks_job_config("/dbfs/config.yaml")
            print("Databricks Job Configuration Created:")
            print(f"  Job name: {job_config['name']}")
            print(f"  Cluster type: {job_config['new_cluster']['node_type_id']}")
            print(f"  Number of workers: {job_config['new_cluster']['num_workers']}")
        
        print("✅ Databricks features demonstration completed successfully")
        
    except ImportError as e:
        print(f"⚠️  PySpark not available for Databricks features: {e}")
        print("This is expected in environments without PySpark")
    except Exception as e:
        print(f"⚠️  Error demonstrating Databricks features: {e}")


def demonstrate_unity_catalog_integration():
    """Demonstrate Unity Catalog integration patterns."""
    print("\n=== Unity Catalog Integration ===")
    
    # Unity Catalog table source examples
    unity_catalog_sources = {
        "three_part_name": "main.customer_data.customers",
        "delta_table": {
            "type": "unity_catalog",
            "catalog": "main",
            "schema": "customer_data", 
            "table": "customers"
        },
        "volume_source": {
            "type": "volume",
            "catalog": "main",
            "schema": "data_ingestion",
            "volume": "raw_files",
            "file_path": "customers/customers.csv",
            "format": "csv"
        },
        "delta_path": {
            "type": "delta",
            "path": "/Volumes/main/customer_data/delta_tables/customers"
        }
    }
    
    print("Unity Catalog Source Examples:")
    for source_name, source_config in unity_catalog_sources.items():
        print(f"  {source_name}: {source_config}")
    
    print("✅ Unity Catalog integration patterns demonstrated")


def demonstrate_databricks_job_workflow():
    """Demonstrate how to set up data validation as a Databricks job."""
    print("\n=== Databricks Job Workflow ===")
    
    # Example job script that would run in Databricks
    job_script = '''
# This script would be saved as /dbfs/databricks/scripts/run_validation.py
import sys
from data_validator import DataValidator

def main():
    """Main validation function for Databricks job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run data validation in Databricks")
    parser.add_argument("--config", required=True, help="Path to validation config")
    parser.add_argument("--table", help="Specific table to validate")
    parser.add_argument("--engine", default="databricks", help="Engine type")
    
    args = parser.parse_args()
    
    # Load configuration
    validator = DataValidator(args.config)
    
    # Get table name from widget if not provided
    if not args.table and hasattr(validator.engine, 'get_widget_value'):
        args.table = validator.engine.get_widget_value("table_name", "")
    
    if args.table:
        # Validate specific table
        # Load data from Unity Catalog
        df = validator.engine.load_data(args.table)
        summary = validator.validate_table(df, args.table)
        
        # Log results to Databricks
        if hasattr(validator.engine, 'log_to_databricks'):
            validator.engine.log_to_databricks(
                f"Validation completed: {summary.passed_rules}/{summary.total_rules} rules passed"
            )
        
        # Print summary
        print(f"Validation Summary for {args.table}:")
        print(f"  Total rules: {summary.total_rules}")
        print(f"  Passed: {summary.passed_rules}")
        print(f"  Failed: {summary.failed_rules}")
        print(f"  Success rate: {summary.overall_success_rate:.2%}")
        
        # Exit with error code if validation failed
        if summary.failed_rules > 0:
            sys.exit(1)
    else:
        print("No table specified for validation")
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
    
    print("Example Databricks Job Script:")
    print("```python")
    print(job_script)
    print("```")
    
    # Example Databricks job configuration
    config = create_databricks_sample_config()
    validator = DataValidator(config)
    
    if hasattr(validator.engine, 'create_databricks_job_config'):
        job_config = validator.engine.create_databricks_job_config("/dbfs/config/databricks_config.yaml")
        
        print("\nDatabricks Job Configuration JSON:")
        import json
        print(json.dumps(job_config, indent=2))
    
    print("✅ Databricks job workflow demonstrated")


def demonstrate_databricks_monitoring():
    """Demonstrate monitoring and logging in Databricks."""
    print("\n=== Databricks Monitoring & Logging ===")
    
    # Example validation results with Databricks metadata
    from data_validator.engines import ValidationResult, ValidationSummary
    
    # Mock results with Databricks-specific metadata
    results = [
        ValidationResult(
            rule_name="unity_catalog_lineage",
            rule_type="unity_catalog_lineage",
            passed=True,
            failed_count=0,
            total_count=1,
            success_rate=1.0,
            message="Unity Catalog lineage validation passed",
            severity="info",
            execution_time_ms=25.3,
            metadata={
                "engine": "databricks",
                "rule_type": "lineage",
                "databricks_runtime": True,
                "cluster_id": "0123-456789-abcdef",
                "catalog": "main",
                "schema": "customer_data"
            }
        ),
        ValidationResult(
            rule_name="delta_table_quality",
            rule_type="delta_quality",
            passed=True,
            failed_count=0,
            total_count=1,
            success_rate=1.0,
            message="Delta table quality validation passed",
            severity="info",
            execution_time_ms=42.7,
            metadata={
                "engine": "databricks",
                "rule_type": "delta_quality",
                "delta_version": "2.4.0",
                "file_count": 127,
                "partition_count": 12
            }
        )
    ]
    
    summary = ValidationSummary(
        table_name="customer_data",
        total_rules=2,
        passed_rules=2,
        failed_rules=0,
        warning_rules=0,
        error_rules=0,
        overall_success_rate=1.0,
        total_execution_time_ms=68.0,
        results=results
    )
    
    config = create_databricks_sample_config()
    validator = DataValidator(config)
    
    # Generate report
    report = validator.get_validation_report(summary)
    
    print("Databricks Validation Report:")
    print(f"  Engine: {report['engine_type']}")
    print(f"  Success rate: {report['overall_stats']['overall_success_rate']:.1%}")
    print(f"  Execution time: {report['overall_stats']['total_execution_time_ms']:.1f}ms")
    
    for table_name, table_result in report["table_results"].items():
        print(f"\n  Table: {table_name}")
        for rule in table_result['rules']:
            status = "✅ PASS" if rule['passed'] else "❌ FAIL"
            print(f"    {status} {rule['name']}: {rule['message']}")
    
    print("✅ Databricks monitoring demonstration completed")


def main():
    """Main demonstration function."""
    print("Databricks Data Validator Example")
    print("=================================")
    
    # Demonstrate various Databricks features
    create_databricks_sample_config()
    demonstrate_databricks_features()
    demonstrate_unity_catalog_integration()
    demonstrate_databricks_job_workflow()
    demonstrate_databricks_monitoring()
    
    print("\n=== Summary ===")
    print("This example demonstrated:")
    print("1. Creating Databricks-specific validation configurations")
    print("2. Databricks cluster and Unity Catalog integration")
    print("3. Delta Lake quality validation")
    print("4. Databricks job workflow setup")
    print("5. Monitoring and logging in Databricks environment")
    print("6. Widget and secret management")
    
    print("\n=== Deployment Instructions ===")
    print("To deploy data validation in Databricks:")
    print("1. Upload validation configuration to DBFS or Unity Catalog Volume")
    print("2. Install data-validator package: %pip install data-validator[spark]")
    print("3. Create Databricks job using the provided job configuration")
    print("4. Set up job parameters using widgets")
    print("5. Configure DQX integration for monitoring")
    print("6. Schedule job execution as needed")


if __name__ == "__main__":
    main()