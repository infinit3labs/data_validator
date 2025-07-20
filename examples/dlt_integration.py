"""
Example of integrating data_validator with Databricks Delta Live Tables (DLT).

This example shows how to use the data validator within DLT pipelines
for data quality validation and expectations.
"""

# Note: This is an example file and requires Databricks environment to run
# It demonstrates the integration patterns for DLT

try:
    import dlt
    from pyspark.sql import SparkSession
    DLT_AVAILABLE = True
except ImportError:
    DLT_AVAILABLE = False
    print("This example requires Databricks DLT environment")

from data_validator import DataValidator


# Example DLT configuration for data validation
DLT_CONFIG = {
    "version": "1.0",
    "engine": {
        "type": "pyspark",
        "connection_params": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    },
    "dqx": {
        "enabled": True,
        "metrics_table": "data_quality.dlt_validation_metrics",
        "quarantine_table": "data_quality.dlt_quarantined_records"
    },
    "tables": [
        {
            "name": "bronze_customers",
            "description": "Bronze layer customer data validation",
            "rules": [
                {
                    "name": "customer_id_not_null",
                    "rule_type": "completeness",
                    "column": "customer_id",
                    "threshold": 1.0,
                    "severity": "error"
                },
                {
                    "name": "email_format",
                    "rule_type": "pattern",
                    "column": "email",
                    "parameters": {
                        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                    },
                    "threshold": 0.95,
                    "severity": "warning"
                }
            ]
        },
        {
            "name": "silver_customers",
            "description": "Silver layer customer data validation",
            "rules": [
                {
                    "name": "customer_id_unique",
                    "rule_type": "uniqueness",
                    "column": "customer_id",
                    "severity": "error"
                },
                {
                    "name": "age_reasonable",
                    "rule_type": "range",
                    "column": "age",
                    "parameters": {"min_value": 0, "max_value": 120},
                    "threshold": 0.99,
                    "severity": "error"
                }
            ]
        }
    ]
}


def create_dlt_validator():
    """Create a data validator configured for DLT."""
    return DataValidator(DLT_CONFIG)


# Example DLT pipeline using data_validator
if DLT_AVAILABLE:
    
    @dlt.table(
        name="bronze_customers",
        comment="Raw customer data from source systems",
        table_properties={
            "quality": "bronze"
        }
    )
    def bronze_customers():
        """Bronze layer: Raw customer data ingestion."""
        # Read from source (e.g., cloud storage, CDC, etc.)
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/tmp/schemas/customers")
            .load("/tmp/data/raw/customers/")
        )
    
    
    @dlt.table(
        name="silver_customers",
        comment="Validated and cleaned customer data",
        table_properties={
            "quality": "silver"
        }
    )
    @dlt.expect_all_or_drop({
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_email": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'"
    })
    def silver_customers():
        """Silver layer: Validated customer data with quality checks."""
        validator = create_dlt_validator()
        
        # Get bronze data
        bronze_df = dlt.read("bronze_customers")
        
        # Run validation
        summary = validator.validate_with_dlt(bronze_df, "bronze_customers", dlt_expectations=True)
        
        # Log validation results
        print(f"Validation summary: {summary.passed_rules}/{summary.total_rules} rules passed")
        
        # Apply filters to clean data
        cleaned_df = validator.apply_filters(bronze_df, "silver_customers")
        
        return cleaned_df
    
    
    @dlt.table(
        name="gold_customer_metrics",
        comment="Customer data quality metrics",
        table_properties={
            "quality": "gold"
        }
    )
    def gold_customer_metrics():
        """Gold layer: Aggregated customer metrics with quality indicators."""
        validator = create_dlt_validator()
        
        # Get silver data
        silver_df = dlt.read("silver_customers")
        
        # Run final validation
        summary = validator.validate_table(silver_df, "silver_customers")
        
        # Create quality metrics
        return spark.sql(f"""
            SELECT 
                current_timestamp() as metric_timestamp,
                'customers' as table_name,
                {summary.total_rules} as total_rules,
                {summary.passed_rules} as passed_rules,
                {summary.overall_success_rate} as success_rate,
                '{summary.table_name}' as source_table
        """)


# Example usage patterns for DLT integration
class DLTDataValidatorIntegration:
    """Helper class for DLT and data_validator integration."""
    
    def __init__(self, config_path: str):
        self.validator = DataValidator(config_path)
    
    def create_dlt_expectations(self, table_name: str):
        """
        Create DLT expectations from validation rules.
        
        Returns dictionary of expectation name -> SQL expression
        """
        rules = self.validator.config.get_enabled_rules(table_name)
        expectations = {}
        
        for rule in rules:
            if rule.rule_type == "completeness":
                expectations[f"valid_{rule.column}"] = f"{rule.column} IS NOT NULL"
            
            elif rule.rule_type == "range" and "min_value" in rule.parameters and "max_value" in rule.parameters:
                min_val = rule.parameters["min_value"]
                max_val = rule.parameters["max_value"]
                expectations[f"valid_{rule.column}_range"] = f"{rule.column} BETWEEN {min_val} AND {max_val}"
            
            elif rule.rule_type == "pattern" and "pattern" in rule.parameters:
                pattern = rule.parameters["pattern"]
                expectations[f"valid_{rule.column}_pattern"] = f"{rule.column} RLIKE '{pattern}'"
        
        return expectations
    
    def validate_and_quarantine(self, df, table_name: str):
        """
        Validate data and separate good/bad records.
        
        Returns tuple of (good_df, bad_df)
        """
        # Run validation
        summary = self.validator.validate_table(df, table_name)
        
        # Apply filters to get good data
        good_df = self.validator.apply_filters(df, table_name)
        
        # Get bad data by anti-join
        bad_df = df.join(good_df, on=df.columns, how="left_anti")
        
        return good_df, bad_df
    
    def create_quality_metrics(self, df, table_name: str):
        """Create quality metrics for monitoring."""
        summary = self.validator.validate_table(df, table_name)
        
        metrics = []
        for result in summary.results:
            metrics.append({
                "table_name": table_name,
                "rule_name": result.rule_name,
                "rule_type": result.rule_type,
                "passed": result.passed,
                "success_rate": result.success_rate,
                "failed_count": result.failed_count,
                "total_count": result.total_count,
                "execution_time_ms": result.execution_time_ms,
                "severity": result.severity
            })
        
        return spark.createDataFrame(metrics)


# Example configuration for different DLT layers
MULTI_LAYER_DLT_CONFIG = {
    "version": "1.0",
    "engine": {"type": "pyspark"},
    "dqx": {"enabled": True},
    "tables": [
        {
            "name": "bronze_layer",
            "description": "Bronze layer validation - basic format checks",
            "rules": [
                {
                    "name": "required_fields_present",
                    "rule_type": "completeness",
                    "column": "id",
                    "severity": "error"
                }
            ]
        },
        {
            "name": "silver_layer",
            "description": "Silver layer validation - business rules",
            "rules": [
                {
                    "name": "data_freshness",
                    "rule_type": "custom",
                    "expression": "SELECT COUNT(*) FROM {table} WHERE created_date < DATE_SUB(CURRENT_DATE(), 7)",
                    "threshold": 0.05,
                    "severity": "warning"
                },
                {
                    "name": "id_uniqueness",
                    "rule_type": "uniqueness",
                    "column": "id",
                    "severity": "error"
                }
            ]
        },
        {
            "name": "gold_layer",
            "description": "Gold layer validation - final quality checks",
            "rules": [
                {
                    "name": "completeness_check",
                    "rule_type": "custom",
                    "expression": "SELECT COUNT(*) FROM {table} WHERE id IS NULL OR name IS NULL OR email IS NULL",
                    "threshold": 0.01,
                    "severity": "error"
                }
            ]
        }
    ]
}


def example_dlt_pipeline_with_validation():
    """Example showing complete DLT pipeline with validation at each layer."""
    print("Example DLT Pipeline with Data Validation")
    print("==========================================")
    
    print("This example demonstrates:")
    print("1. Bronze layer: Basic format validation")
    print("2. Silver layer: Business rule validation") 
    print("3. Gold layer: Final quality assurance")
    print("4. Quality metrics tracking")
    print("5. Data quarantine for failed records")
    
    print("\nDLT Tables that would be created:")
    print("- bronze_customers (raw data)")
    print("- silver_customers (validated data)")
    print("- gold_customer_metrics (quality metrics)")
    print("- quarantine_customers (failed records)")
    
    print("\nValidation rules applied:")
    config = DLT_CONFIG
    for table in config["tables"]:
        print(f"\n{table['name']}:")
        for rule in table["rules"]:
            print(f"  - {rule['name']} ({rule['rule_type']}): {rule['severity']}")
    
    print("\nIntegration benefits:")
    print("- Automated data quality enforcement")
    print("- Consistent validation across pipeline layers")
    print("- Quality metrics for monitoring")
    print("- Failed record quarantine and analysis")
    print("- DLT expectations from validation rules")


if __name__ == "__main__":
    example_dlt_pipeline_with_validation()