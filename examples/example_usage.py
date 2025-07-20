#!/usr/bin/env python3
"""
Example script demonstrating data_validator usage with different engines.
"""

import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import data_validator
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_validator import DataValidator, ValidationConfig
from data_validator.config import ValidationRule, TableConfig, EngineConfig, DQXConfig


def create_sample_data():
    """Create sample data for demonstration."""
    try:
        import pandas as pd
        
        # Sample customer data
        customers_data = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5, None, 7, 8, 9, 10],
            'name': ['Alice', 'Bob', 'Charlie', None, 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
            'email': ['alice@email.com', 'bob@email.com', 'invalid-email', 'dave@email.com', 
                     'eve@email.com', 'frank@email.com', 'grace@email.com', 'henry@email.com', 
                     'ivy@email.com', 'jack@email.com'],
            'age': [25, 30, 35, 40, 22, 28, 33, 38, 155, 29]  # 155 is invalid age
        })
        
        return customers_data
    except ImportError:
        print("Pandas not available. Creating mock data structure.")
        return None


def demonstrate_config_creation():
    """Demonstrate creating configuration programmatically."""
    print("=== Creating Configuration Programmatically ===")
    
    # Create validation rules
    completeness_rule = ValidationRule(
        name="customer_id_completeness",
        description="Customer ID should not be null",
        rule_type="completeness",
        column="customer_id",
        threshold=0.9,
        severity="error"
    )
    
    email_pattern_rule = ValidationRule(
        name="email_pattern",
        description="Email should follow valid format",
        rule_type="pattern",
        column="email",
        parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
        threshold=0.8,
        severity="warning"
    )
    
    age_range_rule = ValidationRule(
        name="age_range",
        description="Age should be between 0 and 120",
        rule_type="range",
        column="age",
        parameters={"min_value": 0, "max_value": 120},
        threshold=0.95,
        severity="error"
    )
    
    # Create table configuration
    table_config = TableConfig(
        name="customers",
        description="Customer data validation",
        rules=[completeness_rule, email_pattern_rule, age_range_rule]
    )
    
    # Create engine configuration (using DuckDB for demonstration)
    engine_config = EngineConfig(
        type="duckdb",
        connection_params={"database": ":memory:"},
        options={}
    )
    
    # Create DQX configuration
    dqx_config = DQXConfig(
        enabled=False,  # Disabled for this example
        output_path="/tmp/dqx_results"
    )
    
    # Create complete configuration
    config = ValidationConfig(
        version="1.0",
        metadata={"description": "Programmatically created config"},
        engine=engine_config,
        dqx=dqx_config,
        tables=[table_config]
    )
    
    print(f"Created configuration with {len(config.tables)} table(s)")
    print(f"Engine type: {config.engine.type}")
    print(f"Total rules for customers table: {len(table_config.rules)}")
    
    return config


def demonstrate_yaml_config():
    """Demonstrate loading configuration from YAML."""
    print("\n=== Loading Configuration from YAML ===")
    
    # Use the sample config file
    config_path = Path(__file__).parent / "sample_config.yaml"
    
    if config_path.exists():
        try:
            config = ValidationConfig.from_yaml(config_path)
            print(f"Loaded configuration from {config_path}")
            print(f"Engine type: {config.engine.type}")
            print(f"Number of tables: {len(config.tables)}")
            print(f"DQX enabled: {config.dqx.enabled}")
            return config
        except Exception as e:
            print(f"Error loading YAML config: {e}")
            return None
    else:
        print(f"Sample config file not found at {config_path}")
        return None


def demonstrate_validation_with_mock_engine():
    """Demonstrate validation with a mock engine (no external dependencies)."""
    print("\n=== Demonstrating Validation Logic ===")
    
    config = demonstrate_config_creation()
    
    # Create a validator
    try:
        # This will fail if DuckDB is not installed, which is expected
        validator = DataValidator(config)
        print("DataValidator created successfully")
        
        # For demonstration, we'll show how the validation would work
        # without actually executing it (since we may not have the dependencies)
        print("Validation rules configured:")
        for table in config.tables:
            print(f"  Table: {table.name}")
            for rule in table.rules:
                print(f"    - {rule.name} ({rule.rule_type}): {rule.severity}")
        
    except ImportError as e:
        print(f"Engine dependencies not available: {e}")
        print("This is expected in environments without DuckDB/Polars/PySpark")
        print("The configuration structure is still valid.")


def demonstrate_report_generation():
    """Demonstrate report generation with mock data."""
    print("\n=== Demonstrating Report Generation ===")
    
    # Import the result classes
    from data_validator.engines import ValidationResult, ValidationSummary
    
    # Create mock validation results
    results = [
        ValidationResult(
            rule_name="customer_id_completeness",
            rule_type="completeness",
            passed=False,
            failed_count=1,
            total_count=10,
            success_rate=0.9,
            message="1 out of 10 records have null customer_id",
            severity="error",
            execution_time_ms=15.2,
            metadata={"engine": "duckdb"}
        ),
        ValidationResult(
            rule_name="email_pattern",
            rule_type="pattern",
            passed=False,
            failed_count=1,
            total_count=10,
            success_rate=0.9,
            message="1 out of 10 records have invalid email format",
            severity="warning",
            execution_time_ms=8.7,
            metadata={"engine": "duckdb"}
        ),
        ValidationResult(
            rule_name="age_range",
            rule_type="range",
            passed=False,
            failed_count=1,
            total_count=10,
            success_rate=0.9,
            message="1 out of 10 records have age outside valid range",
            severity="error",
            execution_time_ms=12.3,
            metadata={"engine": "duckdb"}
        )
    ]
    
    # Create validation summary
    summary = ValidationSummary(
        table_name="customers",
        total_rules=3,
        passed_rules=0,
        failed_rules=3,
        warning_rules=1,
        error_rules=2,
        overall_success_rate=0.0,
        total_execution_time_ms=36.2,
        results=results
    )
    
    # Create validator to generate report
    config = demonstrate_config_creation()
    validator = DataValidator(config)
    
    # Generate report
    report = validator.get_validation_report(summary)
    
    print("Validation Report:")
    print(f"  Total tables: {report['total_tables']}")
    print(f"  Overall success rate: {report['overall_stats']['overall_success_rate']:.1%}")
    print(f"  Total execution time: {report['overall_stats']['total_execution_time_ms']:.1f}ms")
    
    for table_name, table_result in report["table_results"].items():
        print(f"\n  Table: {table_name}")
        print(f"    Rules: {table_result['passed_rules']}/{table_result['total_rules']} passed")
        print(f"    Success rate: {table_result['success_rate']:.1%}")
        
        for rule in table_result['rules']:
            status = "✅ PASS" if rule['passed'] else "❌ FAIL"
            print(f"      {status} {rule['name']} ({rule['severity']}): {rule['message']}")


def main():
    """Main demonstration function."""
    print("Data Validator Example Script")
    print("============================")
    
    # Demonstrate different ways to create and use configurations
    demonstrate_config_creation()
    demonstrate_yaml_config()
    demonstrate_validation_with_mock_engine()
    demonstrate_report_generation()
    
    print("\n=== Summary ===")
    print("This example demonstrated:")
    print("1. Creating validation configurations programmatically")
    print("2. Loading configurations from YAML files")
    print("3. Setting up validation rules and engines")
    print("4. Generating validation reports")
    print("\nTo run actual validation, install the required engine dependencies:")
    print("  pip install duckdb pandas  # For DuckDB engine")
    print("  pip install polars         # For Polars engine")
    print("  pip install pyspark        # For PySpark engine")


if __name__ == "__main__":
    main()