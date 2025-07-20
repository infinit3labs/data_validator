"""
PySpark validation engine implementation.
"""

import time
from typing import Any, Dict, Optional, Union

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

from . import ValidationEngine, ValidationResult
from ..config import ValidationRule, EngineConfig


class PySparkValidationEngine(ValidationEngine):
    """PySpark implementation of validation engine."""
    
    def __init__(self, config: EngineConfig):
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not available. Install with: pip install pyspark")
        
        super().__init__(config)
        self._spark: Optional[SparkSession] = None
    
    def connect(self) -> None:
        """Establish Spark session."""
        if self._spark is None:
            builder = SparkSession.builder.appName("DataValidator")
            
            # Apply connection parameters
            for key, value in self.config.connection_params.items():
                builder = builder.config(key, value)
            
            # Apply additional options
            for key, value in self.config.options.items():
                builder = builder.config(key, value)
            
            self._spark = builder.getOrCreate()
    
    def disconnect(self) -> None:
        """Stop Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
    
    def load_data(self, source: Union[str, DataFrame]) -> DataFrame:
        """Load data from source."""
        if isinstance(source, DataFrame):
            return source
        elif isinstance(source, str):
            # Assume it's a table name or file path
            if "." in source and "/" in source:
                # File path
                return self._spark.read.option("header", "true").csv(source)
            else:
                # Table name
                return self._spark.table(source)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
    
    def execute_rule(self, data: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule."""
        start_time = time.time()
        
        try:
            total_count = data.count()
            failed_count = 0
            
            if rule.rule_type == "completeness":
                # Check for null/missing values
                if rule.column:
                    failed_count = data.filter(col(rule.column).isNull() | isnan(col(rule.column))).count()
                else:
                    raise ValueError("Completeness rule requires a column name")
            
            elif rule.rule_type == "uniqueness":
                # Check for duplicate values
                if rule.column:
                    unique_count = data.select(rule.column).distinct().count()
                    failed_count = total_count - unique_count
                else:
                    raise ValueError("Uniqueness rule requires a column name")
            
            elif rule.rule_type == "range":
                # Check if values are within specified range
                if rule.column and "min_value" in rule.parameters and "max_value" in rule.parameters:
                    min_val = rule.parameters["min_value"]
                    max_val = rule.parameters["max_value"]
                    failed_count = data.filter(
                        ~((col(rule.column) >= min_val) & (col(rule.column) <= max_val))
                    ).count()
                else:
                    raise ValueError("Range rule requires column name and min_value/max_value parameters")
            
            elif rule.rule_type == "pattern":
                # Check if values match a regex pattern
                if rule.column and "pattern" in rule.parameters:
                    pattern = rule.parameters["pattern"]
                    failed_count = data.filter(~col(rule.column).rlike(pattern)).count()
                else:
                    raise ValueError("Pattern rule requires column name and pattern parameter")
            
            elif rule.rule_type == "custom":
                # Execute custom SQL expression
                if rule.expression:
                    # Create temporary view for the data
                    temp_view = f"temp_view_{int(time.time() * 1000)}"
                    data.createOrReplaceTempView(temp_view)
                    
                    # Execute custom expression
                    result_df = self._spark.sql(rule.expression.replace("{table}", temp_view))
                    failed_count = result_df.count()
                    
                    # Clean up
                    self._spark.catalog.dropTempView(temp_view)
                else:
                    raise ValueError("Custom rule requires an expression")
            
            else:
                raise ValueError(f"Unsupported rule type: {rule.rule_type}")
            
            # Calculate success rate
            success_rate = (total_count - failed_count) / total_count if total_count > 0 else 1.0
            
            # Determine if rule passed based on threshold
            passed = True
            if rule.threshold is not None:
                passed = success_rate >= rule.threshold
            else:
                passed = failed_count == 0
            
            end_time = time.time()
            execution_time = (end_time - start_time) * 1000
            
            message = f"Rule '{rule.name}': {failed_count}/{total_count} failed records"
            if rule.threshold:
                message += f" (success rate: {success_rate:.2%}, threshold: {rule.threshold:.2%})"
            
            return ValidationResult(
                rule_name=rule.name,
                rule_type=rule.rule_type,
                passed=passed,
                failed_count=failed_count,
                total_count=total_count,
                success_rate=success_rate,
                message=message,
                severity=rule.severity,
                execution_time_ms=execution_time,
                metadata={"engine": "pyspark", "rule_parameters": rule.parameters}
            )
        
        except Exception as e:
            end_time = time.time()
            execution_time = (end_time - start_time) * 1000
            
            return ValidationResult(
                rule_name=rule.name,
                rule_type=rule.rule_type,
                passed=False,
                failed_count=-1,
                total_count=-1,
                success_rate=0.0,
                message=f"Rule execution failed: {str(e)}",
                severity="error",
                execution_time_ms=execution_time,
                metadata={"engine": "pyspark", "error": str(e)}
            )
    
    def apply_filter(self, data: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply validation rule as a filter."""
        if rule.rule_type == "completeness":
            if rule.column:
                return data.filter(col(rule.column).isNotNull() & ~isnan(col(rule.column)))
        
        elif rule.rule_type == "range":
            if rule.column and "min_value" in rule.parameters and "max_value" in rule.parameters:
                min_val = rule.parameters["min_value"]
                max_val = rule.parameters["max_value"]
                return data.filter((col(rule.column) >= min_val) & (col(rule.column) <= max_val))
        
        elif rule.rule_type == "pattern":
            if rule.column and "pattern" in rule.parameters:
                pattern = rule.parameters["pattern"]
                return data.filter(col(rule.column).rlike(pattern))
        
        elif rule.rule_type == "custom":
            if rule.expression:
                # For custom filters, assume the expression defines a WHERE clause
                return data.filter(rule.expression)
        
        # If no filter can be applied, return original data
        return data