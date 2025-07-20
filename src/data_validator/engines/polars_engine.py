"""
Polars validation engine implementation.
"""

import time
import re
from typing import Any, Dict, Optional, Union

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

from . import ValidationEngine, ValidationResult
from ..config import ValidationRule, EngineConfig


class PolarsValidationEngine(ValidationEngine):
    """Polars implementation of validation engine."""
    
    def __init__(self, config: EngineConfig):
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available. Install with: pip install polars")
        
        super().__init__(config)
    
    def connect(self) -> None:
        """No explicit connection needed for Polars."""
        pass
    
    def disconnect(self) -> None:
        """No explicit disconnection needed for Polars."""
        pass
    
    def load_data(self, source: Union[str, pl.DataFrame]) -> pl.DataFrame:
        """Load data from source."""
        if isinstance(source, pl.DataFrame):
            return source
        elif isinstance(source, str):
            if source.endswith('.csv'):
                return pl.read_csv(source)
            elif source.endswith('.parquet'):
                return pl.read_parquet(source)
            elif source.endswith('.json'):
                return pl.read_json(source)
            else:
                raise ValueError(f"Unsupported file format or table source: {source}")
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
    
    def execute_rule(self, data: pl.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule."""
        start_time = time.time()
        
        try:
            total_count = data.height
            failed_count = 0
            
            if rule.rule_type == "completeness":
                # Check for null/missing values
                if rule.column:
                    if rule.column in data.columns:
                        failed_count = data.filter(pl.col(rule.column).is_null()).height
                    else:
                        raise ValueError(f"Column '{rule.column}' not found in data")
                else:
                    raise ValueError("Completeness rule requires a column name")
            
            elif rule.rule_type == "uniqueness":
                # Check for duplicate values
                if rule.column:
                    if rule.column in data.columns:
                        unique_count = data.select(rule.column).unique().height
                        failed_count = total_count - unique_count
                    else:
                        raise ValueError(f"Column '{rule.column}' not found in data")
                else:
                    raise ValueError("Uniqueness rule requires a column name")
            
            elif rule.rule_type == "range":
                # Check if values are within specified range
                if rule.column and "min_value" in rule.parameters and "max_value" in rule.parameters:
                    if rule.column in data.columns:
                        min_val = rule.parameters["min_value"]
                        max_val = rule.parameters["max_value"]
                        failed_count = data.filter(
                            ~((pl.col(rule.column) >= min_val) & (pl.col(rule.column) <= max_val))
                        ).height
                    else:
                        raise ValueError(f"Column '{rule.column}' not found in data")
                else:
                    raise ValueError("Range rule requires column name and min_value/max_value parameters")
            
            elif rule.rule_type == "pattern":
                # Check if values match a regex pattern
                if rule.column and "pattern" in rule.parameters:
                    if rule.column in data.columns:
                        pattern = rule.parameters["pattern"]
                        # Convert string column to check pattern
                        failed_count = data.filter(
                            ~pl.col(rule.column).cast(pl.Utf8).str.contains(pattern)
                        ).height
                    else:
                        raise ValueError(f"Column '{rule.column}' not found in data")
                else:
                    raise ValueError("Pattern rule requires column name and pattern parameter")
            
            elif rule.rule_type == "custom":
                # Execute custom expression using Polars expressions
                if rule.expression:
                    # This is a simplified custom expression handler
                    # In a real implementation, you'd need a more sophisticated expression parser
                    try:
                        # Try to evaluate as a Polars expression
                        # For now, we'll handle simple cases
                        if ">" in rule.expression and rule.column:
                            # Simple comparison: column > value
                            parts = rule.expression.split(">")
                            if len(parts) == 2:
                                col_name = parts[0].strip()
                                value = float(parts[1].strip())
                                failed_count = data.filter(~(pl.col(col_name) > value)).height
                        elif "<" in rule.expression and rule.column:
                            # Simple comparison: column < value
                            parts = rule.expression.split("<")
                            if len(parts) == 2:
                                col_name = parts[0].strip()
                                value = float(parts[1].strip())
                                failed_count = data.filter(~(pl.col(col_name) < value)).height
                        else:
                            raise ValueError("Complex custom expressions not yet supported in Polars engine")
                    except Exception as e:
                        raise ValueError(f"Failed to execute custom expression: {str(e)}")
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
                metadata={"engine": "polars", "rule_parameters": rule.parameters}
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
                metadata={"engine": "polars", "error": str(e)}
            )
    
    def apply_filter(self, data: pl.DataFrame, rule: ValidationRule) -> pl.DataFrame:
        """Apply validation rule as a filter."""
        try:
            if rule.rule_type == "completeness":
                if rule.column and rule.column in data.columns:
                    return data.filter(pl.col(rule.column).is_not_null())
            
            elif rule.rule_type == "range":
                if rule.column and rule.column in data.columns and "min_value" in rule.parameters and "max_value" in rule.parameters:
                    min_val = rule.parameters["min_value"]
                    max_val = rule.parameters["max_value"]
                    return data.filter((pl.col(rule.column) >= min_val) & (pl.col(rule.column) <= max_val))
            
            elif rule.rule_type == "pattern":
                if rule.column and rule.column in data.columns and "pattern" in rule.parameters:
                    pattern = rule.parameters["pattern"]
                    return data.filter(pl.col(rule.column).cast(pl.Utf8).str.contains(pattern))
            
            elif rule.rule_type == "custom":
                if rule.expression:
                    # Simple custom expression handling
                    if ">" in rule.expression and rule.column:
                        parts = rule.expression.split(">")
                        if len(parts) == 2:
                            col_name = parts[0].strip()
                            value = float(parts[1].strip())
                            return data.filter(pl.col(col_name) > value)
                    elif "<" in rule.expression and rule.column:
                        parts = rule.expression.split("<")
                        if len(parts) == 2:
                            col_name = parts[0].strip()
                            value = float(parts[1].strip())
                            return data.filter(pl.col(col_name) < value)
            
            # If no filter can be applied, return original data
            return data
        
        except Exception:
            # If filtering fails, return original data
            return data