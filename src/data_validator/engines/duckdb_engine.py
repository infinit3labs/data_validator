"""
DuckDB validation engine implementation.
"""

import time
from typing import Any, Dict, Optional, Union

try:
    import duckdb
    import pandas as pd
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None
    pd = None

from . import ValidationEngine, ValidationResult
from ..config import ValidationRule, EngineConfig


class DuckDBValidationEngine(ValidationEngine):
    """DuckDB implementation of validation engine."""
    
    def __init__(self, config: EngineConfig):
        if not DUCKDB_AVAILABLE:
            raise ImportError("DuckDB is not available. Install with: pip install duckdb pandas")
        
        super().__init__(config)
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
    
    def connect(self) -> None:
        """Establish DuckDB connection."""
        if self._connection is None:
            # Get database path from connection params or use in-memory
            db_path = self.config.connection_params.get("database", ":memory:")
            self._connection = duckdb.connect(db_path)
            
            # Apply DuckDB-specific configuration options (filter out engine-level options)
            engine_options = {'max_retries', 'retry_delay'}
            for key, value in self.config.options.items():
                if key not in engine_options:
                    self._connection.execute(f"SET {key} = '{value}'")
    
    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def load_data(self, source: Union[str, pd.DataFrame]) -> str:
        """Load data from source and return table name."""
        if isinstance(source, pd.DataFrame):
            # Register pandas DataFrame as a table
            table_name = f"temp_table_{int(time.time() * 1000)}"
            self._connection.register(table_name, source)
            return table_name
        
        elif isinstance(source, str):
            if source.endswith(('.csv', '.parquet', '.json')):
                # File path - create a table from file
                table_name = f"temp_table_{int(time.time() * 1000)}"
                if source.endswith('.csv'):
                    self._connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{source}')")
                elif source.endswith('.parquet'):
                    self._connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{source}')")
                elif source.endswith('.json'):
                    self._connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{source}')")
                return table_name
            else:
                # Assume it's already a table name
                return source
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
    
    def execute_rule(self, table_name: str, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule."""
        start_time = time.time()
        
        try:
            # Get total count
            total_count = self._connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            failed_count = 0
            
            if rule.rule_type == "completeness":
                # Check for null/missing values
                if rule.column:
                    failed_count = self._connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {rule.column} IS NULL"
                    ).fetchone()[0]
                else:
                    raise ValueError("Completeness rule requires a column name")
            
            elif rule.rule_type == "uniqueness":
                # Check for duplicate values
                if rule.column:
                    unique_count = self._connection.execute(
                        f"SELECT COUNT(DISTINCT {rule.column}) FROM {table_name}"
                    ).fetchone()[0]
                    failed_count = total_count - unique_count
                else:
                    raise ValueError("Uniqueness rule requires a column name")
            
            elif rule.rule_type == "range":
                # Check if values are within specified range
                if rule.column and "min_value" in rule.parameters and "max_value" in rule.parameters:
                    min_val = rule.parameters["min_value"]
                    max_val = rule.parameters["max_value"]
                    failed_count = self._connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {rule.column} < {min_val} OR {rule.column} > {max_val}"
                    ).fetchone()[0]
                else:
                    raise ValueError("Range rule requires column name and min_value/max_value parameters")
            
            elif rule.rule_type == "pattern":
                # Check if values match a regex pattern
                if rule.column and "pattern" in rule.parameters:
                    pattern = rule.parameters["pattern"]
                    failed_count = self._connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE NOT regexp_matches({rule.column}, '{pattern}')"
                    ).fetchone()[0]
                else:
                    raise ValueError("Pattern rule requires column name and pattern parameter")
            
            elif rule.rule_type == "custom":
                # Execute custom SQL expression
                if rule.expression:
                    query = rule.expression.replace("{table}", table_name)
                    failed_count = self._connection.execute(query).fetchone()[0]
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
                metadata={"engine": "duckdb", "rule_parameters": rule.parameters}
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
                metadata={"engine": "duckdb", "error": str(e)}
            )
    
    def apply_filter(self, table_name: str, rule: ValidationRule) -> str:
        """Apply validation rule as a filter and return new table name."""
        filtered_table = f"filtered_{table_name}_{int(time.time() * 1000)}"
        
        try:
            if rule.rule_type == "completeness":
                if rule.column:
                    self._connection.execute(
                        f"CREATE TABLE {filtered_table} AS SELECT * FROM {table_name} WHERE {rule.column} IS NOT NULL"
                    )
            
            elif rule.rule_type == "range":
                if rule.column and "min_value" in rule.parameters and "max_value" in rule.parameters:
                    min_val = rule.parameters["min_value"]
                    max_val = rule.parameters["max_value"]
                    self._connection.execute(
                        f"CREATE TABLE {filtered_table} AS SELECT * FROM {table_name} WHERE {rule.column} >= {min_val} AND {rule.column} <= {max_val}"
                    )
            
            elif rule.rule_type == "pattern":
                if rule.column and "pattern" in rule.parameters:
                    pattern = rule.parameters["pattern"]
                    self._connection.execute(
                        f"CREATE TABLE {filtered_table} AS SELECT * FROM {table_name} WHERE regexp_matches({rule.column}, '{pattern}')"
                    )
            
            elif rule.rule_type == "custom":
                if rule.expression:
                    # For custom filters, assume the expression defines a WHERE clause
                    where_clause = rule.expression.replace("{table}", table_name)
                    self._connection.execute(
                        f"CREATE TABLE {filtered_table} AS SELECT * FROM {table_name} WHERE {where_clause}"
                    )
            
            else:
                # If no filter can be applied, create a copy of the original table
                self._connection.execute(
                    f"CREATE TABLE {filtered_table} AS SELECT * FROM {table_name}"
                )
            
            return filtered_table
        
        except Exception:
            # If filtering fails, return original table
            return table_name
    
    def get_dataframe(self, table_name: str) -> pd.DataFrame:
        """Get DataFrame from table name."""
        return self._connection.execute(f"SELECT * FROM {table_name}").df()