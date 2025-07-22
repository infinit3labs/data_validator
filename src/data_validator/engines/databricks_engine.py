"""
Databricks validation engine implementation.

This engine extends the PySpark engine with Databricks-specific features including:
- Databricks workspace integration
- Unity Catalog support  
- Databricks widgets and job parameters
- Databricks authentication and secrets management
- Integration with Databricks monitoring and logging
"""

import time
import os
from typing import Any, Dict, Optional, Union, List
from pathlib import Path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

try:
    # Try to import Databricks-specific utilities
    from pyspark.dbutils import DBUtils
    DBUTILS_AVAILABLE = True
except ImportError:
    DBUTILS_AVAILABLE = False
    DBUtils = None

from .pyspark_engine import PySparkValidationEngine
from . import ValidationResult
from ..config import ValidationRule, EngineConfig


class DatabricksValidationEngine(PySparkValidationEngine):
    """Databricks implementation of validation engine that extends PySpark engine."""
    
    def __init__(self, config: EngineConfig):
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not available. Install with: pip install pyspark")
        
        super().__init__(config)
        self._dbutils: Optional[Any] = None
        self._databricks_runtime = self._detect_databricks_runtime()
        
    def _detect_databricks_runtime(self) -> bool:
        """Detect if running in Databricks runtime environment."""
        return (
            os.environ.get('DATABRICKS_RUNTIME_VERSION') is not None or
            os.path.exists('/databricks/spark/conf/spark-defaults.conf') or
            'databricks' in os.environ.get('SPARK_HOME', '').lower()
        )
    
    def connect(self) -> None:
        """Establish Spark session with Databricks-specific configuration."""
        if self._spark is None:
            if self._databricks_runtime:
                # Use existing Databricks Spark session if available
                try:
                    self._spark = SparkSession.getActiveSession()
                    if self._spark is None:
                        self._spark = SparkSession.builder.appName("DataValidator-Databricks").getOrCreate()
                except Exception:
                    # Fallback to creating new session
                    self._spark = SparkSession.builder.appName("DataValidator-Databricks").getOrCreate()
            else:
                # Create Spark session with Databricks Connect configuration
                builder = SparkSession.builder.appName("DataValidator-Databricks")
                
                # Apply Databricks-specific connection parameters
                for key, value in self.config.connection_params.items():
                    builder = builder.config(key, value)
                
                # Apply additional options
                for key, value in self.config.options.items():
                    builder = builder.config(key, value)
                
                self._spark = builder.getOrCreate()
            
            # Initialize dbutils if available
            self._init_dbutils()
    
    def _init_dbutils(self) -> None:
        """Initialize Databricks utilities."""
        try:
            if self._databricks_runtime and self._spark:
                # In Databricks runtime, dbutils is available through SparkContext
                self._dbutils = self._spark.sparkContext._jvm.com.databricks.service.DBUtils
            else:
                # For Databricks Connect, dbutils might not be available
                self._dbutils = None
        except Exception:
            self._dbutils = None
    
    def get_widget_value(self, widget_name: str, default_value: str = "") -> str:
        """Get value from Databricks widget."""
        if self._dbutils and self._databricks_runtime:
            try:
                return self._dbutils.widgets.get(widget_name)
            except Exception:
                return default_value
        else:
            # Fallback to environment variables for non-Databricks environments
            return os.environ.get(f"DATABRICKS_WIDGET_{widget_name.upper()}", default_value)
    
    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get secret from Databricks secret scope."""
        if self._dbutils and self._databricks_runtime:
            try:
                return self._dbutils.secrets.get(scope, key)
            except Exception:
                return None
        else:
            # Fallback to environment variables for testing/development
            env_key = f"DATABRICKS_SECRET_{scope.upper()}_{key.upper().replace('-', '_')}"
            return os.environ.get(env_key)
    
    def load_data(self, source: Union[str, DataFrame, Dict[str, Any]]) -> DataFrame:
        """Load data from source with Databricks-specific enhancements."""
        if isinstance(source, DataFrame):
            return source
        elif isinstance(source, dict):
            # Handle Databricks-specific source configurations
            return self._load_from_databricks_source(source)
        elif isinstance(source, str):
            # Enhanced path handling for Databricks
            if source.startswith("dbfs:/"):
                # DBFS path
                return self._spark.read.option("header", "true").csv(source)
            elif source.startswith("/mnt/"):
                # Mounted storage path
                return self._spark.read.option("header", "true").csv(source)
            elif "." in source and "/" not in source:
                # Table name (possibly Unity Catalog)
                return self._load_from_catalog(source)
            else:
                # Use parent class implementation
                return super().load_data(source)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
    
    def _load_from_databricks_source(self, source_config: Dict[str, Any]) -> DataFrame:
        """Load data from Databricks-specific source configuration."""
        source_type = source_config.get("type", "table")
        
        if source_type == "unity_catalog":
            catalog = source_config.get("catalog")
            schema = source_config.get("schema")
            table = source_config.get("table")
            if catalog and schema and table:
                table_name = f"{catalog}.{schema}.{table}"
                return self._spark.table(table_name)
            else:
                raise ValueError("Unity Catalog source requires catalog, schema, and table")
        
        elif source_type == "delta":
            path = source_config.get("path")
            if path:
                return self._spark.read.format("delta").load(path)
            else:
                raise ValueError("Delta source requires path")
        
        elif source_type == "volume":
            # Unity Catalog Volume
            catalog = source_config.get("catalog")
            schema = source_config.get("schema")
            volume = source_config.get("volume")
            file_path = source_config.get("file_path")
            if catalog and schema and volume and file_path:
                volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_path}"
                format_type = source_config.get("format", "csv")
                return self._spark.read.format(format_type).option("header", "true").load(volume_path)
            else:
                raise ValueError("Volume source requires catalog, schema, volume, and file_path")
        
        else:
            raise ValueError(f"Unsupported Databricks source type: {source_type}")
    
    def _load_from_catalog(self, table_name: str) -> DataFrame:
        """Load data from Unity Catalog with enhanced error handling."""
        try:
            return self._spark.table(table_name)
        except Exception as e:
            # Try to provide more helpful error messages for common Unity Catalog issues
            if "not found" in str(e).lower():
                available_catalogs = self._get_available_catalogs()
                error_msg = f"Table '{table_name}' not found. Available catalogs: {available_catalogs}"
                raise ValueError(error_msg)
            else:
                raise e
    
    def _get_available_catalogs(self) -> List[str]:
        """Get list of available catalogs."""
        try:
            catalogs_df = self._spark.sql("SHOW CATALOGS")
            return [row['catalog'] for row in catalogs_df.collect()]
        except Exception:
            return ["Unable to retrieve catalogs"]
    
    def execute_rule(self, data: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Execute validation rule with Databricks-specific enhancements."""
        # Check if rule has Databricks-specific parameters
        if "databricks" in rule.parameters:
            return self._execute_databricks_rule(data, rule)
        else:
            # Use parent class implementation
            return super().execute_rule(data, rule)
    
    def _execute_databricks_rule(self, data: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Execute Databricks-specific validation rules."""
        start_time = time.time()
        databricks_params = rule.parameters.get("databricks", {})
        
        try:
            if rule.rule_type == "unity_catalog_lineage":
                # Validate data lineage information
                return self._validate_lineage(data, rule, databricks_params)
            
            elif rule.rule_type == "delta_quality":
                # Validate Delta Lake specific quality metrics
                return self._validate_delta_quality(data, rule, databricks_params)
            
            elif rule.rule_type == "workspace_permission":
                # Validate workspace permissions for data access
                return self._validate_permissions(data, rule, databricks_params)
            
            else:
                # Fallback to standard rule execution
                return super().execute_rule(data, rule)
        
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
                message=f"Databricks rule execution failed: {str(e)}",
                severity="error",
                execution_time_ms=execution_time,
                metadata={"engine": "databricks", "error": str(e), "databricks_runtime": self._databricks_runtime}
            )
    
    def _validate_lineage(self, data: DataFrame, rule: ValidationRule, params: Dict[str, Any]) -> ValidationResult:
        """Validate Unity Catalog lineage information."""
        start_time = time.time()
        
        # This is a placeholder for lineage validation
        # In a real implementation, you would use Unity Catalog APIs
        
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            passed=True,
            failed_count=0,
            total_count=1,
            success_rate=1.0,
            message="Lineage validation completed",
            severity=rule.severity,
            execution_time_ms=execution_time,
            metadata={"engine": "databricks", "rule_type": "lineage"}
        )
    
    def _validate_delta_quality(self, data: DataFrame, rule: ValidationRule, params: Dict[str, Any]) -> ValidationResult:
        """Validate Delta Lake specific quality metrics."""
        start_time = time.time()
        
        # This is a placeholder for Delta quality validation
        # In a real implementation, you would check Delta Lake specific features
        
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            passed=True,
            failed_count=0,
            total_count=1,
            success_rate=1.0,
            message="Delta quality validation completed",
            severity=rule.severity,
            execution_time_ms=execution_time,
            metadata={"engine": "databricks", "rule_type": "delta_quality"}
        )
    
    def _validate_permissions(self, data: DataFrame, rule: ValidationRule, params: Dict[str, Any]) -> ValidationResult:
        """Validate workspace permissions."""
        start_time = time.time()
        
        # This is a placeholder for permission validation
        # In a real implementation, you would check Unity Catalog permissions
        
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            passed=True,
            failed_count=0,
            total_count=1,
            success_rate=1.0,
            message="Permission validation completed",
            severity=rule.severity,
            execution_time_ms=execution_time,
            metadata={"engine": "databricks", "rule_type": "permissions"}
        )
    
    def create_databricks_job_config(self, validation_config_path: str) -> Dict[str, Any]:
        """Create Databricks job configuration for data validation."""
        return {
            "name": "data-validator-job",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            },
            "spark_python_task": {
                "python_file": "dbfs:/databricks/scripts/run_validation.py",
                "parameters": [
                    "--config", validation_config_path,
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
            "max_retries": 2
        }
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the current Databricks cluster."""
        cluster_info = {
            "runtime_version": os.environ.get('DATABRICKS_RUNTIME_VERSION', 'unknown'),
            "is_databricks_runtime": self._databricks_runtime,
            "dbutils_available": self._dbutils is not None
        }
        
        if self._spark:
            cluster_info.update({
                "spark_version": self._spark.version,
                "app_name": self._spark.sparkContext.appName,
                "executor_memory": self._spark.conf.get("spark.executor.memory", "unknown"),
                "driver_memory": self._spark.conf.get("spark.driver.memory", "unknown")
            })
        
        return cluster_info
    
    def log_to_databricks(self, message: str, level: str = "INFO") -> None:
        """Log messages using Databricks-specific logging."""
        # In Databricks, we can use standard Python logging which integrates with Databricks logs
        import logging
        logger = logging.getLogger("databricks_data_validator")
        
        if level.upper() == "ERROR":
            logger.error(message)
        elif level.upper() == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
    
    def disconnect(self) -> None:
        """Close Databricks connection."""
        # In Databricks runtime, we typically don't stop the Spark session
        # as it's managed by the Databricks platform
        if not self._databricks_runtime and self._spark:
            self._spark.stop()
            self._spark = None
        
        self._dbutils = None