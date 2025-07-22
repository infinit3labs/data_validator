"""
Tests for Databricks validation engine and utilities.
"""

import pytest
import tempfile
import os
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from data_validator import DataValidator, ValidationConfig
from data_validator.config import ValidationRule, TableConfig, EngineConfig
from data_validator.engines import ValidationResult, ValidationSummary


def create_databricks_config():
    """Create a test Databricks configuration."""
    # Create Databricks-specific validation rules
    completeness_rule = ValidationRule(
        name="customer_id_completeness",
        rule_type="completeness",
        column="customer_id",
        severity="error"
    )
    
    unity_catalog_rule = ValidationRule(
        name="unity_catalog_lineage",
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
        rule_type="delta_quality",
        parameters={
            "databricks": {
                "check_delta_log": True,
                "check_file_stats": True
            }
        },
        severity="info"
    )
    
    table_config = TableConfig(
        name="test_table",
        rules=[completeness_rule, unity_catalog_rule, delta_quality_rule]
    )
    
    engine_config = EngineConfig(
        type="databricks",
        connection_params={
            "spark.sql.adaptive.enabled": "true",
            "spark.databricks.delta.preview.enabled": "true"
        },
        options={
            "unity_catalog.enabled": "true"
        }
    )
    
    return ValidationConfig(
        engine=engine_config,
        tables=[table_config]
    )


def test_databricks_engine_creation():
    """Test creating Databricks validation engine."""
    config = create_databricks_config()
    
    # Mock PySpark availability
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        assert validator.config.engine.type == "databricks"


def test_databricks_runtime_detection():
    """Test Databricks runtime detection."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        # Test detection with environment variable
        with patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '13.3.x-scala2.12'}):
            validator = DataValidator(config)
            engine = validator.engine
            assert engine._databricks_runtime is True
        
        # Test detection without environment variable
        with patch.dict(os.environ, {}, clear=True):
            with patch('os.path.exists', return_value=False):
                validator = DataValidator(config)
                engine = validator.engine
                assert engine._databricks_runtime is False


def test_databricks_widget_functionality():
    """Test Databricks widget functionality."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        engine = validator.engine
        
        # Test widget value retrieval (fallback to environment)
        with patch.dict(os.environ, {'DATABRICKS_WIDGET_TABLE_NAME': 'test_table'}):
            value = engine.get_widget_value("table_name", "default")
            assert value == "test_table"
        
        # Test default value
        value = engine.get_widget_value("nonexistent_widget", "default_value")
        assert value == "default_value"


def test_databricks_secrets_functionality():
    """Test Databricks secrets functionality."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        engine = validator.engine
        
        # Test secret retrieval (fallback to environment)
        with patch.dict(os.environ, {'DATABRICKS_SECRET_MYSCOPE_API_KEY': 'secret_value'}):
            value = engine.get_secret("myscope", "api-key")
            assert value == "secret_value"
        
        # Test missing secret
        value = engine.get_secret("nonexistent", "key")
        assert value is None


def test_databricks_data_loading():
    """Test Databricks-specific data loading."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        # Mock Spark session and DataFrame
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.table.return_value = mock_df
        mock_spark.read.format.return_value.load.return_value = mock_df
        
        validator = DataValidator(config)
        engine = validator.engine
        engine._spark = mock_spark
        
        # Test Unity Catalog table loading
        result = engine.load_data("main.schema.table")
        mock_spark.table.assert_called_with("main.schema.table")
        assert result == mock_df
        
        # Test Unity Catalog source configuration
        unity_catalog_source = {
            "type": "unity_catalog",
            "catalog": "main",
            "schema": "customer_data",
            "table": "customers"
        }
        result = engine.load_data(unity_catalog_source)
        mock_spark.table.assert_called_with("main.customer_data.customers")
        
        # Test Delta source configuration
        delta_source = {
            "type": "delta",
            "path": "/delta/table/path"
        }
        mock_spark.read.format.return_value = mock_spark.read
        mock_spark.read.load.return_value = mock_df
        result = engine.load_data(delta_source)
        mock_spark.read.format.assert_called_with("delta")
        mock_spark.read.load.assert_called_with("/delta/table/path")


def test_databricks_specific_validation_rules():
    """Test execution of Databricks-specific validation rules."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        validator = DataValidator(config)
        engine = validator.engine
        
        # Test Unity Catalog lineage rule
        lineage_rule = ValidationRule(
            name="lineage_check",
            rule_type="unity_catalog_lineage",
            parameters={
                "databricks": {
                    "catalog": "main",
                    "schema": "test"
                }
            },
            severity="info"
        )
        
        result = engine.execute_rule(mock_df, lineage_rule)
        assert isinstance(result, ValidationResult)
        assert result.rule_name == "lineage_check"
        assert result.rule_type == "unity_catalog_lineage"
        assert result.passed is True
        
        # Test Delta quality rule
        delta_rule = ValidationRule(
            name="delta_quality",
            rule_type="delta_quality",
            parameters={
                "databricks": {
                    "check_delta_log": True
                }
            },
            severity="info"
        )
        
        result = engine.execute_rule(mock_df, delta_rule)
        assert isinstance(result, ValidationResult)
        assert result.rule_name == "delta_quality"
        assert result.rule_type == "delta_quality"
        assert result.passed is True


def test_databricks_cluster_info():
    """Test cluster information retrieval."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        with patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '13.3.x-scala2.12'}):
            validator = DataValidator(config)
            engine = validator.engine
            
            # Mock Spark session
            mock_spark = Mock()
            mock_spark.version = "3.4.0"
            mock_spark.sparkContext.appName = "test-app"
            mock_conf = Mock()
            mock_conf.get.return_value = "2g"
            mock_spark.conf = mock_conf
            engine._spark = mock_spark
            
            cluster_info = engine.get_cluster_info()
            
            assert cluster_info["runtime_version"] == "13.3.x-scala2.12"
            assert cluster_info["is_databricks_runtime"] is True
            assert cluster_info["spark_version"] == "3.4.0"
            assert cluster_info["app_name"] == "test-app"


def test_databricks_job_config_creation():
    """Test Databricks job configuration creation."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        engine = validator.engine
        
        job_config = engine.create_databricks_job_config("/dbfs/config.yaml")
        
        assert job_config["name"] == "data-validator-job"
        assert "new_cluster" in job_config
        assert "spark_python_task" in job_config
        assert job_config["spark_python_task"]["parameters"] == [
            "--config", "/dbfs/config.yaml",
            "--engine", "databricks"
        ]
        assert "libraries" in job_config
        assert job_config["libraries"][0]["pypi"]["package"] == "data-validator[spark]"


def test_databricks_logging():
    """Test Databricks-specific logging."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        engine = validator.engine
        
        # Mock logging
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            engine.log_to_databricks("Test message", "INFO")
            mock_logger.info.assert_called_with("Test message")
            
            engine.log_to_databricks("Error message", "ERROR")
            mock_logger.error.assert_called_with("Error message")


def test_databricks_job_manager():
    """Test DatabricksJobManager functionality."""
    try:
        from data_validator.databricks_utils import DatabricksJobManager
        
        manager = DatabricksJobManager()
        
        # Test job configuration creation
        job_config = manager.create_validation_job(
            "test-job",
            "/dbfs/config.yaml"
        )
        
        assert job_config["name"] == "test-job"
        assert "new_cluster" in job_config
        assert "spark_python_task" in job_config
        
        # Test streaming job configuration
        stream_config = {
            "source_table": "events",
            "checkpoint_location": "/tmp/checkpoints",
            "trigger_interval": 60
        }
        
        streaming_job = manager.create_streaming_validation_job(
            "streaming-job",
            "/dbfs/config.yaml",
            stream_config
        )
        
        assert streaming_job["name"] == "streaming-job"
        assert streaming_job["timeout_seconds"] == 0  # Streaming jobs run indefinitely
        
        # Test DLT pipeline configuration
        dlt_config = manager.create_dlt_pipeline_config(
            "dlt-pipeline",
            {"target_database": "test_db"}
        )
        
        assert dlt_config["name"] == "dlt-pipeline"
        assert dlt_config["target"] == "test_db"
        
    except ImportError:
        pytest.skip("DatabricksJobManager not available")


def test_databricks_config_validation():
    """Test validation configuration with Databricks engine."""
    config_dict = {
        "version": "1.0",
        "engine": {
            "type": "databricks",
            "connection_params": {
                "spark.sql.adaptive.enabled": "true"
            }
        },
        "tables": [{
            "name": "test_table",
            "rules": [{
                "name": "test_rule",
                "rule_type": "completeness",
                "column": "id",
                "severity": "error"
            }]
        }]
    }
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_dict, f)
        temp_path = f.name

    try:
        with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
            validator = DataValidator(temp_path)
            assert validator.config.engine.type == "databricks"
    finally:
        Path(temp_path).unlink()
def test_databricks_error_handling():
    """Test error handling in Databricks engine."""
    config = create_databricks_config()
    
    with patch('data_validator.engines.databricks_engine.PYSPARK_AVAILABLE', True):
        validator = DataValidator(config)
        engine = validator.engine
        
        # Test error in Databricks-specific rule execution
        error_rule = ValidationRule(
            name="error_rule",
            rule_type="unity_catalog_lineage",
            parameters={
                "databricks": {"invalid": "config"}
            },
            severity="error"
        )
        
        mock_df = Mock()
        
        # Mock an exception during rule execution
        with patch.object(engine, '_validate_lineage', side_effect=Exception("Test error")):
            result = engine.execute_rule(mock_df, error_rule)
            
            assert isinstance(result, ValidationResult)
            assert result.passed is False
            assert "Test error" in result.message
            assert result.metadata["engine"] == "databricks"


def test_databricks_yaml_config_loading():
    """Test loading Databricks configuration from YAML."""
    databricks_yaml = """
version: "1.0"
engine:
  type: "databricks"
  connection_params:
    spark.sql.adaptive.enabled: "true"
  options:
    unity_catalog.enabled: "true"
tables:
  - name: "test_table"
    rules:
      - name: "completeness_check"
        rule_type: "completeness"
        column: "id"
        severity: "error"
      - name: "unity_catalog_check"
        rule_type: "unity_catalog_lineage"
        parameters:
          databricks:
            catalog: "main"
            schema: "test"
        severity: "info"
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(databricks_yaml)
        f.flush()
        
        try:
            config = ValidationConfig.from_yaml(f.name)
            assert config.engine.type == "databricks"
            assert len(config.tables) == 1
            assert len(config.tables[0].rules) == 2
            
            # Find the Unity Catalog rule
            unity_rule = next(
                rule for rule in config.tables[0].rules 
                if rule.rule_type == "unity_catalog_lineage"
            )
            assert unity_rule.parameters["databricks"]["catalog"] == "main"
            
        finally:
            os.unlink(f.name)


if __name__ == "__main__":
    # Run specific tests if needed
    import subprocess
    subprocess.run(["python", "-m", "pytest", __file__, "-v"])