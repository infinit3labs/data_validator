"""
Configuration module for data validation rules.

This module defines the structure and validation of YAML configuration files
used to define data quality rules.
"""

from typing import Dict, List, Optional, Any, Union
import os
from pydantic import BaseModel, Field
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
from pathlib import Path


class ValidationRule(BaseModel):
    """Individual validation rule configuration."""
    
    name: str = Field(description="Name of the validation rule")
    description: Optional[str] = Field(None, description="Description of what this rule validates")
    rule_type: str = Field(description="Type of validation rule (e.g., 'completeness', 'uniqueness', 'range')")
    column: Optional[str] = Field(None, description="Column name to validate (if applicable)")
    expression: Optional[str] = Field(None, description="SQL expression or validation logic")
    threshold: Optional[float] = Field(None, description="Threshold for validation (0.0 to 1.0)")
    severity: str = Field(default="error", description="Severity level: error, warning, info")
    enabled: bool = Field(default=True, description="Whether this rule is enabled")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Additional parameters for the rule")
    
    @field_validator('severity')
    def validate_severity(cls, v):
        allowed_severities = {'error', 'warning', 'info'}
        if v not in allowed_severities:
            raise ValueError(f"Severity must be one of {allowed_severities}")
        return v
    
    @field_validator('threshold')
    def validate_threshold(cls, v):
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError("Threshold must be between 0.0 and 1.0")
        return v


class TableConfig(BaseModel):
    """Configuration for a specific table."""
    
    name: str = Field(description="Table name")
    description: Optional[str] = Field(None, description="Table description")
    rules: List[ValidationRule] = Field(description="List of validation rules for this table")
    
    @field_validator('rules')
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("At least one validation rule must be defined")
        return v


class EngineConfig(BaseModel):
    """Engine-specific configuration."""
    
    type: str = Field(description="Engine type: pyspark, duckdb, polars")
    connection_params: Dict[str, Any] = Field(default_factory=dict, description="Engine connection parameters")
    options: Dict[str, Any] = Field(default_factory=dict, description="Engine-specific options")
    
    @field_validator('type')
    def validate_engine_type(cls, v):
        allowed_engines = {'pyspark', 'databricks', 'duckdb', 'polars'}
        if v not in allowed_engines:
            raise ValueError(f"Engine type must be one of {allowed_engines}")
        return v


class DQXConfig(BaseModel):
    """Databricks DQX integration configuration."""
    
    enabled: bool = Field(default=True, description="Whether to use DQX integration")
    output_path: Optional[str] = Field(None, description="Path to store DQX results")
    metrics_table: Optional[str] = Field(None, description="Table to store DQX metrics")
    quarantine_table: Optional[str] = Field(None, description="Table to store quarantined records")


class ValidationConfig(BaseModel):
    """Complete validation configuration."""
    
    version: str = Field(default="1.0", description="Configuration version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Configuration metadata")
    engine: EngineConfig = Field(description="Engine configuration")
    dqx: DQXConfig = Field(default_factory=DQXConfig, description="DQX configuration")
    tables: List[TableConfig] = Field(description="List of table configurations")
    global_rules: List[ValidationRule] = Field(default_factory=list, description="Global validation rules")
    
    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> 'ValidationConfig':
        """Load configuration from YAML file."""
        yaml_path = Path(yaml_path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")
        
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        return cls(**data)
    
    
    def to_yaml(self, yaml_path: Union[str, Path]) -> None:
        """Save configuration to YAML file."""
        yaml_path = Path(yaml_path)
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.dict(), f, default_flow_style=False, sort_keys=False)
    
    def get_table_config(self, table_name: str) -> Optional[TableConfig]:
        """Get configuration for a specific table."""
        for table in self.tables:
            if table.name == table_name:
                return table
        return None

    def get_enabled_rules(self, table_name: Optional[str] = None) -> List[ValidationRule]:
        """Get all enabled rules, optionally filtered by table name."""
        rules = []
        
        # Add global rules
        rules.extend([rule for rule in self.global_rules if rule.enabled])
        
        # Add table-specific rules
        if table_name:
            table_config = self.get_table_config(table_name)
            if table_config:
                rules.extend([rule for rule in table_config.rules if rule.enabled])
        else:
            # Add all table rules if no specific table requested
            for table in self.tables:
                rules.extend([rule for rule in table.rules if rule.enabled])
        
        return rules


def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries."""
    result = dict(base)
    for key, value in overrides.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _env_to_dict(prefix: str) -> Dict[str, Any]:
    """Convert environment variables with prefix to a nested dictionary."""
    data: Dict[str, Any] = {}
    for env, value in os.environ.items():
        if env.startswith(prefix):
            path = env[len(prefix):].lower().split("__")
            current = data
            for part in path[:-1]:
                current = current.setdefault(part, {})
            current[path[-1]] = yaml.safe_load(value)
    return data


class ValidationSettings(BaseSettings):
    """Pydantic settings allowing environment variable overrides."""

    version: Optional[str] = None
    metadata: Dict[str, Any] = {}
    engine: Optional[EngineConfig] = None
    dqx: Optional[DQXConfig] = None
    tables: Optional[List[TableConfig]] = None
    global_rules: Optional[List[ValidationRule]] = None

    model_config = SettingsConfigDict(env_prefix="DV_", env_nested_delimiter="__", extra="ignore")


def load_config(yaml_path: Optional[Union[str, Path]] = None) -> ValidationConfig:
    """Load configuration from YAML file and environment variables."""
    # Determine config path from parameter, widget env, or environment variable
    if yaml_path is None:
        yaml_path = os.environ.get("DV_CONFIG_PATH") or os.environ.get("DATABRICKS_WIDGET_CONFIG")

    data: Dict[str, Any] = {}
    if yaml_path:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

    # Convert Databricks widget environment variables to DV_ style
    widget_overrides = _env_to_dict("DATABRICKS_WIDGET_")
    data = _deep_merge(data, widget_overrides)

    env_settings = ValidationSettings()
    env_overrides = env_settings.model_dump(exclude_none=True)
    data = _deep_merge(data, env_overrides)

    return ValidationConfig(**data)