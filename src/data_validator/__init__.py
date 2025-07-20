"""
Data Validator - A flexible data validation module using YAML configuration.

This package provides a unified interface for applying data quality rules
against Spark DataFrames with support for multiple compute engines including
PySpark, DuckDB, and Polars. It integrates with Databricks labs DQX package
and supports Delta Live Tables.
"""

__version__ = "0.1.0"
__author__ = "Data Validator Team"

from .validator import DataValidator
from .config import ValidationConfig
from .engines import ValidationEngine

# Import Databricks utilities conditionally
try:
    from .databricks_utils import DatabricksJobManager
    __all__ = ["DataValidator", "ValidationConfig", "ValidationEngine", "DatabricksJobManager"]
except ImportError:
    # DatabricksJobManager not available (missing dependencies)
    __all__ = ["DataValidator", "ValidationConfig", "ValidationEngine"]