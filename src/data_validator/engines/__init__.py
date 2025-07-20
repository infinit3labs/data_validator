"""
Abstract engine interface and implementations for different compute engines.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from ..config import ValidationRule, EngineConfig


@dataclass
class ValidationResult:
    """Result of a validation rule execution."""
    
    rule_name: str
    rule_type: str
    passed: bool
    failed_count: int
    total_count: int
    success_rate: float
    message: str
    severity: str
    execution_time_ms: float
    metadata: Dict[str, Any]


@dataclass
class ValidationSummary:
    """Summary of all validation results."""
    
    table_name: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    error_rules: int
    overall_success_rate: float
    total_execution_time_ms: float
    results: List[ValidationResult]


class ValidationEngine(ABC):
    """Abstract base class for validation engines."""
    
    def __init__(self, config: EngineConfig):
        self.config = config
        self._connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the compute engine."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the compute engine."""
        pass
    
    @abstractmethod
    def load_data(self, source: Union[str, Any]) -> Any:
        """Load data from source (table name, file path, or DataFrame)."""
        pass
    
    @abstractmethod
    def execute_rule(self, data: Any, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule against the data."""
        pass
    
    @abstractmethod
    def apply_filter(self, data: Any, rule: ValidationRule) -> Any:
        """Apply validation rule as a filter and return filtered data."""
        pass
    
    def execute_rules(self, data: Any, rules: List[ValidationRule], table_name: str = "unknown") -> ValidationSummary:
        """Execute multiple validation rules against the data."""
        import time
        
        start_time = time.time()
        results = []
        
        for rule in rules:
            if rule.enabled:
                result = self.execute_rule(data, rule)
                results.append(result)
        
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Calculate summary statistics
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        warning_rules = sum(1 for r in results if r.severity == "warning" and not r.passed)
        error_rules = sum(1 for r in results if r.severity == "error" and not r.passed)
        overall_success_rate = passed_rules / total_rules if total_rules > 0 else 1.0
        
        return ValidationSummary(
            table_name=table_name,
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            warning_rules=warning_rules,
            error_rules=error_rules,
            overall_success_rate=overall_success_rate,
            total_execution_time_ms=execution_time,
            results=results
        )
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def create_engine(config: EngineConfig) -> ValidationEngine:
    """Factory function to create appropriate validation engine."""
    if config.type == "pyspark":
        from .pyspark_engine import PySparkValidationEngine
        return PySparkValidationEngine(config)
    elif config.type == "duckdb":
        from .duckdb_engine import DuckDBValidationEngine
        return DuckDBValidationEngine(config)
    elif config.type == "polars":
        from .polars_engine import PolarsValidationEngine
        return PolarsValidationEngine(config)
    else:
        raise ValueError(f"Unsupported engine type: {config.type}")