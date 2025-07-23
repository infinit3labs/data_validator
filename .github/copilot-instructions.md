# Copilot Instructions for Data Validator

## Project Architecture

This is a **multi-engine data validation framework** that applies YAML-configured quality rules across different compute engines (PySpark, Databricks, DuckDB, Polars). The architecture follows a **plugin pattern** with abstract base classes defining engine interfaces.

### Core Components

- **`src/data_validator/validator.py`** - Main orchestrator class (`DataValidator`) that coordinates validation workflows
- **`src/data_validator/config.py`** - Pydantic-based configuration system with environment variable merging via `DV_` prefix
- **`src/data_validator/engines/`** - Abstract `ValidationEngine` base class with concrete implementations per compute engine
- **`src/data_validator/settings.py`** - Multi-source configuration loader (YAML + env vars + Databricks widgets)

### Key Design Patterns

**Configuration Merging**: Configuration follows a precedence hierarchy: `YAML base → Environment variables (DV_*) → Databricks widgets`. Use double underscores for nested keys: `DV_ENGINE__TYPE=duckdb`.

**Engine Abstraction**: All engines implement the same interface (`connect()`, `load_data()`, `execute_rule()`, `apply_filter()`). The `create_engine()` factory function instantiates the correct engine based on config.

**Rule System**: Rules are defined as Pydantic models in `ValidationRule` class. Each rule has `rule_type` (completeness, uniqueness, range, pattern, custom), `severity` (error/warning/info), and optional `threshold` (0.0-1.0).

## Development Workflows

### Testing Strategy
- **Use DuckDB for tests** - All test files use `engine: {type: "duckdb"}` for fast, dependency-free testing
- **Test configuration objects** - Create `ValidationConfig` objects directly rather than YAML files where possible
- **Mock external dependencies** - Use conditional imports with fallbacks for Spark/Databricks dependencies

### Adding New Engines
1. Inherit from `ValidationEngine` in `src/data_validator/engines/`
2. Implement all abstract methods (`connect`, `load_data`, `execute_rule`, `apply_filter`)
3. Add engine type validation in `EngineConfig.validate_engine_type()`
4. Update `create_engine()` factory function in `engines/__init__.py`

### Configuration Patterns
- **Environment overrides**: Use `DV_` prefix with double underscores: `DV_ENGINE__CONNECTION_PARAMS__DATABASE=":memory:"`
- **Databricks widgets**: Widget values automatically override config when `use_widgets=True`
- **Optional dependencies**: Import engines conditionally with `try/except ImportError` pattern

## Databricks Integration

### Special Considerations
- **Runtime detection**: `DatabricksValidationEngine` auto-detects Databricks runtime via environment variables
- **dbutils integration**: Provides `get_widget_value()` and `get_secret()` methods for Databricks-specific features
- **Unity Catalog**: Tables can be referenced as `"main.schema.table"` format in data sources
- **Job deployment**: Use `databricks_job.py` module with `--config`, `--sources`, `--output` arguments

### DQX Integration
When `dqx.enabled: true`, validation results integrate with Databricks Labs DQX for advanced monitoring. Configure `metrics_table` and `quarantine_table` in YAML.

## Code Generation Guidelines

### Configuration Files
- Always include `version: "1.0"` at the top
- Use meaningful `name` and `description` fields for rules
- Set appropriate `severity` levels: `error` for critical issues, `warning` for quality alerts
- Include `threshold` values for statistical rules (typically 0.90-0.99)

### Error Handling
- Engines use context managers (`with engine:`) for proper connection lifecycle
- All engine operations should handle missing dependencies gracefully
- Use `ValidationResult.metadata` dict for engine-specific diagnostic information

### File Patterns
- Config files go in `examples/` with descriptive names (`databricks_config.yaml`, `duckdb_config.yaml`)
- Engine implementations follow naming: `{engine_name}_engine.py`
- Test files mirror source structure: `test_{module}.py`

## Key Integration Points

### Multi-Engine Data Loading
The `load_data()` method accepts multiple input types:
- DataFrame objects (pandas, Spark, Polars)
- Table names (engine-specific resolution)
- File paths (CSV, Parquet, Delta)
- Unity Catalog references (`"catalog.schema.table"`)

### Rule Execution Flow
1. `DataValidator.validate_table()` → `ValidationEngine.execute_rules()`
2. Each rule → `execute_rule()` → `ValidationResult`
3. Results aggregated into `ValidationSummary` with overall success rate
4. Optional DQX integration for metrics persistence

Use this understanding when extending functionality or debugging issues. Prioritize the multi-engine abstraction and configuration-driven approach in all modifications.
