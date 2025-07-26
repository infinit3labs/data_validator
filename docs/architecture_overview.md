# Architecture Overview

This document provides a high-level summary of the main classes in the
`data_validator` package and how they interact with each other.

## Configuration Classes (`config.py`)

- **`ValidationRule`** – represents a single rule (e.g. completeness, uniqueness).
- **`TableConfig`** – groups rules for a specific table.
- **`EngineConfig`** – settings for the validation engine (type and options).
- **`DQXConfig`** – optional Databricks DQX integration settings.
- **`PipelineConfig`** – controls state persistence for pipeline runs.
- **`ValidationConfig`** – top level configuration combining all sections. It
  loads from YAML via `from_yaml()` and provides helpers such as
  `get_enabled_rules()`.

## Core Engine Classes (`engines` package)

- **`ValidationEngine`** – abstract base class that defines the interface for
  connecting to a compute engine, loading data and executing rules. It produces a
  `ValidationSummary` containing multiple `ValidationResult` objects.
- **`PySparkValidationEngine`**, **`DatabricksValidationEngine`**,
  **`DuckDBValidationEngine`**, **`PolarsValidationEngine`** – concrete
  implementations of `ValidationEngine` for the supported compute engines.
  The Databricks engine extends the PySpark engine with workspace specific
  helpers.
- **Factory** – `create_engine()` instantiates the correct engine based on the
  `EngineConfig`.

## State Management (`state.py`)

- **`PipelineState`** – small utility dataclass that persists table processing
  progress in a JSON file so validations can resume when rerun.

## Validator Orchestration (`validator.py`)

- **`DataValidator`** – main entry point. It loads a `ValidationConfig`, creates
  a `ValidationEngine`, and exposes methods to validate one table or multiple
  tables. It can also apply rules as filters and integrates with optional DQX and
  Delta Live Tables features.

Interaction summary:

1. `DataValidator` loads a `ValidationConfig` via `settings.load_config()`.
2. `ValidationConfig.engine` determines which `ValidationEngine` implementation is
   created via `create_engine()`.
3. The chosen `ValidationEngine` executes the `ValidationRule` objects from the
   configuration and returns `ValidationSummary` results.
4. `DataValidator` aggregates these summaries and can produce a structured
   report with `get_validation_report()`.
5. Optional pipeline state is stored through `PipelineState` if configured.

## Databricks Utilities

- **`DatabricksJobManager`** (`databricks_utils.py`) – helper class to generate
  Databricks job and pipeline configurations and to output deployment scripts.
- **`run_job`** (`databricks_job.py`) – simple CLI entrypoint used by
  `DatabricksJobManager` scripts to execute validations on Databricks.

## Command Line Interface (`cli.py`)

Provides a small CLI (`data-validator`) for running validations locally. It
parses command line arguments, instantiates `DataValidator` and prints a JSON
report.

