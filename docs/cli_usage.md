# Command Line Usage

The package provides a `data-validator` CLI for running validations without writing any code.

```bash
# Validate all tables defined in config and sources
data-validator --config config.yaml --sources sources.yaml --output report.json

# Validate a single table when data can be loaded by the engine
data-validator --config config.yaml --table customers
```

The sources file should map table names to file paths or other data sources. It can be YAML or JSON.

The generated report is written as JSON and is identical to the output produced by `DataValidator.get_validation_report`.
