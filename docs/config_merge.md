# Configuration Merging

This project supports loading configuration from YAML files with optional overrides from environment variables and Databricks widgets.

## Usage

1. Create a YAML configuration file as normal.
2. Optionally set environment variables prefixed with `VALIDATOR_` to override settings. Nested values use double underscores:
   ```bash
   export VALIDATOR_ENGINE__TYPE=duckdb
   export VALIDATOR_DQX__ENABLED=false
   ```
3. When running on Databricks notebooks, widgets named `config` or `engine` can be used to supply a configuration path or engine override.

The helper function `data_validator.settings.load_config()` returns a merged `ValidationConfig` instance combining these sources. `DataValidator` automatically uses this loader when given a YAML path.

## Example

```python
from data_validator.settings import load_config

config = load_config("config.yaml")  # env vars and widgets are merged automatically
validator = DataValidator(config)
```
