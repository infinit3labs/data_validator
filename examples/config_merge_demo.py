import os
from data_validator import DataValidator

# Path to example config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "sample_config.yaml")

# Override engine type using environment variable
os.environ["DV_ENGINE__TYPE"] = "duckdb"
os.environ["DV_ENGINE__CONNECTION_PARAMS__DATABASE"] = ":memory:"

validator = DataValidator(CONFIG_PATH)
print("Engine type from merged config:", validator.config.engine.type)
print("Database:", validator.config.engine.connection_params.get("database"))
