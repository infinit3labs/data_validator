import os
from pathlib import Path
import yaml

from data_validator.settings import load_config


def test_env_var_override(tmp_path: Path) -> None:
    config = {"version": "1.0", "engine": {"type": "pyspark"}, "tables": []}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(config))

    os.environ["VALIDATOR_ENGINE__TYPE"] = "duckdb"
    loaded = load_config(str(path))

    assert loaded.engine.type == "duckdb"


class DummyWidget:
    def __init__(self, values):
        self._values = values

    def get(self, name):
        return self._values.get(name, "")


class DummyDbutils:
    def __init__(self, values):
        self.widgets = DummyWidget(values)


def test_widget_override(monkeypatch, tmp_path: Path) -> None:
    config = {"version": "1.0", "engine": {"type": "pyspark"}, "tables": []}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(config))

    dbutils = DummyDbutils({"engine": "polars"})
    monkeypatch.setattr("data_validator.settings._get_dbutils", lambda: dbutils)

    loaded = load_config(str(path), use_widgets=True)
    assert loaded.engine.type == "polars"
