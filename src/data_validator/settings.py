from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic_settings import BaseSettings
from pydantic import Field

from .config import ValidationConfig


def _get_dbutils():
    """Return the dbutils instance if running on Databricks."""
    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except Exception:
        pass
    return None


class EnvSettings(BaseSettings):
    """Load configuration values from environment variables."""

    config_file: Optional[str] = None
    engine__type: Optional[str] = None
    engine__connection_params: Optional[dict[str, Any]] = None
    engine__options: Optional[dict[str, Any]] = None
    dqx__enabled: Optional[bool] = None

    class Config:
        env_prefix = "VALIDATOR_"
        extra = "allow"
        case_sensitive = False


EnvSettings.model_rebuild()


def merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge two dictionaries."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key] = merge_dicts(base[key], value)
        else:
            base[key] = value
    return base


def _expand_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Expand double-underscore keys into nested dictionaries."""
    result: dict[str, Any] = {}
    for key, value in data.items():
        parts = key.split("__")
        current = result
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value
    return result


def load_config(
    yaml_path: str | None = None,
    *,
    env_prefix: str = "VALIDATOR",
    use_widgets: bool = False,
) -> ValidationConfig:
    """Load configuration from YAML with optional environment and widget overrides."""

    settings = EnvSettings(_env_prefix=f"{env_prefix}_")
    if yaml_path is None:
        yaml_path = settings.config_file

    widget_overrides: dict[str, Any] = {}
    if use_widgets:
        dbutils = _get_dbutils()
        if dbutils is not None:
            try:
                widget_config = dbutils.widgets.get("config")
                if widget_config:
                    yaml_path = widget_config
            except Exception:
                pass
            try:
                widget_engine = dbutils.widgets.get("engine")
                if widget_engine:
                    widget_overrides.setdefault("engine", {})["type"] = widget_engine
            except Exception:
                pass

    base_data: dict[str, Any] = {}
    if yaml_path:
        with open(Path(yaml_path), encoding="utf-8") as f:
            base_data = yaml.safe_load(f) or {}

    env_overrides = _expand_keys(
        settings.model_dump(exclude={"config_file"}, exclude_none=True)
    )
    merged = merge_dicts(base_data, env_overrides)
    merged = merge_dicts(merged, widget_overrides)
    return ValidationConfig.model_validate(merged)
