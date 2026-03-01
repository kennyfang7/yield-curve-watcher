import yaml
from typing import Any, Dict


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    _validate(cfg, path)
    return cfg


def _validate(cfg: Dict[str, Any], path: str) -> None:
    """Raise ValueError with a clear message for obviously wrong config values."""
    if "lookback_days" in cfg:
        try:
            val = int(cfg["lookback_days"])
        except (TypeError, ValueError):
            raise ValueError(
                f"Config '{path}': 'lookback_days' must be an integer, "
                f"got {cfg['lookback_days']!r}"
            )
        if val <= 0:
            raise ValueError(
                f"Config '{path}': 'lookback_days' must be a positive integer, got {val}"
            )

    for list_key in ("economies", "indicators", "signals"):
        if list_key in cfg and not isinstance(cfg[list_key], list):
            raise ValueError(
                f"Config '{path}': '{list_key}' must be a list, "
                f"got {type(cfg[list_key]).__name__}"
            )

    for str_key in ("fetcher", "notifier"):
        if str_key in cfg and not isinstance(cfg[str_key], str):
            raise ValueError(
                f"Config '{path}': '{str_key}' must be a string, "
                f"got {type(cfg[str_key]).__name__}"
            )
