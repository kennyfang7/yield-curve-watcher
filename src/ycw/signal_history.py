import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from .types import Signal

logger = logging.getLogger(__name__)

_DEFAULT_HISTORY_DIR = "~/.ycw/cache"
_HISTORY_FILENAME = "signal_history.json"


class SignalHistory:
    """Persist signal firing history to suppress repeat notifications.

    A signal is considered new when:
    - It has never been seen before, OR
    - It was previously inactive (cleared from a prior run), OR
    - It has been continuously active for >= ttl_days days.

    Parameters
    ----------
    ttl_days:
        Days before a continuously active signal re-notifies. Default 7.
    path:
        Path to the JSON history file. Defaults to YCW_HISTORY_FILE env var,
        falling back to ~/.ycw/cache/signal_history.json.
    """

    def __init__(self, ttl_days: int = 7, path: Path | None = None) -> None:
        self._ttl_days = ttl_days
        if path is not None:
            self._path = Path(path)
        else:
            env_path = os.getenv("YCW_HISTORY_FILE")
            if env_path:
                self._path = Path(env_path)
            else:
                self._path = (
                    Path(_DEFAULT_HISTORY_DIR).expanduser() / _HISTORY_FILENAME
                )
        self._history = self._load()

    def _load(self) -> dict:
        try:
            with open(self._path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.warning(
                "SignalHistory: failed to read %s (%s); starting empty",
                self._path,
                exc,
            )
            return {}

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(self._history, f, indent=2)
        except Exception as exc:
            logger.warning("SignalHistory: failed to write %s (%s)", self._path, exc)

    @staticmethod
    def _key(sig: Signal) -> str:
        return f"{sig.code}|{sig.level}|{sig.economy}"

    def filter_new(self, signals: List[Signal]) -> List[Signal]:
        """Return only signals that should trigger a notification this run."""
        now = datetime.now(timezone.utc)
        new = []
        for sig in signals:
            k = self._key(sig)
            entry = self._history.get(k)
            if entry is None or not entry["active"]:
                new.append(sig)
            else:
                first_seen = datetime.fromisoformat(entry["first_seen"])
                age_days = (now - first_seen).total_seconds() / 86400
                if age_days >= self._ttl_days:
                    new.append(sig)
        return new

    def update(self, current_signals: List[Signal]) -> None:
        """Update history with the current run's signals and save to disk."""
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.isoformat()
        current_keys = {self._key(s) for s in current_signals}

        for sig in current_signals:
            k = self._key(sig)
            entry = self._history.get(k)
            if entry is None or not entry["active"]:
                # New or re-fired: start fresh
                self._history[k] = {
                    "first_seen": now_str,
                    "last_seen": now_str,
                    "active": True,
                }
            else:
                first_seen = datetime.fromisoformat(entry["first_seen"])
                age_days = (now_dt - first_seen).total_seconds() / 86400
                if age_days >= self._ttl_days:
                    # TTL expired — reset so next suppression window starts now
                    self._history[k] = {
                        "first_seen": now_str,
                        "last_seen": now_str,
                        "active": True,
                    }
                else:
                    self._history[k]["last_seen"] = now_str

        # Mark signals that didn't fire this run as cleared
        for k in list(self._history):
            if self._history[k]["active"] and k not in current_keys:
                self._history[k]["active"] = False

        self._save()
