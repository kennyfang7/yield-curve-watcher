import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

from .utils.fred import fetch_fred_series

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = "~/.ycw/cache"


class FredCache:
    """Disk-backed cache for FRED series fetched via :func:`fetch_fred_series`.

    Each ``(series_id, start, end)`` triple is stored as a single parquet file
    under *cache_dir*.  The staleness rule is deliberately simple:

    * **end < today** — historical data is immutable; serve from disk if the
      file exists, fetch and write it otherwise.
    * **end == today** — data may not yet be final; always re-fetch from FRED
      (the on-disk file is overwritten if the write succeeds).

    Read and write errors are caught, logged as warnings, and silently fall
    back to a live FRED request so that a corrupted file, a full disk, or a
    permissions problem never crashes the pipeline.

    Parameters
    ----------
    api_key:
        FRED API key used for cache-miss fetches.
    cache_dir:
        Directory for parquet files.  Defaults to ``FRED_CACHE_DIR`` env var,
        falling back to ``~/.ycw/cache``.
    """

    def __init__(self, api_key: str, cache_dir: Path | None = None) -> None:
        self._api_key = api_key
        raw_dir = (
            Path(cache_dir)
            if cache_dir is not None
            else Path(os.getenv("FRED_CACHE_DIR", _DEFAULT_CACHE_DIR)).expanduser()
        )
        try:
            raw_dir.mkdir(parents=True, exist_ok=True)
            self._dir: Path | None = raw_dir
        except OSError as exc:
            logger.warning(
                "FredCache: cannot create cache directory %s (%s); "
                "all requests will go to FRED directly",
                raw_dir,
                exc,
            )
            self._dir = None

    def _path(self, sid: str, start: str, end: str) -> Path | None:
        if self._dir is None:
            return None
        return self._dir / f"{sid}__{start}__{end}.parquet"

    @staticmethod
    def _end_is_today(end: str) -> bool:
        return pd.to_datetime(end).date() >= date.today()

    def get(self, sid: str, start: str, end: str) -> pd.Series:
        """Return a FRED series, reading from disk cache when possible.

        Parameters
        ----------
        sid:    FRED series ID (e.g. ``"DGS10"``).
        start:  ISO date string ``"YYYY-MM-DD"``.
        end:    ISO date string ``"YYYY-MM-DD"``.
        """
        path = self._path(sid, start, end)
        end_is_today = self._end_is_today(end)

        # Serve from cache for historical (immutable) requests.
        if not end_is_today and path is not None and path.exists():
            try:
                return pd.read_parquet(path).squeeze("columns")
            except Exception as exc:
                logger.warning(
                    "FredCache: failed to read %s (%s); fetching live",
                    path,
                    exc,
                )

        # Live fetch — always for today's end date, or on cache miss/read error.
        series = fetch_fred_series(sid, start, end, self._api_key)

        # Persist historical data only; today's data may change intraday.
        if not end_is_today and path is not None:
            try:
                series.to_frame().to_parquet(path)
            except Exception as exc:
                logger.warning(
                    "FredCache: failed to write %s (%s); result still returned",
                    path,
                    exc,
                )

        return series
