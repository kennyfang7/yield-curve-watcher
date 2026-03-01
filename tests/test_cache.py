"""Tests for FredCache — disk-backed FRED series cache.

All tests use a pytest tmp_path fixture for isolation and mock requests.get
so no live HTTP calls are made.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ycw.cache import FredCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_OBSERVATIONS = [
    {"date": "2020-01-31", "value": "4.5"},
    {"date": "2020-02-29", "value": "4.6"},
    {"date": "2020-03-31", "value": "4.7"},
]

_HISTORICAL_START = "2020-01-01"
_HISTORICAL_END = "2020-03-31"  # always in the past


def _mock_response(observations=_FAKE_OBSERVATIONS):
    resp = MagicMock()
    resp.json.return_value = {"observations": observations}
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Core caching behaviour
# ---------------------------------------------------------------------------

def test_historical_request_cached(tmp_path):
    """Second call for a historical range must not trigger a live fetch."""
    with patch("requests.get", return_value=_mock_response()) as mock_get:
        cache = FredCache("fake-key", cache_dir=tmp_path)

        result1 = cache.get("DGS10", _HISTORICAL_START, _HISTORICAL_END)
        result2 = cache.get("DGS10", _HISTORICAL_START, _HISTORICAL_END)

        # HTTP request should have fired exactly once; second call served from disk.
        mock_get.assert_called_once()

    # Parquet file was written after the first (live) fetch.
    parquet = tmp_path / f"DGS10__{_HISTORICAL_START}__{_HISTORICAL_END}.parquet"
    assert parquet.exists()

    # Values from both calls match the fake observations.
    assert list(result1.values) == pytest.approx([4.5, 4.6, 4.7])
    assert list(result2.values) == pytest.approx([4.5, 4.6, 4.7])


def test_parquet_file_naming(tmp_path):
    """Cache file is named {sid}__{start}__{end}.parquet."""
    with patch("requests.get", return_value=_mock_response()):
        cache = FredCache("fake-key", cache_dir=tmp_path)
        cache.get("BAA", _HISTORICAL_START, _HISTORICAL_END)

    assert (tmp_path / f"BAA__{_HISTORICAL_START}__{_HISTORICAL_END}.parquet").exists()


def test_corrupted_file_falls_back_to_live_fetch(tmp_path):
    """A corrupted parquet file must cause a silent re-fetch, not raise."""
    parquet = tmp_path / f"DGS10__{_HISTORICAL_START}__{_HISTORICAL_END}.parquet"
    parquet.write_bytes(b"not valid parquet content")

    with patch("requests.get", return_value=_mock_response()) as mock_get:
        cache = FredCache("fake-key", cache_dir=tmp_path)
        result = cache.get("DGS10", _HISTORICAL_START, _HISTORICAL_END)

    # Fell back to live fetch despite the corrupted file.
    mock_get.assert_called_once()
    assert list(result.values) == pytest.approx([4.5, 4.6, 4.7])


def test_write_failure_still_returns_series(tmp_path):
    """A failed parquet write must not crash; the series is still returned."""
    with patch("requests.get", return_value=_mock_response()):
        cache = FredCache("fake-key", cache_dir=tmp_path)
        with patch("pandas.DataFrame.to_parquet", side_effect=OSError("disk full")):
            result = cache.get("DGS10", _HISTORICAL_START, _HISTORICAL_END)

    assert list(result.values) == pytest.approx([4.5, 4.6, 4.7])


def test_today_end_always_refetches(tmp_path):
    """Requests with end == today must bypass the cache on every call."""
    import pandas as _pd
    today = _pd.Timestamp.today().strftime("%Y-%m-%d")

    with patch("requests.get", return_value=_mock_response()) as mock_get:
        cache = FredCache("fake-key", cache_dir=tmp_path)
        cache.get("DGS10", "1962-01-01", today)
        cache.get("DGS10", "1962-01-01", today)

    assert mock_get.call_count == 2


def test_fred_cache_dir_env_var(tmp_path, monkeypatch):
    """FRED_CACHE_DIR env var overrides the default cache directory."""
    monkeypatch.setenv("FRED_CACHE_DIR", str(tmp_path))

    with patch("requests.get", return_value=_mock_response()):
        cache = FredCache("fake-key")  # no explicit cache_dir
        cache.get("DGS10", _HISTORICAL_START, _HISTORICAL_END)

    assert (tmp_path / f"DGS10__{_HISTORICAL_START}__{_HISTORICAL_END}.parquet").exists()
