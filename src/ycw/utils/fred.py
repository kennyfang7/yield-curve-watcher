"""Shared FRED API fetch utility used across fetchers, indicators, and signals."""
import math
import requests
import pandas as pd


def fetch_fred_series(
    sid: str,
    start: str,
    end: str,
    api_key: str,
    timeout: int = 30,
) -> pd.Series:
    """Fetch a single FRED series and return it as a float Series.

    Missing-value markers ("." or None from FRED) become NaN.

    Parameters
    ----------
    sid:      FRED series ID (e.g. "DGS10")
    start:    ISO date string "YYYY-MM-DD"
    end:      ISO date string "YYYY-MM-DD"
    api_key:  FRED API key
    timeout:  HTTP timeout in seconds
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": sid,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
    }
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    observations = r.json()["observations"]
    s = pd.Series(
        {
            pd.to_datetime(x["date"]): (
                float(x["value"])
                if x.get("value") not in (".", None, "")
                else math.nan
            )
            for x in observations
        },
        name=sid,
        dtype=float,
    )
    return s
