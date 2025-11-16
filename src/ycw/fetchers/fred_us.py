import os, math
import pandas as pd
import requests
from datetime import date
from ..types import CurveFetcherResult
from .base import BaseFetcher

FRED_SERIES = {
    "1M":  "DGS1MO",
    "3M":  "DGS3MO",
    "6M":  "DGS6MO",
    "1Y":  "DGS1",
    "2Y":  "DGS2",
    "3Y":  "DGS3",
    "5Y":  "DGS5",
    "7Y":  "DGS7",
    "10Y": "DGS10",
    "20Y": "DGS20",
    "30Y": "DGS30",
}

class USFredFetcher(BaseFetcher):
    economy = "US"
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY not found. export FRED_API_KEY=...")
    def _series(self, sid: str, start: date, end: date) -> pd.Series:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {"series_id": sid, "api_key": self.api_key, "file_type": "json",
                  "observation_start": start.isoformat(), "observation_end": end.isoformat()}
        r = requests.get(url, params=params, timeout=30); r.raise_for_status()
        data = r.json()["observations"]
        s = pd.Series({pd.to_datetime(x["date"]): (float(x["value"]) if x["value"] not in (".", None) else math.nan) for x in data}, name=sid)
        return s
    def fetch(self, start: date, end: date) -> CurveFetcherResult:
        series = {tenor: self._series(sid, start, end) for tenor, sid in FRED_SERIES.items()}
        df = pd.DataFrame(series).sort_index().ffill().dropna(how="all")
        return CurveFetcherResult(df=df, economy=self.economy)
