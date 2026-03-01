import os
import pandas as pd
from datetime import date
from ..types import CurveFetcherResult
from .base import BaseFetcher
from ..utils.fred import fetch_fred_series

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

# Maximum number of consecutive days to forward-fill; avoids propagating
# arbitrarily stale rates across extended data outages.
_FFILL_LIMIT = 10


class USFredFetcher(BaseFetcher):
    economy = "US"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY not found. export FRED_API_KEY=...")

    def fetch(self, start: date, end: date) -> CurveFetcherResult:
        series = {
            tenor: fetch_fred_series(
                sid, start.isoformat(), end.isoformat(), self.api_key
            )
            for tenor, sid in FRED_SERIES.items()
        }
        df = (
            pd.DataFrame(series)
            .sort_index()
            .ffill(limit=_FFILL_LIMIT)
            .dropna(how="all")
        )
        return CurveFetcherResult(df=df, economy=self.economy)
