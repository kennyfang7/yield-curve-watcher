import os
import pandas as pd
from ..types import IndicatorResult
from .base import BaseIndicator
from ..utils.fred import fetch_fred_series

SERIES = {"BAA_YIELD": "BAA", "HY_OAS": "BAMLH0A0HYM2"}

# Maximum consecutive days to forward-fill credit series.
_FFILL_LIMIT = 10


class USCreditIndicators(BaseIndicator):
    name = "credit_us"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY not found for credit indicators.")

    def compute(self, economy: str, df: pd.DataFrame) -> IndicatorResult:
        # Validate early before making any network calls.
        if "10Y" not in df.columns:
            raise ValueError(
                "credit_us indicator requires '10Y' column in curve DataFrame"
            )

        start = df.index.min().date().isoformat()
        end = df.index.max().date().isoformat()

        baa = fetch_fred_series(SERIES["BAA_YIELD"], start, end, self.api_key)
        hy = fetch_fred_series(SERIES["HY_OAS"], start, end, self.api_key)

        # Reuse DGS10 already fetched by USFredFetcher (stored as "10Y" column).
        # This avoids a redundant HTTP request and ensures temporal alignment.
        dgs10 = df["10Y"].rename("DGS10")

        mat = (
            pd.concat(
                [baa.rename("BAA"), dgs10, hy.rename("HY_OAS")],
                axis=1,
            )
            .sort_index()
            .ffill(limit=_FFILL_LIMIT)
        )

        features: dict = {}
        series: dict = {}

        baa10 = (mat["BAA"] - mat["DGS10"]) * 100.0
        features["baa_minus_10y_bps"] = float(baa10.iloc[-1])
        series["baa_minus_10y_bps"] = baa10

        features["hy_oas_bps"] = float(mat["HY_OAS"].iloc[-1])
        series["hy_oas_bps"] = mat["HY_OAS"]

        # 1-month change: resample to month-end, diff by 1 period, then
        # reindex back to daily for consistency.  More accurate than diff(21).
        hy_monthly = mat["HY_OAS"].resample("ME").last()
        hy_mom_monthly = hy_monthly.diff(1)
        features["hy_oas_1m_change_bps"] = (
            float(hy_mom_monthly.iloc[-1])
            if hy_mom_monthly.notna().any()
            else float("nan")
        )
        series["hy_oas_1m_change_bps"] = hy_mom_monthly

        return IndicatorResult(economy=economy, features=features, series=series)
