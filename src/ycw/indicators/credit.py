import os, pandas as pd, requests, math
from ..types import IndicatorResult
from .base import BaseIndicator

SERIES = {"BAA_YIELD": "BAA", "DGS10": "DGS10", "HY_OAS": "BAMLH0A0HYM2"}

class USCreditIndicators(BaseIndicator):
    name = "credit_us"
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key: raise RuntimeError("FRED_API_KEY not found for credit indicators.")
    def _fred_series(self, sid: str, start, end) -> pd.Series:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {"series_id": sid, "api_key": self.api_key, "file_type": "json",
                  "observation_start": start.isoformat(), "observation_end": end.isoformat()}
        r = requests.get(url, params=params, timeout=30); r.raise_for_status()
        data = r.json()["observations"]
        s = pd.Series({pd.to_datetime(x["date"]): (float(x["value"]) if x["value"] not in (".", None) else math.nan) for x in data})
        return s
    def compute(self, economy: str, df: pd.DataFrame) -> IndicatorResult:
        start, end = df.index.min().date(), df.index.max().date()
        baa = self._fred_series(SERIES["BAA_YIELD"], start, end)
        dgs10 = self._fred_series(SERIES["DGS10"], start, end)
        hy = self._fred_series(SERIES["HY_OAS"], start, end)
        mat = pd.concat([baa.rename("BAA"), dgs10.rename("DGS10"), hy.rename("HY_OAS")], axis=1).sort_index().ffill()
        features, series = {}, {}
        baa10 = (mat["BAA"] - mat["DGS10"]) * 100.0
        features["baa_minus_10y_bps"] = float(baa10.iloc[-1]); series["baa_minus_10y_bps"] = baa10
        features["hy_oas_bps"] = float(mat["HY_OAS"].iloc[-1]); series["hy_oas_bps"] = mat["HY_OAS"]
        hy_mom = mat["HY_OAS"].diff(21)
        series["hy_oas_1m_change_bps"] = hy_mom
        features["hy_oas_1m_change_bps"] = float(hy_mom.iloc[-1]) if hy_mom.notna().any() else float("nan")
        return IndicatorResult(economy=economy, features=features, series=series)
