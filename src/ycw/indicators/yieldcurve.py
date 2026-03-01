import pandas as pd
from ..types import IndicatorResult
from .base import BaseIndicator

class YieldCurveIndicators(BaseIndicator):
    name = "yieldcurve"
    def compute(self, economy: str, df: pd.DataFrame) -> IndicatorResult:
        df = df.sort_index().copy()
        features, series = {}, {}
        if {"10Y","2Y","3M"}.issubset(df.columns):
            slope_10_2 = (df["10Y"] - df["2Y"]) * 100.0
            slope_10_3m = (df["10Y"] - df["3M"]) * 100.0
            features["slope_10Y_2Y_bps"] = float(slope_10_2.iloc[-1])
            features["slope_10Y_3M_bps"] = float(slope_10_3m.iloc[-1])
            features["inversion_10Y_2Y"] = features["slope_10Y_2Y_bps"] < 0
            features["inversion_10Y_3M"] = features["slope_10Y_3M_bps"] < 0
            series["slope_10Y_2Y_bps"] = slope_10_2
            series["slope_10Y_3M_bps"] = slope_10_3m
            d10 = df["10Y"].diff() * 100.0
            series["d10_bps"] = d10
            # Net 5-day cumulative move (absolute magnitude) — catches sustained
            # directional moves rather than a single volatile day in a quiet week.
            features["jump_10Y_last5d_bps"] = (
                abs(float(d10.tail(5).sum())) if d10.notna().any() else float("nan")
            )
            if len(df) >= 200:
                ma200 = df["10Y"].rolling(200).mean()
                prev = df["10Y"].shift(1) - ma200.shift(1)
                curr = df["10Y"] - ma200
                cross = None
                if prev.iloc[-1] < 0 <= curr.iloc[-1]: cross = "bullish_cross"
                elif prev.iloc[-1] > 0 >= curr.iloc[-1]: cross = "bearish_cross"
                features["ma200_cross_10Y"] = cross
        return IndicatorResult(economy=economy, features=features, series=series)
