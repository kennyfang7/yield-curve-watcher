from dataclasses import dataclass
from typing import Any, Dict, List
import pandas as pd

TENORS = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]


@dataclass
class CurveFetcherResult:
    df: pd.DataFrame     # wide, index=DatetimeIndex, columns in TENORS (percent, levels)
    economy: str         # e.g., US, UK, EZ


@dataclass
class IndicatorResult:
    economy: str
    features: Dict[str, Any]          # scalar features (latest value)
    series: Dict[str, pd.Series]      # timeseries by key


@dataclass
class Signal:
    level: str     # "info", "watch", "warning"
    code: str
    message: str
    economy: str = ""


@dataclass
class RunOutput:
    latest_date: str
    latest_yields_pct: Dict[str, float]
    indicators: Dict[str, Any]
    signals: List[Signal]
