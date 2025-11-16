import os, math, requests, numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from typing import List, Dict
from ..types import Signal

SERIES = {"DGS10":"DGS10", "DGS3MO":"DGS3MO", "USREC":"USREC"}

def fred_series(sid: str, start: str, end: str, api_key: str) -> pd.Series:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": sid, "api_key": api_key, "file_type": "json", "observation_start": start, "observation_end": end}
    r = requests.get(url, params=params, timeout=30); r.raise_for_status()
    data = r.json()["observations"]
    def to_val(x): 
        v = x.get("value", ".")
        if v in (".", None): return np.nan
        try: return float(v)
        except: return np.nan
    s = pd.Series({pd.to_datetime(x["date"]): to_val(x) for x in data}, name=sid)
    return s

class LogitRecessionSignal:
    name = "logit_recession"
    def __init__(self, horizon_months: int = 12):
        self.h = horizon_months
    def _train_and_nowcast(self, api_key: str) -> float:
        start, end = "1962-01-01", pd.Timestamp.today().strftime("%Y-%m-%d")
        d10 = fred_series(SERIES["DGS10"], start, end, api_key)
        d3m = fred_series(SERIES["DGS3MO"], start, end, api_key)
        rec = fred_series(SERIES["USREC"], start, end, api_key).fillna(0.0)
        curve = (d10 - d3m)
        s = curve.resample("M").last().dropna()
        r = rec.resample("M").last().astype(int)
        X, y = [], []
        for t, val in s.items():
            future_end = t + pd.DateOffset(months=self.h)
            if future_end in r.index:
                X.append([val]); y.append(int(r.loc[future_end] > 0))
        if len(set(y)) < 2: return float("nan")
        model = LogisticRegression()
        model.fit(np.array(X), np.array(y))
        latest_spread = float(s.iloc[-1])
        prob = model.predict_proba([[latest_spread]])[0,1]
        return float(prob)
    def evaluate(self, economy: str, f: Dict[str, float]) -> List[Signal]:
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            return [Signal(level="watch", code="logit_missing_key", message=f"[{economy}] Set FRED_API_KEY to compute logit recession probability")]
        try:
            p = self._train_and_nowcast(api_key)
        except Exception as e:
            return [Signal(level="watch", code="logit_error", message=f"[{economy}] Logit model error: {e}")]
        if math.isnan(p): 
            return [Signal(level="info", code="logit_na", message=f"[{economy}] Logit probability unavailable")]
        sigs: List[Signal] = []
        sigs.append(Signal(level="info", code="logit_prob", message=f"[{economy}] Recession probability (next {self.h}m): {p*100:.1f}%"))
        if p >= 0.5: sigs.append(Signal(level="warning", code="logit_high", message=f"[{economy}] High recession risk per logit ({p*100:.1f}%)"))
        elif p >= 0.3: sigs.append(Signal(level="watch", code="logit_watch", message=f"[{economy}] Elevated recession risk per logit ({p*100:.1f}%)"))
        return sigs
