import requests, pandas as pd
from sklearn.metrics import classification_report

def fetch_usrec(start, end, api_key: str) -> pd.Series:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id":"USREC","api_key":api_key,"file_type":"json","observation_start":start.isoformat(),"observation_end":end.isoformat()}
    r = requests.get(url, params=params, timeout=30); r.raise_for_status()
    data = r.json()["observations"]
    s = pd.Series({pd.to_datetime(x["date"]): int(float(x["value"])) if x["value"] not in (".", None) else 0 for x in data}, name="USREC")
    return s

def evaluate_signal_monthly(signal_ts: pd.Series, recession_ts: pd.Series, horizon_months: int = 12) -> dict:
    sig = signal_ts.resample("M").last().fillna(False).astype(bool)
    rec = recession_ts.resample("M").last().fillna(0).astype(int) > 0
    y_true, y_pred = [], []
    for d in sig.index:
        fut = rec.loc[d: d + pd.DateOffset(months=horizon_months)]
        y_true.append(bool(fut.any()))
        y_pred.append(bool(sig.loc[d]))
    report = classification_report(y_true, y_pred, target_names=["No Recession Soon","Recession Soon"], output_dict=False, zero_division=0)
    return {"classification_report": report}
