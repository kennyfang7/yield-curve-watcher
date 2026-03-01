import pandas as pd
from datetime import date
from sklearn.metrics import classification_report
from ..utils.fred import fetch_fred_series


def fetch_usrec(start: date, end: date, api_key: str) -> pd.Series:
    """Fetch the NBER recession indicator (USREC) from FRED.

    Missing values are returned as NaN (not coerced to 0) so that callers
    can decide how to handle periods where NBER has not yet published a
    recession determination.
    """
    s = fetch_fred_series(
        "USREC", start.isoformat(), end.isoformat(), api_key
    )
    s.name = "USREC"
    return s


def evaluate_signal_monthly(
    signal_ts: pd.Series,
    recession_ts: pd.Series,
    horizon_months: int = 12,
) -> dict:
    """Evaluate signal accuracy against future recession labels.

    The last ``horizon_months`` observations are excluded from evaluation
    because their future recession labels cannot be known yet — including them
    would silently label the trailing window as "no recession" and artificially
    inflate precision.
    """
    sig = signal_ts.resample("ME").last().fillna(False).astype(bool)
    rec = recession_ts.resample("ME").last().fillna(0).astype(int) > 0

    # Determine the latest date for which a full horizon window exists.
    max_label_date = rec.index.max() - pd.DateOffset(months=horizon_months)
    sig = sig[sig.index <= max_label_date]

    y_true, y_pred = [], []
    for d in sig.index:
        fut = rec.loc[d: d + pd.DateOffset(months=horizon_months)]
        y_true.append(bool(fut.any()))
        y_pred.append(bool(sig.loc[d]))

    report = classification_report(
        y_true,
        y_pred,
        target_names=["No Recession Soon", "Recession Soon"],
        output_dict=False,
        zero_division=0,
    )
    return {"classification_report": report}
