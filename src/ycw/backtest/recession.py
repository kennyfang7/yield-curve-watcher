import numpy as np
import pandas as pd
from datetime import date
from sklearn.linear_model import LogisticRegression
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


def walk_forward_logit_backtest(
    api_key: str,
    start: str = "1962-01-01",
    horizon_months: int = 12,
    min_train_samples: int = 60,
) -> dict:
    """Walk-forward (expanding window) backtest of the logit recession model.

    At each month *t* a LogisticRegression is trained **only** on months
    strictly before *t* whose forward recession label is already known, then
    used to predict the recession probability for *t*.  This eliminates the
    look-ahead bias that would arise from training once on the full history and
    back-filling predictions.

    Parameters
    ----------
    api_key:
        FRED API key.
    start:
        ISO date string for the start of the data download.
    horizon_months:
        Forecast horizon (months).  Used for both label construction and
        evaluation — a prediction at *t* is correct if ``USREC`` is 1 at any
        point in ``(t, t + horizon_months]``.
    min_train_samples:
        Minimum number of labelled training months required before the first
        prediction is produced.  Avoids noisy estimates early in the sample.

    Returns
    -------
    dict with keys:

    ``"probabilities"``
        :class:`pandas.Series` of out-of-sample predicted recession
        probabilities, indexed by month-end date.  Months for which there were
        insufficient training data are omitted.

    ``"classification_report"``
        ``str`` — sklearn classification report comparing predictions
        (threshold 0.5) to realised recession labels, restricted to months
        where a full *horizon_months* evaluation window has elapsed.
    """
    end = pd.Timestamp.today().strftime("%Y-%m-%d")

    d10 = fetch_fred_series("DGS10", start, end, api_key)
    d3m = fetch_fred_series("DGS3MO", start, end, api_key)
    rec_raw = fetch_fred_series("USREC", start, end, api_key)

    # All series aligned to month-end.
    slope: pd.Series = (d10 - d3m).resample("ME").last().dropna()
    rec: pd.Series = rec_raw.resample("ME").last()  # NaN = label not yet published

    dates = slope.index
    probs: dict[pd.Timestamp, float] = {}

    for i, t in enumerate(dates):
        # Build training set from months s strictly before t.
        # Label for s: did a recession occur within horizon_months after s?
        # Skip s whose forward label is missing (NBER not yet published).
        X_train: list[list[float]] = []
        y_train: list[int] = []

        for s in dates[:i]:
            label_date = s + pd.DateOffset(months=horizon_months)
            label_val = rec.reindex([label_date])
            if label_val.isna().all():
                continue
            X_train.append([float(slope.loc[s])])
            y_train.append(int(label_val.iloc[0] > 0))

        if len(X_train) < min_train_samples:
            continue
        if len(set(y_train)) < 2:
            # Cannot fit a binary classifier with only one class present.
            continue

        model = LogisticRegression(max_iter=1000)
        model.fit(np.array(X_train), np.array(y_train))
        prob = float(model.predict_proba([[float(slope.loc[t])]])[0, 1])
        probs[t] = prob

    prob_series = pd.Series(probs, name="recession_prob")

    if prob_series.empty:
        return {
            "probabilities": prob_series,
            "classification_report": "Insufficient data for evaluation.",
        }

    # Restrict evaluation to months where the full forward window has elapsed.
    max_eval_date = rec.index.max() - pd.DateOffset(months=horizon_months)
    eval_dates = prob_series.index[prob_series.index <= max_eval_date]

    y_true: list[bool] = []
    y_pred_binary: list[bool] = []
    for t in eval_dates:
        fut = rec.loc[t: t + pd.DateOffset(months=horizon_months)]
        y_true.append(bool(fut.dropna().any()))
        y_pred_binary.append(bool(prob_series.loc[t] >= 0.5))

    report = classification_report(
        y_true,
        y_pred_binary,
        target_names=["No Recession Soon", "Recession Soon"],
        output_dict=False,
        zero_division=0,
    )

    return {
        "probabilities": prob_series,
        "classification_report": report,
    }
