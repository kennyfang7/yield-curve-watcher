import logging
import math
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from typing import List, Dict

from ..types import Signal
from .base import BaseSignal
from ..cache import FredCache

logger = logging.getLogger(__name__)

SERIES = {"DGS10": "DGS10", "DGS3MO": "DGS3MO", "USREC": "USREC"}


class LogitRecessionSignal(BaseSignal):
    name = "logit_recession"

    def __init__(self, horizon_months: int = 12, cache: FredCache | None = None):
        self.h = horizon_months
        self._cached_prob: float | None = None
        self._cache = cache  # Resolved lazily in _train_and_nowcast if None.

    def _train_and_nowcast(self, api_key: str) -> float:
        if self._cached_prob is not None:
            return self._cached_prob

        cache = self._cache or FredCache(api_key)
        start = "1962-01-01"
        end = pd.Timestamp.today().strftime("%Y-%m-%d")

        d10 = cache.get(SERIES["DGS10"], start, end)
        d3m = cache.get(SERIES["DGS3MO"], start, end)

        # USREC missing values should not be silently treated as "no recession".
        # Keep them as NaN so they are excluded from training labels.
        rec_raw = cache.get(SERIES["USREC"], start, end)
        rec = rec_raw.resample("ME").last()

        curve = (d10 - d3m)
        s = curve.resample("ME").last().dropna()

        X, y = [], []
        # Exclude the most recent `h` months from training to avoid using
        # NBER labels that wouldn't yet have been published at the time of
        # observation (NBER typically announces recessions 6–18 months late).
        training_cutoff = s.index[-1] - pd.DateOffset(months=self.h)
        for t, val in s.items():
            if t > training_cutoff:
                continue
            future_end = t + pd.DateOffset(months=self.h)
            # Use .get() semantics via reindex to avoid KeyError on sparse index
            future_vals = rec.reindex([future_end])
            if future_vals.isna().all():
                continue
            X.append([val])
            y.append(int(future_vals.iloc[0] > 0))

        if len(set(y)) < 2:
            return float("nan")

        model = LogisticRegression(max_iter=1000)
        model.fit(np.array(X), np.array(y))

        latest_spread = float(s.iloc[-1])
        prob = float(model.predict_proba([[latest_spread]])[0, 1])
        self._cached_prob = prob
        return prob

    def evaluate(self, economy: str, f: Dict[str, float]) -> List[Signal]:
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            return [Signal(
                level="watch",
                code="logit_missing_key",
                message=f"[{economy}] Set FRED_API_KEY to compute logit recession probability",
            )]
        try:
            p = self._train_and_nowcast(api_key)
        except Exception:
            logger.exception("Logit recession signal failed for economy '%s'", economy)
            return [Signal(
                level="watch",
                code="logit_error",
                message=f"[{economy}] Logit model error — see logs for details",
            )]

        if math.isnan(p):
            return [Signal(
                level="info",
                code="logit_na",
                message=f"[{economy}] Logit probability unavailable",
            )]

        sigs: List[Signal] = []
        sigs.append(Signal(
            level="info",
            code="logit_prob",
            message=f"[{economy}] Recession probability (next {self.h}m): {p * 100:.1f}%",
        ))
        if p >= 0.5:
            sigs.append(Signal(
                level="warning",
                code="logit_high",
                message=f"[{economy}] High recession risk per logit ({p * 100:.1f}%)",
            ))
        elif p >= 0.3:
            sigs.append(Signal(
                level="watch",
                code="logit_watch",
                message=f"[{economy}] Elevated recession risk per logit ({p * 100:.1f}%)",
            ))
        return sigs
