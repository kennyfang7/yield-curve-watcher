import logging
from datetime import date, timedelta
from typing import Dict, Any, List
import pandas as pd
from .registry import Registry
from .types import Signal

logger = logging.getLogger(__name__)


def run_pipeline(reg: Registry, cfg: Dict[str, Any]) -> Dict[str, Any]:
    today = date.today()
    lookback_days = int(cfg.get("lookback_days", 730))
    start = today - timedelta(days=lookback_days)

    economies = cfg.get("economies", ["US"])
    fetcher_name = cfg.get("fetcher", "US_FRED")
    indicator_names = cfg.get("indicators", ["yieldcurve", "credit_us"])
    signal_names = cfg.get("signals", ["composite_default", "logit_recession"])
    notifier_name = cfg.get("notifier", "console")

    if fetcher_name not in reg.fetchers:
        raise KeyError(
            f"Fetcher '{fetcher_name}' not registered. "
            f"Available: {list(reg.fetchers.keys())}"
        )
    for name in indicator_names:
        if name not in reg.indicators:
            raise KeyError(
                f"Indicator '{name}' not registered. "
                f"Available: {list(reg.indicators.keys())}"
            )
    for name in signal_names:
        if name not in reg.signals:
            raise KeyError(
                f"Signal '{name}' not registered. "
                f"Available: {list(reg.signals.keys())}"
            )
    if notifier_name not in reg.notifiers:
        raise KeyError(
            f"Notifier '{notifier_name}' not registered. "
            f"Available: {list(reg.notifiers.keys())}"
        )

    fetcher = reg.fetchers[fetcher_name]()
    indicators = [reg.indicators[name]() for name in indicator_names]
    signal_makers = [reg.signals[name]() for name in signal_names]
    notifier = reg.notifiers[notifier_name]()

    results: Dict[str, Any] = {}

    for econ in economies:
        res = fetcher.fetch(start, today)
        df = res.df

        all_features: Dict[str, Any] = {}
        for ind in indicators:
            try:
                out = ind.compute(econ, df)
                all_features.update(out.features)
            except Exception:
                logger.exception("Indicator '%s' failed for economy '%s'", ind.name, econ)

        sigs: List[Signal] = []
        for sm in signal_makers:
            try:
                sigs.extend(sm.evaluate(econ, all_features))
            except Exception:
                logger.exception("Signal '%s' failed for economy '%s'", sm.name, econ)

        notifier.notify(sigs)

        if df.empty or df.index.empty:
            latest_date = "N/A"
        else:
            latest_date = pd.to_datetime(df.index.max()).strftime("%Y-%m-%d")

        results[econ] = {
            "latest_date": latest_date,
            "latest_yields_pct": {
                k: float(df[k].iloc[-1])
                for k in df.columns
                if not df.empty and not pd.isna(df[k].iloc[-1])
            },
            "indicators": all_features,
            "signals": [s.__dict__ for s in sigs],
        }
    return results
