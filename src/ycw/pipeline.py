from datetime import date, timedelta
from typing import Dict, Any, List
import pandas as pd
from .registry import Registry
from .types import Signal

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
        raise KeyError(f"Fetcher {fetcher_name} not registered")
    fetcher = reg.fetchers[fetcher_name]()
    indicators = [reg.indicators[name]() for name in indicator_names]
    signal_makers = [reg.signals[name]() for name in signal_names]
    notifier = reg.notifiers[notifier_name]()

    results: Dict[str, Any] = {}

    for econ in economies:
        res = fetcher.fetch(start, today)
        df = res.df

        all_features = {}
        for ind in indicators:
            out = ind.compute(res.economy, df)
            all_features.update(out.features)

        sigs: List[Signal] = []
        for sm in signal_makers:
            sigs.extend(sm.evaluate(res.economy, all_features))

        notifier.notify(sigs)

        results[econ] = {
            "latest_date": pd.to_datetime(df.index.max()).strftime("%Y-%m-%d"),
            "latest_yields_pct": {k: float(df[k].iloc[-1]) for k in df.columns if not pd.isna(df[k].iloc[-1])},
            "indicators": all_features,
            "signals": [s.__dict__ for s in sigs],
        }
    return results
