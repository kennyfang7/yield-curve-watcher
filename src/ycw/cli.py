import argparse, json
from .config import load_config
from .registry import Registry
from .pipeline import run_pipeline

# force imports to register
from .fetchers.fred_us import USFredFetcher
from .indicators.yieldcurve import YieldCurveIndicators
from .indicators.credit import USCreditIndicators
from .signals.composite import CompositeSignal
from .signals.logit_recession import LogitRecessionSignal
from .notifiers.console import ConsoleNotifier
from .notifiers.slack_webhook import SlackWebhookNotifier

def build_registry():
    reg = Registry()
    reg.register_fetcher("US_FRED", USFredFetcher)
    reg.register_indicator("yieldcurve", YieldCurveIndicators)
    reg.register_indicator("credit_us", USCreditIndicators)
    reg.register_signal("composite_default", CompositeSignal)
    reg.register_signal("logit_recession", LogitRecessionSignal)
    reg.register_notifier("console", ConsoleNotifier)
    reg.register_notifier("slack_webhook", SlackWebhookNotifier)
    return reg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run"], help="run")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--json_out", default="", help="Write results JSON to this path")
    args = parser.parse_args()

    cfg = load_config(args.config)
    reg = build_registry()
    results = run_pipeline(reg, cfg)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
