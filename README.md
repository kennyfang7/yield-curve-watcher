# YieldCurveWatch (YCW)

A modular, plugin-ready framework for monitoring yield curves and credit spreads, generating macro stress alerts, backtesting predictive accuracy, a Streamlit dashboard, and a logit-based recession probability signal.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

export FRED_API_KEY=YOUR_KEY
python -m ycw.cli run --config examples/config.example.yml
```

## Dashboard

```bash
streamlit run dashboard/app.py
```
