import streamlit as st
import pandas as pd
import altair as alt
from ycw.cli import build_registry
from ycw.pipeline import run_pipeline
from ycw.config import load_config

st.set_page_config(page_title="Yield Curve Watch", layout="wide")
st.title("📈 Yield Curve & Credit Spread Watch")

cfg = load_config("examples/config.example.yml")
reg = build_registry()
results = run_pipeline(reg, cfg)

for econ, out in results.items():
    st.header(f"{econ} — {out['latest_date']}")

    # Yield curve
    yields = pd.Series(out["latest_yields_pct"]).reset_index()
    yields.columns = ["Tenor", "Yield"]
    st.subheader("Latest Yield Curve")
    st.altair_chart(
        alt.Chart(yields).mark_line(point=True).encode(
            x=alt.X("Tenor:N", sort=None),
            y=alt.Y("Yield:Q")
        ),
        use_container_width=True
    )

    # Indicators (key ones)
    st.subheader("Key Indicators")
    key = {k: v for k, v in out["indicators"].items() if k in ["slope_10Y_2Y_bps","slope_10Y_3M_bps","baa_minus_10y_bps","hy_oas_bps"]}
    st.json(key)

    # Signals
    st.subheader("Signals / Alerts")
    for sig in out["signals"]:
        if sig["level"] == "warning": st.error(sig["message"])
        elif sig["level"] == "watch": st.warning(sig["message"])
        else: st.info(sig["message"])
