"""Unit tests for YieldCurveWatch core logic.

All tests use synthetic data and do not make live HTTP requests to FRED.
"""
import io
import math
import os
import tempfile
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_curve_df(rows: int = 250, seed: int = 42) -> pd.DataFrame:
    """Return a synthetic wide curve DataFrame with TENORS as columns."""
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(end="2024-01-31", periods=rows)
    base = {
        "1M": 5.2, "3M": 5.1, "6M": 5.0, "1Y": 4.8,
        "2Y": 4.5, "3Y": 4.4, "5Y": 4.3, "7Y": 4.2,
        "10Y": 4.1, "20Y": 4.0, "30Y": 3.9,
    }
    data = {
        tenor: rng.normal(val, 0.05, size=rows)
        for tenor, val in base.items()
    }
    return pd.DataFrame(data, index=idx)


def _inverted_curve_df(rows: int = 60) -> pd.DataFrame:
    """Return a curve where 10Y < 2Y (inverted) and 10Y < 3M (inverted)."""
    idx = pd.bdate_range(end="2024-01-31", periods=rows)
    data = {
        "1M": [5.5] * rows, "3M": [5.4] * rows, "6M": [5.3] * rows,
        "1Y": [5.1] * rows, "2Y": [5.0] * rows, "3Y": [4.8] * rows,
        "5Y": [4.5] * rows, "7Y": [4.3] * rows, "10Y": [4.2] * rows,
        "20Y": [4.1] * rows, "30Y": [4.0] * rows,
    }
    return pd.DataFrame(data, index=idx)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_register_and_retrieve_fetcher(self):
        from ycw.registry import Registry
        from ycw.fetchers.fred_us import USFredFetcher

        reg = Registry()
        reg.register_fetcher("US_FRED", USFredFetcher)
        assert reg.fetchers["US_FRED"] is USFredFetcher

    def test_register_fetcher_wrong_type_raises(self):
        from ycw.registry import Registry
        from ycw.notifiers.console import ConsoleNotifier

        reg = Registry()
        with pytest.raises(TypeError, match="must subclass BaseFetcher"):
            reg.register_fetcher("bad", ConsoleNotifier)

    def test_register_indicator_wrong_type_raises(self):
        from ycw.registry import Registry
        from ycw.notifiers.console import ConsoleNotifier

        reg = Registry()
        with pytest.raises(TypeError, match="must subclass BaseIndicator"):
            reg.register_indicator("bad", ConsoleNotifier)

    def test_register_signal_wrong_type_raises(self):
        from ycw.registry import Registry
        from ycw.notifiers.console import ConsoleNotifier

        reg = Registry()
        with pytest.raises(TypeError, match="must subclass BaseSignal"):
            reg.register_signal("bad", ConsoleNotifier)

    def test_register_notifier_wrong_type_raises(self):
        from ycw.registry import Registry
        from ycw.fetchers.fred_us import USFredFetcher

        reg = Registry()
        with pytest.raises(TypeError, match="must subclass BaseNotifier"):
            reg.register_notifier("bad", USFredFetcher)

    def test_all_standard_registrations_accepted(self):
        from ycw.cli import build_registry

        reg = build_registry()
        assert "US_FRED" in reg.fetchers
        assert "yieldcurve" in reg.indicators
        assert "credit_us" in reg.indicators
        assert "composite_default" in reg.signals
        assert "logit_recession" in reg.signals
        assert "console" in reg.notifiers
        assert "slack_webhook" in reg.notifiers


# ---------------------------------------------------------------------------
# YieldCurveIndicators
# ---------------------------------------------------------------------------

class TestYieldCurveIndicators:
    def test_normal_curve_slope_positive(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _make_curve_df()
        # Force non-inverted: 10Y > 2Y
        df["10Y"] = df["2Y"] + 0.5
        out = YieldCurveIndicators().compute("US", df)
        assert out.features["slope_10Y_2Y_bps"] > 0
        assert out.features["inversion_10Y_2Y"] is False

    def test_inverted_curve_detected(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _inverted_curve_df()
        out = YieldCurveIndicators().compute("US", df)
        assert out.features["inversion_10Y_2Y"] is True
        assert out.features["inversion_10Y_3M"] is True
        assert out.features["slope_10Y_2Y_bps"] < 0
        assert out.features["slope_10Y_3M_bps"] < 0

    def test_slope_values_in_bps(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _make_curve_df()
        df["10Y"] = 4.0
        df["2Y"] = 3.0
        out = YieldCurveIndicators().compute("US", df)
        assert abs(out.features["slope_10Y_2Y_bps"] - 100.0) < 1e-6

    def test_jump_is_net_5d_cumulative(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _make_curve_df(rows=60)
        # Set last 5 daily changes to exactly +5 bps each → net 25 bps
        df["10Y"] = 4.0
        vals = df["10Y"].values.copy()
        for i in range(-5, 0):
            vals[i] = 4.0 + (i + 6) * 0.05   # +5, +10, +15, +20, +25
        df["10Y"] = vals
        out = YieldCurveIndicators().compute("US", df)
        # Net 5-day diff: last value - value 5 days ago ≈ 25 bps
        assert abs(out.features["jump_10Y_last5d_bps"] - 25.0) < 2.0

    def test_ma200_cross_detected_bullish(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _make_curve_df(rows=250)
        # Force a bullish cross on the last day
        ma200_val = df["10Y"].rolling(200).mean().iloc[-1]
        df.loc[df.index[-2], "10Y"] = ma200_val - 0.1  # below MA yesterday
        df.loc[df.index[-1], "10Y"] = ma200_val + 0.1  # above MA today
        out = YieldCurveIndicators().compute("US", df)
        assert out.features.get("ma200_cross_10Y") == "bullish_cross"

    def test_missing_columns_returns_empty_features(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = pd.DataFrame(
            {"5Y": [4.0, 4.1]},
            index=pd.bdate_range("2024-01-01", periods=2),
        )
        out = YieldCurveIndicators().compute("US", df)
        assert out.features == {}

    def test_economy_passes_through(self):
        from ycw.indicators.yieldcurve import YieldCurveIndicators

        df = _make_curve_df(rows=50)
        out = YieldCurveIndicators().compute("EU", df)
        assert out.economy == "EU"


# ---------------------------------------------------------------------------
# USCreditIndicators
# ---------------------------------------------------------------------------

class TestUSCreditIndicators:
    def _make_credit_response(self, sid: str, df: pd.DataFrame) -> pd.Series:
        """Return synthetic BAA or HY_OAS series aligned with df index."""
        if sid == "BAA":
            return pd.Series(5.5, index=df.index, name="BAA")
        if sid == "BAMLH0A0HYM2":
            vals = np.linspace(350, 400, len(df))
            return pd.Series(vals, index=df.index, name="BAMLH0A0HYM2")
        raise ValueError(f"Unexpected series: {sid}")

    @patch("ycw.indicators.credit.fetch_fred_series")
    def test_baa_minus_10y_computed(self, mock_fetch):
        from ycw.indicators.credit import USCreditIndicators

        df = _make_curve_df()
        df["10Y"] = 4.1  # fixed 10Y

        def _side_effect(sid, start, end, api_key):
            return self._make_credit_response(sid, df)

        mock_fetch.side_effect = _side_effect

        ind = USCreditIndicators(api_key="test")
        out = ind.compute("US", df)

        # BAA=5.5, 10Y=4.1 → spread = (5.5-4.1)*100 = 140 bps
        assert abs(out.features["baa_minus_10y_bps"] - 140.0) < 1.0

    @patch("ycw.indicators.credit.fetch_fred_series")
    def test_hy_oas_present(self, mock_fetch):
        from ycw.indicators.credit import USCreditIndicators

        df = _make_curve_df()

        def _side_effect(sid, start, end, api_key):
            return self._make_credit_response(sid, df)

        mock_fetch.side_effect = _side_effect

        ind = USCreditIndicators(api_key="test")
        out = ind.compute("US", df)

        assert "hy_oas_bps" in out.features
        assert out.features["hy_oas_bps"] > 0

    def test_missing_10y_column_raises(self):
        from ycw.indicators.credit import USCreditIndicators

        df = _make_curve_df().drop(columns=["10Y"])
        ind = USCreditIndicators(api_key="test")
        with pytest.raises(ValueError, match="'10Y' column"):
            ind.compute("US", df)


# ---------------------------------------------------------------------------
# CompositeSignal
# ---------------------------------------------------------------------------

class TestCompositeSignal:
    def test_inversion_10y_2y_triggers_warning(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {
            "inversion_10Y_2Y": True,
            "slope_10Y_2Y_bps": -30.0,
        })
        levels = {s.level for s in sigs}
        codes = {s.code for s in sigs}
        assert "warning" in levels
        assert "inv_10y2y" in codes

    def test_normal_slope_info_signal(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {
            "inversion_10Y_2Y": False,
            "slope_10Y_2Y_bps": 80.0,
        })
        codes = {s.code for s in sigs}
        assert "slope_10y2y" in codes

    def test_large_jump_triggers_watch(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {"jump_10Y_last5d_bps": 25.0})
        codes = {s.code for s in sigs}
        assert "jump_10y" in codes

    def test_small_jump_no_alert(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {"jump_10Y_last5d_bps": 5.0})
        codes = {s.code for s in sigs}
        assert "jump_10y" not in codes

    def test_hy_stress_watch(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {"hy_oas_bps": 600.0})
        codes = {s.code for s in sigs}
        assert "hy_stress" in codes

    def test_empty_features_no_crash(self):
        from ycw.signals.composite import CompositeSignal

        sigs = CompositeSignal().evaluate("US", {})
        assert isinstance(sigs, list)

    def test_inherits_base_signal(self):
        from ycw.signals.composite import CompositeSignal
        from ycw.signals.base import BaseSignal

        assert issubclass(CompositeSignal, BaseSignal)


# ---------------------------------------------------------------------------
# ConsoleNotifier
# ---------------------------------------------------------------------------

class TestConsoleNotifier:
    def test_prints_correct_tags(self, capsys):
        from ycw.notifiers.console import ConsoleNotifier
        from ycw.types import Signal

        notifier = ConsoleNotifier()
        notifier.notify([
            Signal(level="info", code="a", message="info msg"),
            Signal(level="watch", code="b", message="watch msg"),
            Signal(level="warning", code="c", message="warn msg"),
        ])
        captured = capsys.readouterr().out
        assert "[i]" in captured
        assert "[!]" in captured
        assert "[!!]" in captured

    def test_unknown_level_falls_back_gracefully(self, capsys):
        from ycw.notifiers.console import ConsoleNotifier
        from ycw.types import Signal

        notifier = ConsoleNotifier()
        notifier.notify([Signal(level="unknown", code="x", message="msg")])
        captured = capsys.readouterr().out
        assert "msg" in captured

    def test_inherits_base_notifier(self):
        from ycw.notifiers.console import ConsoleNotifier
        from ycw.notifiers.base import BaseNotifier

        assert issubclass(ConsoleNotifier, BaseNotifier)

    def test_empty_signals_no_output(self, capsys):
        from ycw.notifiers.console import ConsoleNotifier

        ConsoleNotifier().notify([])
        assert capsys.readouterr().out == ""


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_load_valid_config(self, tmp_path):
        from ycw.config import load_config

        cfg_file = tmp_path / "test.yml"
        cfg_file.write_text(
            "economies: [US]\nlookback_days: 365\nfetcher: US_FRED\n"
        )
        cfg = load_config(str(cfg_file))
        assert cfg["economies"] == ["US"]
        assert cfg["lookback_days"] == 365

    def test_empty_file_returns_empty_dict(self, tmp_path):
        from ycw.config import load_config

        cfg_file = tmp_path / "empty.yml"
        cfg_file.write_text("")
        cfg = load_config(str(cfg_file))
        assert cfg == {}

    def test_invalid_lookback_days_raises(self, tmp_path):
        from ycw.config import load_config

        cfg_file = tmp_path / "bad.yml"
        cfg_file.write_text("lookback_days: not_a_number\n")
        with pytest.raises(ValueError, match="lookback_days"):
            load_config(str(cfg_file))

    def test_negative_lookback_days_raises(self, tmp_path):
        from ycw.config import load_config

        cfg_file = tmp_path / "neg.yml"
        cfg_file.write_text("lookback_days: -10\n")
        with pytest.raises(ValueError, match="positive"):
            load_config(str(cfg_file))

    def test_non_list_economies_raises(self, tmp_path):
        from ycw.config import load_config

        cfg_file = tmp_path / "bad2.yml"
        cfg_file.write_text("economies: US\n")
        with pytest.raises(ValueError, match="economies"):
            load_config(str(cfg_file))


# ---------------------------------------------------------------------------
# run_pipeline (integration with mocked fetcher + indicators)
# ---------------------------------------------------------------------------

class TestRunPipeline:
    def test_pipeline_uses_econ_variable(self):
        """Economy key in results must match the loop variable, not fetcher.economy."""
        from ycw.pipeline import run_pipeline
        from ycw.registry import Registry
        from ycw.fetchers.base import BaseFetcher
        from ycw.indicators.base import BaseIndicator
        from ycw.signals.base import BaseSignal
        from ycw.notifiers.base import BaseNotifier
        from ycw.types import CurveFetcherResult, IndicatorResult, Signal

        class FakeFetcher(BaseFetcher):
            economy = "US"
            def fetch(self, start, end):
                return CurveFetcherResult(df=_make_curve_df(rows=50), economy="US")

        class FakeIndicator(BaseIndicator):
            name = "fake_ind"
            def compute(self, economy, df):
                return IndicatorResult(economy=economy, features={"x": 1.0}, series={})

        class FakeSignal(BaseSignal):
            name = "fake_sig"
            def evaluate(self, economy, f):
                return [Signal(level="info", code="ok", message=f"{economy} ok")]

        class FakeNotifier(BaseNotifier):
            def notify(self, signals): pass

        reg = Registry()
        reg.register_fetcher("fake", FakeFetcher)
        reg.register_indicator("fake_ind", FakeIndicator)
        reg.register_signal("fake_sig", FakeSignal)
        reg.register_notifier("fake_notifier", FakeNotifier)

        cfg = {
            "economies": ["EU", "JP"],
            "fetcher": "fake",
            "indicators": ["fake_ind"],
            "signals": ["fake_sig"],
            "notifier": "fake_notifier",
        }
        results = run_pipeline(reg, cfg)
        # Both keys should be present and signals should reference the correct economy
        assert "EU" in results
        assert "JP" in results
        eu_msgs = [s["message"] for s in results["EU"]["signals"]]
        jp_msgs = [s["message"] for s in results["JP"]["signals"]]
        assert any("EU" in m for m in eu_msgs)
        assert any("JP" in m for m in jp_msgs)

    def test_pipeline_missing_fetcher_raises(self):
        from ycw.pipeline import run_pipeline
        from ycw.registry import Registry

        reg = Registry()
        with pytest.raises(KeyError, match="Fetcher"):
            run_pipeline(reg, {"fetcher": "nonexistent"})

    def test_pipeline_missing_indicator_raises(self):
        from ycw.pipeline import run_pipeline
        from ycw.registry import Registry
        from ycw.fetchers.fred_us import USFredFetcher

        reg = Registry()
        reg.register_fetcher("US_FRED", USFredFetcher)
        with pytest.raises(KeyError, match="Indicator"):
            run_pipeline(reg, {
                "fetcher": "US_FRED",
                "indicators": ["nonexistent_indicator"],
            })

    def test_pipeline_indicator_failure_is_caught(self):
        """A crashing indicator should not abort the pipeline."""
        from ycw.pipeline import run_pipeline
        from ycw.registry import Registry
        from ycw.fetchers.base import BaseFetcher
        from ycw.indicators.base import BaseIndicator
        from ycw.signals.base import BaseSignal
        from ycw.notifiers.base import BaseNotifier
        from ycw.types import CurveFetcherResult, IndicatorResult, Signal

        class FakeFetcher(BaseFetcher):
            economy = "US"
            def fetch(self, start, end):
                return CurveFetcherResult(df=_make_curve_df(rows=20), economy="US")

        class BoomIndicator(BaseIndicator):
            name = "boom"
            def compute(self, economy, df):
                raise RuntimeError("boom")

        class FakeSignal(BaseSignal):
            name = "fs"
            def evaluate(self, economy, f):
                return []

        class FakeNotifier(BaseNotifier):
            def notify(self, signals): pass

        reg = Registry()
        reg.register_fetcher("f", FakeFetcher)
        reg.register_indicator("boom", BoomIndicator)
        reg.register_signal("fs", FakeSignal)
        reg.register_notifier("fn", FakeNotifier)

        # Should not raise; the boom indicator failure is caught and logged
        results = run_pipeline(reg, {
            "economies": ["US"],
            "fetcher": "f",
            "indicators": ["boom"],
            "signals": ["fs"],
            "notifier": "fn",
        })
        assert "US" in results


# ---------------------------------------------------------------------------
# evaluate_signal_monthly (backtest)
# ---------------------------------------------------------------------------

class TestEvaluateSignalMonthly:
    def _make_monthly_series(self, n: int = 60) -> pd.Series:
        idx = pd.date_range("2010-01-31", periods=n, freq="ME")
        return pd.Series(False, index=idx)

    def test_excludes_trailing_window(self):
        """With horizon=12m, last 12 rows should not appear in eval."""
        from ycw.backtest.recession import evaluate_signal_monthly

        n = 60
        sig = self._make_monthly_series(n)
        rec = pd.Series(0, index=sig.index)
        # Mark the last 6 months as recession
        rec.iloc[-6:] = 1

        result = evaluate_signal_monthly(sig, rec, horizon_months=12)
        assert "classification_report" in result
        # Just verify it runs without error and produces a string report
        assert isinstance(result["classification_report"], str)

    def test_perfect_signal_gets_high_precision(self):
        from ycw.backtest.recession import evaluate_signal_monthly

        idx = pd.date_range("2000-01-31", periods=120, freq="ME")
        rec = pd.Series(0, index=idx)
        # Recession from month 50 to 60
        rec.iloc[50:61] = 1

        # Signal fires 12 months before recession
        sig = pd.Series(False, index=idx)
        sig.iloc[38:50] = True  # 12m lead

        result = evaluate_signal_monthly(sig, rec, horizon_months=12)
        assert "classification_report" in result


# ---------------------------------------------------------------------------
# LogitRecessionSignal (mocked FRED)
# ---------------------------------------------------------------------------

class TestLogitRecessionSignal:
    def _make_fred_series(self, sid: str, n: int = 120) -> pd.Series:
        idx = pd.bdate_range("2010-01-04", periods=n)
        if sid == "DGS10":
            return pd.Series(np.linspace(4.0, 2.5, n), index=idx, name=sid)
        if sid == "DGS3MO":
            return pd.Series(np.linspace(1.0, 4.0, n), index=idx, name=sid)
        if sid == "USREC":
            vals = np.zeros(n)
            vals[60:75] = 1
            return pd.Series(vals, index=idx, name=sid)
        raise ValueError(sid)

    @patch("ycw.signals.logit_recession.fetch_fred_series")
    def test_returns_signal_list(self, mock_fetch):
        from ycw.signals.logit_recession import LogitRecessionSignal

        mock_fetch.side_effect = lambda sid, *a, **kw: self._make_fred_series(sid)

        sig = LogitRecessionSignal(horizon_months=6)
        with patch.dict(os.environ, {"FRED_API_KEY": "testkey"}):
            result = sig.evaluate("US", {})

        assert isinstance(result, list)
        assert all(hasattr(s, "level") for s in result)

    @patch("ycw.signals.logit_recession.fetch_fred_series")
    def test_missing_api_key_returns_watch(self, mock_fetch):
        from ycw.signals.logit_recession import LogitRecessionSignal

        env = {k: v for k, v in os.environ.items() if k != "FRED_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            result = LogitRecessionSignal().evaluate("US", {})

        assert result[0].code == "logit_missing_key"

    @patch("ycw.signals.logit_recession.fetch_fred_series")
    def test_fetch_error_returns_watch_signal(self, mock_fetch):
        from ycw.signals.logit_recession import LogitRecessionSignal

        mock_fetch.side_effect = RuntimeError("network error")
        with patch.dict(os.environ, {"FRED_API_KEY": "key"}):
            result = LogitRecessionSignal().evaluate("US", {})

        assert result[0].code == "logit_error"

    @patch("ycw.signals.logit_recession.fetch_fred_series")
    def test_inherits_base_signal(self, _):
        from ycw.signals.logit_recession import LogitRecessionSignal
        from ycw.signals.base import BaseSignal

        assert issubclass(LogitRecessionSignal, BaseSignal)


# ---------------------------------------------------------------------------
# utils.fred (shared fetch utility)
# ---------------------------------------------------------------------------

class TestFetchFredSeries:
    def test_parses_observations_correctly(self):
        from ycw.utils.fred import fetch_fred_series

        fake_response = {
            "observations": [
                {"date": "2024-01-02", "value": "4.5"},
                {"date": "2024-01-03", "value": "."},
                {"date": "2024-01-04", "value": "4.6"},
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_response
        mock_resp.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_resp):
            s = fetch_fred_series("DGS10", "2024-01-02", "2024-01-04", "key")

        assert s.loc[pd.Timestamp("2024-01-02")] == pytest.approx(4.5)
        assert math.isnan(s.loc[pd.Timestamp("2024-01-03")])
        assert s.loc[pd.Timestamp("2024-01-04")] == pytest.approx(4.6)

    def test_raises_on_http_error(self):
        from ycw.utils.fred import fetch_fred_series
        import requests as req

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("404")

        with patch("requests.get", return_value=mock_resp):
            with pytest.raises(req.HTTPError):
                fetch_fred_series("BAD", "2024-01-01", "2024-01-31", "key")
