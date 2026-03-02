"""Tests for SignalHistory — no live I/O, uses tmp_path for JSON file."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from ycw.types import Signal


def _sig(code="inv_10y2y", level="warning", economy="US") -> Signal:
    return Signal(level=level, code=code, message="test msg", economy=economy)


class TestFirstRun:
    def test_all_signals_pass_on_first_run(self, tmp_path):
        from ycw.signal_history import SignalHistory

        h = SignalHistory(path=tmp_path / "hist.json")
        sigs = [_sig("a"), _sig("b")]
        assert h.filter_new(sigs) == sigs


class TestDuplicate:
    def test_duplicate_suppressed_on_second_run(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        sig = _sig()

        h = SignalHistory(path=p)
        h.filter_new([sig])
        h.update([sig])

        h2 = SignalHistory(path=p)
        assert h2.filter_new([sig]) == []


class TestLevelChange:
    def test_level_change_fires_again(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        watch_sig = _sig(level="watch")

        h = SignalHistory(path=p)
        h.filter_new([watch_sig])
        h.update([watch_sig])

        h2 = SignalHistory(path=p)
        warn_sig = _sig(level="warning")
        result = h2.filter_new([warn_sig])
        assert result == [warn_sig]


class TestClearedAndRefired:
    def test_cleared_then_refired_notifies(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        sig = _sig()

        # Run 1: signal fires
        h = SignalHistory(path=p)
        h.filter_new([sig])
        h.update([sig])

        # Run 2: signal absent (cleared)
        h = SignalHistory(path=p)
        h.filter_new([])
        h.update([])

        # Run 3: signal re-appears → should notify again
        h = SignalHistory(path=p)
        result = h.filter_new([sig])
        assert result == [sig]


class TestTTL:
    def test_ttl_expiry_re_notifies(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        p.write_text(json.dumps({
            "inv_10y2y|warning|US": {
                "first_seen": old_time,
                "last_seen": old_time,
                "active": True,
            }
        }))

        h = SignalHistory(ttl_days=7, path=p)
        result = h.filter_new([_sig()])
        assert result == [_sig()]

    def test_ttl_not_expired_suppresses(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        recent_time = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        p.write_text(json.dumps({
            "inv_10y2y|warning|US": {
                "first_seen": recent_time,
                "last_seen": recent_time,
                "active": True,
            }
        }))

        h = SignalHistory(ttl_days=7, path=p)
        assert h.filter_new([_sig()]) == []

    def test_ttl_reset_after_renotify(self, tmp_path):
        """After TTL fires, first_seen should reset so it doesn't fire every run."""
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        p.write_text(json.dumps({
            "inv_10y2y|warning|US": {
                "first_seen": old_time,
                "last_seen": old_time,
                "active": True,
            }
        }))

        sig = _sig()
        h = SignalHistory(ttl_days=7, path=p)
        h.filter_new([sig])
        h.update([sig])   # should reset first_seen to now

        h2 = SignalHistory(ttl_days=7, path=p)
        assert h2.filter_new([sig]) == []  # suppressed again


class TestCorruptJSON:
    def test_corrupt_json_treated_as_empty(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        p.write_text("not { valid json [[[")

        h = SignalHistory(path=p)
        result = h.filter_new([_sig()])
        assert result == [_sig()]  # all signals pass, no exception


class TestEconomyIsolation:
    def test_same_code_different_economy_both_notify(self, tmp_path):
        from ycw.signal_history import SignalHistory

        p = tmp_path / "hist.json"
        us_sig = _sig(economy="US")
        uk_sig = _sig(economy="UK")

        # Fire and suppress US
        h = SignalHistory(path=p)
        h.filter_new([us_sig])
        h.update([us_sig])

        # UK should still notify even though US is suppressed
        h2 = SignalHistory(path=p)
        result = h2.filter_new([us_sig, uk_sig])
        assert us_sig not in result
        assert uk_sig in result
