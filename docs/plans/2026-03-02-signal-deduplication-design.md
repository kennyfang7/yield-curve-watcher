# Signal Deduplication / Alert History Design

**Date:** 2026-03-02
**Status:** Approved

## Problem

The pipeline has no memory between runs. Every execution re-fires every active signal,
causing alert fatigue when the same condition persists for days or weeks.

## Goal

Suppress repeat notifications for signals that have already fired and not changed.
Re-notify only when something genuinely new happens: a signal clears and re-fires,
or a signal has been continuously active long enough to warrant a reminder (TTL).

## Deduplication Key

`(code, level, economy)` — a level change on the same code (e.g. `watch` → `warning`)
counts as a new alert. Same code firing for different economies notifies independently.

This requires adding `economy: str` as an explicit field on `Signal` (currently it is
only embedded in the message string).

## Re-notification Triggers

A suppressed signal becomes new again when **either**:
- It was absent from at least one run (cleared), then re-appears.
- It has been continuously active for `ttl_days` days (default 7), as a reminder.

Whichever comes first.

## Architecture

Approach A: pipeline-level filter. A `SignalHistory` instance is created in
`run_pipeline()` before the economy loop. After signals are generated and before
`notifier.notify()`, the pipeline calls `history.filter_new(sigs)` and
`history.update(sigs)`. Notifiers receive only novel signals; the full signal list
is still written to the JSON run output.

## Components

### 1. `Signal` dataclass — `src/ycw/types.py`

Add `economy: str` field. Update all `Signal(...)` call sites in
`composite.py` and `logit_recession.py` to pass `economy=economy`.

### 2. `SignalHistory` — `src/ycw/signal_history.py` (new)

**Storage:** `~/.ycw/cache/signal_history.json`, overrideable via `YCW_HISTORY_FILE` env var.

**History entry schema:**
```json
{
  "inv_10y2y|warning|US": {
    "first_seen": "2026-03-02T08:00:00",
    "last_seen":  "2026-03-02T08:00:00",
    "active": true
  }
}
```

**Constructor:** `SignalHistory(ttl_days: int = 7, path: Path | None = None)`

**`filter_new(signals: List[Signal]) -> List[Signal]`**
Returns signals that should be notified. A signal passes if:
- Its key is not in history, OR
- Its key exists but `active=false` (previously cleared), OR
- Its key exists, `active=true`, and `now - first_seen >= ttl_days`.

**`update(current_signals: List[Signal]) -> None`**
- Updates `last_seen` for all currently active signals.
- Resets `first_seen` for TTL-triggered re-fires.
- Sets `active=false` for keys absent from the current run.
- Saves to disk. Write errors are logged as warnings, not raised.

Read errors (missing file, corrupt JSON) are treated as empty history — the pipeline
never crashes due to history issues.

### 3. `pipeline.py` changes

```python
history = SignalHistory(ttl_days=int(cfg.get("alert_ttl_days", 7)))

# inside economy loop, after sigs is built:
new_sigs = history.filter_new(sigs)
history.update(sigs)
notifier.notify(new_sigs)
```

Full `sigs` (including suppressed) still written to `results[econ]["signals"]`.

### 4. `config.example.yml`

Add optional field:
```yaml
alert_ttl_days: 7  # re-notify after this many days of continuous activity
```

## Files Changed

| File | Change |
|---|---|
| `src/ycw/types.py` | Add `economy: str` to `Signal` |
| `src/ycw/signal_history.py` | New — `SignalHistory` class |
| `src/ycw/pipeline.py` | Wire `SignalHistory` into run loop |
| `src/ycw/signals/composite.py` | Add `economy=economy` to all `Signal()` calls |
| `src/ycw/signals/logit_recession.py` | Add `economy=economy` to all `Signal()` calls |
| `examples/config.example.yml` | Add `alert_ttl_days: 7` |
| `tests/test_signal_history.py` | New — unit tests for `SignalHistory` |

## Tests

| Test | Verifies |
|---|---|
| First run | All signals pass `filter_new` |
| Duplicate run | Same `(code, level, economy)` is suppressed |
| Level change | `watch` → `warning` on same code fires again |
| Cleared and refired | Signal absent one run then returns → notifies |
| TTL expiry | Continuously active past `ttl_days` → re-notifies, `first_seen` resets |
| Corrupt JSON | Treats as empty history, no exception |
| Write failure | History still returned correctly |
| Economy isolation | Same `code+level` for US and UK both notify independently |
