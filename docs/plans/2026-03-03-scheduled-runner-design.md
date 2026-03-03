# Scheduled Runner Design

**Date:** 2026-03-03
**Feature:** Built-in daily scheduler so the pipeline runs automatically without an external cron job.

---

## Summary

Add a `schedule` CLI command that runs the pipeline on a daily timer configured in the YAML config file. Uses the `schedule` library with exponential backoff retry on failure.

---

## Config

New optional `schedule:` block in the YAML config:

```yaml
schedule:
  time: "08:00"           # 24hr local time to run daily (default: "08:00")
  retry_attempts: 3       # retries on failure (default: 3)
  retry_delay_seconds: 60 # base delay in seconds; doubles each attempt (default: 60)
```

All fields are optional. If the `schedule:` block is absent, defaults apply.

---

## Architecture & Components

### `src/ycw/scheduler.py` (new)

Contains `run_scheduled(reg, cfg)`:
- Reads `cfg.get("schedule", {})` for time, retry_attempts, retry_delay_seconds
- Defines `_run_with_retry()` inner function that calls `run_pipeline()` with exponential backoff on exception
- Registers `schedule.every().day.at("<time>").do(_run_with_retry)`
- Enters a blocking `while True: schedule.run_pending(); time.sleep(30)` loop
- Logs each run start, success, retry attempt, and final failure

### `src/ycw/cli.py` (modified)

- Add `"schedule"` to `argparse` choices
- If `args.command == "schedule"`, call `run_scheduled(reg, cfg)` instead of `run_pipeline()`

### `requirements.txt` (modified)

- Add `schedule>=1.2`

No other files change. `pipeline.py`, `registry.py`, and all plugins are untouched.

---

## Retry & Error Handling

Exponential backoff inside `_run_with_retry()`:

```
attempt 1 → fails → wait 60s
attempt 2 → fails → wait 120s
attempt 3 → fails → log "max retries exhausted, next run at <time> tomorrow"
```

Formula: `delay = retry_delay_seconds * (2 ** attempt_index)`

After exhausting retries, logs the error and returns. The scheduler stays alive and retries at the next scheduled time. Process never crashes.

Uses Python's standard `logging` module (already used throughout the project).

---

## Testing

Three test cases added to `tests/test_basic.py`:

1. **Happy path** — mock `run_pipeline` to succeed; verify `_run_with_retry()` calls it exactly once and does not raise
2. **Retry exhaustion** — mock `run_pipeline` to always raise; verify it retries `retry_attempts` times with exponential delays, then returns without crashing
3. **Config parsing defaults** — verify that `load_config` (or scheduler config parsing) correctly applies defaults when `schedule:` fields are absent: `time` → `"08:00"`, `retry_attempts` → `3`, `retry_delay_seconds` → `60`

FRED API is mocked in all tests — no real API calls required.
