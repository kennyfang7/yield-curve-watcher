# Scheduled Runner Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `schedule` CLI command that runs the pipeline on a daily timer, configured via the YAML config file, with exponential backoff retry on failure.

**Architecture:** A new `src/ycw/scheduler.py` module wraps the existing `run_pipeline()` with retry logic and a `schedule`-library daily loop. The CLI gains a `schedule` command that calls `run_scheduled()`. Nothing else changes.

**Tech Stack:** [`schedule`](https://schedule.readthedocs.io/) >= 1.2, Python `logging`, Python `time`

---

### Task 1: Add `schedule` dependency

**Files:**
- Modify: `requirements.txt`

**Step 1: Add the dependency**

Open `requirements.txt` and append:
```
schedule>=1.2
```

**Step 2: Install it**

```bash
pip install schedule>=1.2
```
Expected: installs without error.

**Step 3: Verify import works**

```bash
python -c "import schedule; print(schedule.__version__)"
```
Expected: prints a version string like `1.2.2`.

**Step 4: Commit**

```bash
git add requirements.txt
git commit -m "chore: add schedule dependency for built-in scheduler"
```

---

### Task 2: Write failing tests for scheduler

**Files:**
- Modify: `tests/test_basic.py`

Add a new test class at the bottom of `tests/test_basic.py`:

**Step 1: Add the test class**

```python
# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class TestScheduler:
    def test_run_with_retry_succeeds_on_first_attempt(self):
        """Happy path: pipeline succeeds, called exactly once."""
        from ycw.scheduler import _run_with_retry

        call_count = {"n": 0}

        def mock_pipeline():
            call_count["n"] += 1

        _run_with_retry(mock_pipeline, retry_attempts=3, retry_delay_seconds=0)
        assert call_count["n"] == 1

    def test_run_with_retry_exhausts_retries_without_crashing(self):
        """If pipeline always fails, retries retry_attempts times then returns."""
        from ycw.scheduler import _run_with_retry

        call_count = {"n": 0}

        def always_fails():
            call_count["n"] += 1
            raise RuntimeError("FRED is down")

        # Should not raise; should return after retry_attempts calls
        _run_with_retry(always_fails, retry_attempts=3, retry_delay_seconds=0)
        assert call_count["n"] == 3

    def test_schedule_config_defaults(self, tmp_path):
        """schedule: block absent → defaults applied by get_schedule_config()."""
        from ycw.scheduler import get_schedule_config

        # No schedule block at all
        cfg_no_schedule = {}
        s = get_schedule_config(cfg_no_schedule)
        assert s["time"] == "08:00"
        assert s["retry_attempts"] == 3
        assert s["retry_delay_seconds"] == 60

        # Partial schedule block — only time provided
        cfg_partial = {"schedule": {"time": "09:30"}}
        s2 = get_schedule_config(cfg_partial)
        assert s2["time"] == "09:30"
        assert s2["retry_attempts"] == 3
        assert s2["retry_delay_seconds"] == 60
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/test_basic.py::TestScheduler -v
```
Expected: `ImportError: cannot import name '_run_with_retry' from 'ycw.scheduler'` (module doesn't exist yet).

**Step 3: Commit the failing tests**

```bash
git add tests/test_basic.py
git commit -m "test: add failing tests for scheduler retry and config defaults"
```

---

### Task 3: Implement `src/ycw/scheduler.py`

**Files:**
- Create: `src/ycw/scheduler.py`

**Step 1: Create the module**

```python
import logging
import time
from typing import Any, Callable, Dict

import schedule

from .pipeline import run_pipeline

logger = logging.getLogger(__name__)

DEFAULTS = {
    "time": "08:00",
    "retry_attempts": 3,
    "retry_delay_seconds": 60,
}


def get_schedule_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return schedule config with defaults applied for missing keys."""
    raw = cfg.get("schedule", {}) or {}
    return {
        "time": raw.get("time", DEFAULTS["time"]),
        "retry_attempts": int(raw.get("retry_attempts", DEFAULTS["retry_attempts"])),
        "retry_delay_seconds": int(raw.get("retry_delay_seconds", DEFAULTS["retry_delay_seconds"])),
    }


def _run_with_retry(
    pipeline_fn: Callable,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 60,
) -> None:
    """Call pipeline_fn with exponential backoff retry. Never raises."""
    for attempt in range(retry_attempts):
        try:
            pipeline_fn()
            logger.info("Scheduled run completed successfully.")
            return
        except Exception as exc:
            if attempt < retry_attempts - 1:
                delay = retry_delay_seconds * (2 ** attempt)
                logger.warning(
                    "Run failed (attempt %d/%d): %s. Retrying in %ds.",
                    attempt + 1, retry_attempts, exc, delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "Run failed after %d attempts: %s. Will retry at next scheduled time.",
                    retry_attempts, exc,
                )


def run_scheduled(reg, cfg: Dict[str, Any]) -> None:
    """Start the blocking daily scheduler loop."""
    sched_cfg = get_schedule_config(cfg)
    run_time = sched_cfg["time"]
    retry_attempts = sched_cfg["retry_attempts"]
    retry_delay_seconds = sched_cfg["retry_delay_seconds"]

    def job():
        logger.info("Starting scheduled pipeline run.")
        _run_with_retry(
            lambda: run_pipeline(reg, cfg),
            retry_attempts=retry_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )

    schedule.every().day.at(run_time).do(job)
    logger.info("Scheduler started. Pipeline will run daily at %s.", run_time)

    while True:
        schedule.run_pending()
        time.sleep(30)
```

**Step 2: Run the tests to verify they pass**

```bash
pytest tests/test_basic.py::TestScheduler -v
```
Expected: all 3 tests PASS.

**Step 3: Commit**

```bash
git add src/ycw/scheduler.py
git commit -m "feat: implement scheduler module with retry and config defaults"
```

---

### Task 4: Wire `schedule` command into CLI

**Files:**
- Modify: `src/ycw/cli.py`

**Step 1: Update `cli.py`**

Change the `choices` in argparse and add the `schedule` branch. Here is the full updated `main()` function:

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "schedule"], help="run | schedule")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--json_out", default="", help="Write results JSON to this path (run only)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    reg = build_registry()

    if args.command == "schedule":
        from .scheduler import run_scheduled
        run_scheduled(reg, cfg)
    else:
        results = run_pipeline(reg, cfg)
        if args.json_out:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
```

**Step 2: Verify the CLI help shows the new command**

```bash
python -m ycw.cli --help
```
Expected output includes: `{run,schedule}`

**Step 3: Run the full test suite to make sure nothing is broken**

```bash
pytest tests/ -v
```
Expected: all tests PASS.

**Step 4: Commit**

```bash
git add src/ycw/cli.py
git commit -m "feat: add schedule CLI command wired to run_scheduled()"
```

---

### Task 5: Update example config

**Files:**
- Modify: `examples/config.example.yml`

**Step 1: Add the `schedule:` block**

Append to `examples/config.example.yml`:

```yaml
schedule:
  time: "08:00"           # 24hr local time to run daily
  retry_attempts: 3       # number of retry attempts on failure
  retry_delay_seconds: 60 # base delay in seconds; doubles each attempt
```

**Step 2: Commit**

```bash
git add examples/config.example.yml
git commit -m "docs: add schedule block to example config"
```

---

### Task 6: Final verification

**Step 1: Run the full test suite**

```bash
pytest tests/ -v
```
Expected: all tests PASS, no warnings about missing imports.

**Step 2: Smoke-test the CLI help**

```bash
python -m ycw.cli --help
python -m ycw.cli schedule --help
```
Expected: both show usage without errors.

**Step 3: Verify `run` command still works**

```bash
python -m ycw.cli run --config examples/config.example.yml
```
Expected: pipeline runs as before (requires `FRED_API_KEY`).
