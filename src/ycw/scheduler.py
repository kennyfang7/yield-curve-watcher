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
