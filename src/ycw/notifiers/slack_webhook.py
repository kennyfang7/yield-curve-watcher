import json
import os
import requests
from typing import List
from ..types import Signal
from .base import BaseNotifier


class SlackWebhookNotifier(BaseNotifier):
    def __init__(self, webhook_url: str | None = None):
        self.url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        if not self.url:
            raise RuntimeError("Slack webhook URL missing")

    def notify(self, signals: List[Signal]) -> None:
        if not signals:
            return
        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{s.level.upper()}* — {s.message}"},
            }
            for s in signals
        ]
        r = requests.post(
            self.url,
            data=json.dumps({"blocks": blocks}),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
