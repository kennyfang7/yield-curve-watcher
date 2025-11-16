import os, json, requests
from typing import List
from ..types import Signal

class SlackWebhookNotifier:
    def __init__(self, webhook_url: str | None = None):
        self.url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        if not self.url:
            raise RuntimeError("Slack webhook URL missing")
    def notify(self, signals: List[Signal]):
        if not signals: return
        blocks = [{"type":"section","text":{"type":"mrkdwn","text":f"*{s.level.upper()}* — {s.message}"}} for s in signals]
        payload = {"blocks": blocks}
        requests.post(self.url, data=json.dumps(payload), headers={"Content-Type":"application/json"}, timeout=10)
