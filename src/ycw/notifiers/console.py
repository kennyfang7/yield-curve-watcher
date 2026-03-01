from typing import List
from ..types import Signal
from .base import BaseNotifier

_LEVEL_TAGS = {"info": "[i]", "watch": "[!]", "warning": "[!!]"}


class ConsoleNotifier(BaseNotifier):
    def notify(self, signals: List[Signal]) -> None:
        for s in signals:
            tag = _LEVEL_TAGS.get(s.level, f"[{s.level}]")
            print(tag, s.message)
