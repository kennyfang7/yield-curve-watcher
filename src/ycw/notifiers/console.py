from typing import List
from ..types import Signal

class ConsoleNotifier:
    def notify(self, signals: List[Signal]):
        for s in signals:
            tag = {"info":"[i]", "watch":"[!]", "warning":"[!!]"}[s.level]
            print(tag, s.message)
