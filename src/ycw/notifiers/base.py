from abc import ABC, abstractmethod
from typing import List
from ..types import Signal

class BaseNotifier(ABC):
    @abstractmethod
    def notify(self, signals: List[Signal]): ...
