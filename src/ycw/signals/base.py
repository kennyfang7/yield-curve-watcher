from abc import ABC, abstractmethod
from typing import List, Dict
from ..types import Signal

class BaseSignal(ABC):
    name: str
    @abstractmethod
    def evaluate(self, economy: str, all_features: Dict[str, float]) -> List[Signal]: ...
