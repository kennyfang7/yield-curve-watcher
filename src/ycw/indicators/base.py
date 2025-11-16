from abc import ABC, abstractmethod
import pandas as pd
from ..types import IndicatorResult

class BaseIndicator(ABC):
    name: str
    @abstractmethod
    def compute(self, economy: str, df: pd.DataFrame) -> IndicatorResult: ...
