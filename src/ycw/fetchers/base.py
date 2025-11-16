from abc import ABC, abstractmethod
from datetime import date
from ..types import CurveFetcherResult

class BaseFetcher(ABC):
    economy: str
    @abstractmethod
    def fetch(self, start: date, end: date) -> CurveFetcherResult: ...
