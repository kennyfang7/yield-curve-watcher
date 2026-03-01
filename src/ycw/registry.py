from .fetchers.base import BaseFetcher
from .indicators.base import BaseIndicator
from .signals.base import BaseSignal
from .notifiers.base import BaseNotifier


class Registry:
    def __init__(self):
        self.fetchers: dict[str, type] = {}
        self.indicators: dict[str, type] = {}
        self.signals: dict[str, type] = {}
        self.notifiers: dict[str, type] = {}
        self.backtests: dict[str, type] = {}

    def register_fetcher(self, name: str, cls: type) -> None:
        if not issubclass(cls, BaseFetcher):
            raise TypeError(f"{cls.__name__} must subclass BaseFetcher")
        self.fetchers[name] = cls

    def register_indicator(self, name: str, cls: type) -> None:
        if not issubclass(cls, BaseIndicator):
            raise TypeError(f"{cls.__name__} must subclass BaseIndicator")
        self.indicators[name] = cls

    def register_signal(self, name: str, cls: type) -> None:
        if not issubclass(cls, BaseSignal):
            raise TypeError(f"{cls.__name__} must subclass BaseSignal")
        self.signals[name] = cls

    def register_notifier(self, name: str, cls: type) -> None:
        if not issubclass(cls, BaseNotifier):
            raise TypeError(f"{cls.__name__} must subclass BaseNotifier")
        self.notifiers[name] = cls

    def register_backtest(self, name: str, cls: type) -> None:
        self.backtests[name] = cls
