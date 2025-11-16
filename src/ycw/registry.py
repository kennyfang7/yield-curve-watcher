class Registry:
    def __init__(self):
        self.fetchers: dict[str, type] = {}
        self.indicators: dict[str, type] = {}
        self.signals: dict[str, type] = {}
        self.notifiers: dict[str, type] = {}
        self.backtests: dict[str, type] = {}

    def register_fetcher(self, name: str, cls: type): self.fetchers[name] = cls
    def register_indicator(self, name: str, cls: type): self.indicators[name] = cls
    def register_signal(self, name: str, cls: type): self.signals[name] = cls
    def register_notifier(self, name: str, cls: type): self.notifiers[name] = cls
    def register_backtest(self, name: str, cls: type): self.backtests[name] = cls
