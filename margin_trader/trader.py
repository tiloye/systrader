from margin_trader.broker import SimBroker
from margin_trader.data_handlers import BacktestDataHandler
from margin_trader.event import FILLEVENT, MARKETEVENT, ORDEREVENT, EventManager


class Trader:
    def __init__(self, data_handler, broker, strategy):
        self.data_event_manager = EventManager()
        self.broker_event_manager = EventManager()

        self.data_handler = data_handler
        self.data_handler.add_event_manager(self.data_event_manager)

        self.broker = broker
        self.broker.add_event_manager(self.broker_event_manager)
        self.broker.add_data_handler(self.data_handler)

        self.strategy = strategy
        self.strategy.add_data_handler(self.data_handler)
        self.strategy.add_broker(self.broker)

        self.data_handler.event_manager.subscribe(MARKETEVENT, broker)
        self.data_handler.event_manager.subscribe(MARKETEVENT, self.strategy)

        self.broker.event_manager.subscribe(ORDEREVENT, self.strategy)
        self.broker.event_manager.subscribe(FILLEVENT, self.strategy)

    def _run_backtest(self):
        """Execute the strategy in an event loop."""

        print("Starting backtest")
        while True:
            self.data_handler.update_bars()
            if not self.data_handler.continue_backtest:
                self.broker.close_all_positions()
                # self._handle_events()
                self.account_history = self.broker.get_account_history()
                break
            # self._handle_events()
        print("Finished running backtest")

    def _run_live(self, **kwargs):
        """Should implement live trading logic."""
        pass

    def run(self, **kwargs):
        if self._is_backtest():
            self._run_backtest()
        else:
            self._run_live(**kwargs)

    def _is_backtest(self):
        if isinstance(self.data_handler, BacktestDataHandler) and isinstance(
            self.broker, SimBroker
        ):
            return True
