import queue

from margin_trader.broker import SimBroker
from margin_trader.data_source import BacktestDataHandler


class Trader:
    def __init__(self, data_handler, broker, strategy):
        self.events = queue.Queue()
        self.data_handler = data_handler
        self.data_handler.add_event_queue(self.events)
        self.broker = broker
        self.broker.add_event_queue(self.events)
        self.strategy = strategy
        self.strategy.add_event_queue(self.events)

    def _handle_events(self) -> None:
        while True:
            try:
                event = self.events.get(False)
            except queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == "MARKET":
                        self.broker.check_pending_orders()
                        self.strategy.calculate_signals(event)
                        self.broker.update_account(event)
                    elif event.type == "ORDER":
                        self.broker.execute_order(event)
                    elif event.type == "FILL":
                        self.broker.update_account(event)

    def _run_backtest(self):
        """Execute the strategy in an event loop."""

        print("Starting backtest")
        while True:
            self.data_handler.update_bars()
            if self.data_handler.continue_backtest:
                pass
            else:
                self.broker.close_all_positions()
                self._handle_events()
                self.account_history = self.broker.get_account_history()
                break
            self._handle_events()
        print("Finished running backtest")

    def _run_live(self, **kwargs):
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
