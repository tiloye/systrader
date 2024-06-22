import queue

import matplotlib.pyplot as plt
import pandas as pd

import margin_trader.performance.metric as metrics
from margin_trader.broker import SimBroker
from margin_trader.data_source import BacktestDataHandler


class Trader:
    def __init__(self, data_handler, broker, strategy):
        self.events = queue.Queue()
        self.data_handler = data_handler
        self.data_handler._add_event_queue(self.events)
        self.broker = broker
        self.broker._add_event_queue(self.events)
        self.strategy = strategy
        self.strategy._add_event_queue(self.events)

    def _handle_events(self) -> None:
        # Handle the events
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

        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            self.data_handler.update_bars()
            if self.data_handler.continue_backtest:
                pass
            else:
                # Close all open positions
                self.broker.close_all_positions()
                self._handle_events()

                # Get backtest result
                self.account_history = self.broker.get_account_history()
                self.balance_equity = self.account_history["balance_equity"]
                self.position_history = self.account_history["positions"]
                self.equity_rets = self.balance_equity.equity.pct_change().fillna(0)
                self.backtest_result = self._backtest_stats()
                break
            self._handle_events()

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

    def _backtest_stats(self) -> pd.DataFrame:
        """Return strategy performance metrics from backtest."""
        ret_stats = metrics.returns_stats(self.equity_rets)
        return ret_stats

    def plot(self):
        self.balance_equity.equity.plot(
            figsize=(16, 8), title="Daily Portfolio Value", ylabel="Value [$]"
        )
        plt.show()
