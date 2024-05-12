import queue
from margin_trader.performance import create_sharpe_ratio, create_drawdowns

class Trader:
    
    def __init__(self, symbols, data_handler, broker, strategy):
        self.symbols = symbols
        self.events = queue.Queue()
        self.data_handler = data_handler
        self.data_handler._add_event_queue(self.events)
        self.broker = broker
        self.broker._add_event_queue(self.events)
        self.strategy = strategy(
            symbols = self.symbols,
            data = self.data_handler,
            broker = self.broker
        )
        self.strategy._add_event_queue(self.events)

    def _run_backtest(self):
        """Execute the strategy in an event loop."""

        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            if self.data_handler.continue_backtest == True:
                self.data_handler.update_bars()
            else:
                break
            
            # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.broker.update_account(event)
                        elif event.type == 'ORDER':
                            self.broker.execute_order(event)
                        elif event.type == 'FILL':
                            self.broker.update_account(event)
        result = self._output_performance()
        return result

    def _output_performance(self):
        """Output the strategy performance from the backtest."""

        account_history = self.broker.get_account_history()
        balance_equity = account_history["balance_equity"]
        equity_rets = balance_equity.equity.pct_change().fillna(0)
        equity_cum_rets = equity_rets.add(1).cumprod()
        sr = create_sharpe_ratio(equity_rets)
        max_dd, period = create_drawdowns(equity_cum_rets)
        return {
            "Sharpe ratio": sr,
            "Maximum drawdown": max_dd,
            "Longest drawdown period": period
        }

        