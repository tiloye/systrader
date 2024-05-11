from queue import Queue
from margin_trader.performance import create_sharpe_ratio, create_drawdowns

class Trader:
    
    def __init__(self, data_handler, broker, strategy, **kwargs):
        self.data_handler = data_handler
        self.broker = broker
        self.strategy = strategy
        self.events = Queue()

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
                except Queue.Empty:
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

    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        account_history = self.broker.get_account_history()
        balance_equity = account_history["balance_equity"]
        equity_rets = balance_equity.equity.pct_change().fillna(0)
        equity_cum_rets = equity_rets.add(1).cumprod()
        sr = create_sharpe_ratio(equity_rets)
        max_dd, period = create_drawdowns(equity_cum_rets)
        print(
            f"Sharpe ratio: {sr}",
            f"Max drawdown: {max_dd}"
            f"Longest drawdown period: {period}"
        )

        