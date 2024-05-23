import queue
import margin_trader.performance as perf
import matplotlib.pyplot as plt
import pandas as pd


class Trader:
    
    def __init__(self, data_handler, broker, strategy):
        self.events = queue.Queue()
        self.data_handler = data_handler
        self.data_handler._add_event_queue(self.events)
        self.broker = broker
        self.broker._add_event_queue(self.events)
        self.strategy = strategy
        self.strategy._add_event_queue(self.events)

    def _run_backtest(self):
        """Execute the strategy in an event loop."""

        while True:
            # Update the bars (specific backtest code, as opposed to live trading)
            self.data_handler.update_bars()
            if self.data_handler.continue_backtest == True:
                pass
            else:
                self.account_history = self.broker.get_account_history()
                self.balance_equity = self.account_history["balance_equity"]
                self.equity_rets = self.balance_equity.equity.pct_change().fillna(0)
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

    def _output_performance(self) -> pd.Series:
        """Output the strategy performance from the backtest."""
        
        perf_measures = {
            "Total Return": perf.calculate_total_return(self.equity_rets)*100,
            "Annual Return": perf.calculate_annual_return(self.equity_rets)*100,
            "Volatiliy": perf.calculate_annual_volatility(self.equity_rets, periods=1)*100,
            "Annual Volatility": perf.calculate_annual_volatility(self.equity_rets)*100,
            "Sharpe ratio": perf.calculate_sharpe_ratio(self.equity_rets),
            "Maximum drawdown": perf.calculate_max_drawdown(self.equity_rets)*100,
            "VaR": perf.calculate_var(self.equity_rets)*100,
            "Longest drawdown period": perf.calculate_longest_dd_period(
                self.equity_rets)
        }
        perf_measures = pd.Series(perf_measures)
        return perf_measures
    
    def plot(self):
        self.balance_equity.equity.plot(figsize=(16, 8))
        plt.show()
        