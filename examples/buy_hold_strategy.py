from margin_trader.strategy import Strategy
from margin_trader.data_source import HistoricCSVDataHandler
from margin_trader.broker import SimBroker
from margin_trader.trader import Trader

class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the 
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """
    
    def calculate_signals(self, event):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are 
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object. 
        """
        if event.type == 'MARKET':
            for s in self.symbols:
                bars = self.data.get_latest_bars(s, N=1)
                if bars is not None and bars != []:
                    if s not in self.broker.get_positions():
                        self.broker.buy(s)


data_handler = HistoricCSVDataHandler(
    csv_dir="./data/",
    symbol_list=["AAPL", "MSFT"]
)
broker = SimBroker(
    data_handler=data_handler,
    commission=0.0
)
trader = Trader(
    symbols=["AAPL", "MSFT"],
    data_handler=data_handler,
    broker=broker,
    strategy=BuyAndHoldStrategy
)
result = trader._run_backtest()
print(result)
trader.plot()