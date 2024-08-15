from margin_trader.broker import SimBroker
from margin_trader.data_handlers import HistoricCSVDataHandler
from margin_trader.strategy import Strategy
from margin_trader.trader import Trader


class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    def on_market(self):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.
        """

        for s in self.symbols:
            if s not in self.broker.get_positions():
                bars = self.data_handler.get_latest_bars(s, N=1)
                if bars is not None and bars != []:
                    self.broker.buy(symbol=s)


if __name__ == "__main__":
    SYMBOLS = ["AAPL"]
    DATA_DIR = "./data/"

    data_handler = HistoricCSVDataHandler(csv_dir=DATA_DIR, symbols=SYMBOLS)
    sim_broker = SimBroker()
    strategy = BuyAndHoldStrategy(symbols=SYMBOLS)
    trader = Trader(data_handler=data_handler, broker=sim_broker, strategy=strategy)
    trader.run()
