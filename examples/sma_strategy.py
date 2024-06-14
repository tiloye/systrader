from margin_trader.broker import SimBroker
from margin_trader.data_source import HistoricCSVDataHandler
from margin_trader.strategy import Strategy
from margin_trader.trader import Trader


class SMAStrategy(Strategy):
    window = 3

    def calculate_signals(self, event):
        if event.type == "MARKET":
            for s in self.symbols:
                bars = self.data.get_latest_bars(s, N=self.window)
                if bars is not None and len(bars) == self.window:
                    close_prices = [bar.close for bar in bars]
                    signal = self._calculate_signal(close_prices)
                    position = self.broker.get_position(s)
                    if position:
                        if signal is not None and position.side != signal:
                            self.broker.close(s)
                            if signal == "BUY":
                                self.broker.buy(s)
                            else:
                                self.broker.sell(s)
                    else:
                        if signal is not None:
                            if signal == "BUY":
                                self.broker.buy(s)
                            else:
                                self.broker.sell(s)

    def _calculate_signal(self, prices: list[float]) -> str | None:
        ma = sum(prices) / self.window
        latest_close = prices[-1]
        prev_close = prices[-2]
        if latest_close > ma and prev_close <= ma:
            return "BUY"
        elif latest_close < ma and prev_close >= ma:
            return "SELL"
        else:
            return None


if __name__ == "__main__":
    SYMBOLS = ["AAPL"]
    DATA_DIR = "./data/"

    data_handler = HistoricCSVDataHandler(csv_dir=DATA_DIR, symbols=SYMBOLS)
    sim_broker = SimBroker(data_handler=data_handler, commission=0.0)
    strategy = SMAStrategy(symbols=SYMBOLS, data=data_handler, broker=sim_broker)
    trader = Trader(data_handler=data_handler, broker=sim_broker, strategy=strategy)
    trader.run()

    result = trader.backtest_result
    trader.plot()
