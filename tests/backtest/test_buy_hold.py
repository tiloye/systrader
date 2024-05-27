import unittest
import csv
import os
import pandas as pd
from pathlib import Path
from examples.buy_hold_strategy import *

CSV_DIR = Path(__file__).parent
SYMBOLS = ["AAPL"]


class TestTraderBacktest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data = [
            ["2024-05-03", 100.0, 105.0, 98.0, 102.0, 102.0, 0],
            ["2024-05-04", 102.0, 108.0, 100.0, 106.0, 106.0, 0],
            ["2024-05-05", 106.0, 110.0, 104.0, 108.0, 108.0, 0],
            ["2024-05-06", 108.0, 112.0, 106.0, 110.0, 110.0, 0],
            ["2024-05-07", 110.0, 115.0, 108.0, 112.0, 112.0, 0],
        ]

        with open(CSV_DIR / "AAPL.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            )
            for day in data:
                writer.writerow(day)

        cls.data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        cls.sim_broker = SimBroker(data_handler=cls.data_handler, commission=0.0)
        cls.strategy = BuyAndHoldStrategy(
            symbols=SYMBOLS, data=cls.data_handler, broker=cls.sim_broker
        )
        cls.trader = Trader(
            data_handler=cls.data_handler, broker=cls.sim_broker, strategy=cls.strategy
        )
        cls.result = cls.trader._run_backtest()

    def test_init(self):
        self.assertTrue(hasattr(self.trader, "events"))
        self.assertTrue(hasattr(self.trader, "broker"))
        self.assertTrue(hasattr(self.trader, "data_handler"))
        self.assertTrue(hasattr(self.trader, "strategy"))

    def test_equity(self):
        data = pd.read_csv(CSV_DIR / "AAPL.csv")
        rets = data.Close.pct_change().fillna(0.0)
        cum_rets = rets.add(1).cumprod().sub(1)
        starting_position_value = 10200.0
        equity = cum_rets.mul(starting_position_value).add(self.sim_broker.balance)
        equity_ret = equity.pct_change().fillna(0.0)
        total_ret = (equity_ret.add(1).prod() - 1) * 100
        ann_ret = (equity_ret.add(1).prod() ** (252 / len(equity_ret)) - 1) * 100

        trader_equity = self.trader.balance_equity.equity
        trader_total_ret = self.result.loc["Total Return"]
        trader_ann_ret = self.result.loc["Annual Return"]
        self.assertListEqual(trader_equity.to_list(), equity.to_list())
        self.assertAlmostEqual(trader_total_ret, total_ret, 2)
        self.assertAlmostEqual(trader_ann_ret, ann_ret, 2)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR / "AAPL.csv"):
            os.remove(CSV_DIR / "AAPL.csv")


if __name__ == "__main__":
    unittest.main()
