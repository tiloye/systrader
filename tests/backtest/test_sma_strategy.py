import unittest
import csv
import os
import pandas as pd
from pathlib import Path
from examples.sma_strategy import *

CSV_DIR = Path(__file__).parent
SYMBOLS = ["AAPL"]


class TestTraderBacktest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data = [
            ["2024-05-01", 100.0, 105.0, 98.0, 102.0],
            ["2024-05-02", 102.0, 108.0, 100.0, 106.0],
            ["2024-05-03", 106.0, 110.0, 104.0, 108.0],  # SMA = 105.33 -> Hold
            ["2024-05-04", 108.0, 112.0, 106.0, 104.0],  # SMA = 106.0 -> Sell
            ["2024-05-05", 104.0, 108.0, 102.0, 106.0],  # SMA = 106.0 -> Hold
            ["2024-05-06", 106.0, 110.0, 104.0, 109.0],  # SMA = 106.33 -> Buy
            ["2024-05-07", 109.0, 113.0, 107.0, 105.0],  # SMA = 106.67 -> Sell
            ["2024-05-08", 105.0, 109.0, 103.0, 103.0],  # SMA = 106.0 -> Hold
            ["2024-05-09", 103.0, 107.0, 101.0, 107.0],  # SMA = 105.0 -> Buy
            ["2024-05-10", 107.0, 111.0, 105.0, 108.0],  # SMA = 106.67 -> Hold
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
        cls.strategy = SMAStrategy(
            symbols=SYMBOLS, data=cls.data_handler, broker=cls.sim_broker
        )
        cls.trader = Trader(
            data_handler=cls.data_handler, broker=cls.sim_broker, strategy=cls.strategy
        )
        cls.trader.run()
        cls.result = cls.trader.backtest_result

    def test_init(self):
        self.assertTrue(hasattr(self.trader, "events"))
        self.assertTrue(hasattr(self.trader, "broker"))
        self.assertTrue(hasattr(self.trader, "data_handler"))
        self.assertTrue(hasattr(self.trader, "strategy"))

    def test_positions(self):
        account_history = self.trader.account_history
        pos_history = account_history["positions"]
        # open_position = self.trader.broker.get_position("AAPL")

        self.assertEqual(len(pos_history), 4)
        self.assertEqual(pos_history.iloc[0].side, "SELL")
        self.assertEqual(
            pos_history.iloc[0].open_time.strftime("%Y-%m-%d"), "2024-05-04"
        )
        self.assertEqual(
            pos_history.iloc[0].close_time.strftime("%Y-%m-%d"), "2024-05-06"
        )
        self.assertEqual(pos_history.pnl.sum(), -1000.0)
        # self.assertEqual(open_position.pnl, 100)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR / "AAPL.csv"):
            os.remove(CSV_DIR / "AAPL.csv")


if __name__ == "__main__":
    unittest.main()
