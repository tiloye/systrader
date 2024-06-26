import csv
import os
import unittest
from pathlib import Path

from examples.buy_hold_strategy import (
    BuyAndHoldStrategy,
    HistoricCSVDataHandler,
    SimBroker,
    Trader,
)

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
        cls.trader.run()

    def test_init(self):
        self.assertTrue(hasattr(self.trader, "events"))
        self.assertTrue(hasattr(self.trader, "broker"))
        self.assertTrue(hasattr(self.trader, "data_handler"))
        self.assertTrue(hasattr(self.trader, "strategy"))

    def test_positions(self):
        account_history = self.trader.account_history
        pos_history = account_history["positions"]

        self.assertEqual(len(pos_history), 1)
        self.assertEqual(pos_history.iloc[0].side, "BUY")
        self.assertEqual(
            pos_history.iloc[0].open_time.strftime("%Y-%m-%d"), "2024-05-03"
        )
        self.assertEqual(
            pos_history.iloc[0].close_time.strftime("%Y-%m-%d"), "2024-05-07"
        )
        self.assertEqual(pos_history.iloc[0].pnl, 1000.0)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR / "AAPL.csv"):
            os.remove(CSV_DIR / "AAPL.csv")


if __name__ == "__main__":
    unittest.main()
