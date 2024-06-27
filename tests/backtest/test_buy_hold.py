import csv
import os
import unittest
from pathlib import Path

import pandas as pd

from examples.buy_hold_strategy import (
    BuyAndHoldStrategy,
    HistoricCSVDataHandler,
    SimBroker,
    Trader,
)

CSV_DIR = Path(__file__).parent
SYMBOLS = ["SYMBOL1"]


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

        with open(CSV_DIR / "SYMBOL1.csv", "w") as csvfile:
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

    def test_position_and_order_history(self):
        pos_history = self.trader.account_history["positions"]
        order_history = self.trader.account_history["orders"]

        expected_pos_history = pd.DataFrame(
            data={
                "symbol": "SYMBOL1",
                "units": 100,
                "open_price": 102.0,
                "close_price": 112.0,
                "commission": 0.0,
                "side": "BUY",
                "open_time": pd.to_datetime("2024-05-03"),
                "pnl": 1000.0,
                "id": 1,
                "close_time": pd.to_datetime("2024-05-07"),
            },
            index=[0],
        )
        expected_order_history = pd.DataFrame(
            data={
                "type": ["ORDER"] * 2,
                "timeindex": [
                    pd.to_datetime("2024-05-03"),
                    pd.to_datetime("2024-05-07"),
                ],
                "symbol": ["SYMBOL1"] * 2,
                "order_type": ["MKT"] * 2,
                "units": [100] * 2,
                "side": ["BUY", "SELL"],
                "status": ["EXECUTED"] * 2,
                "id": [1, 2],
                "pos_id": [1] * 2,
            }
        )

        pd.testing.assert_frame_equal(pos_history, expected_pos_history)
        pd.testing.assert_frame_equal(order_history, expected_order_history)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR / "SYMBOL1.csv"):
            os.remove(CSV_DIR / "SYMBOL1.csv")


if __name__ == "__main__":
    unittest.main()
