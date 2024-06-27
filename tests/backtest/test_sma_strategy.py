import csv
import os
import unittest
from pathlib import Path

import pandas as pd

from examples.sma_strategy import (
    HistoricCSVDataHandler,
    SimBroker,
    SMAStrategy,
    Trader,
)

CSV_DIR = Path(__file__).parent
SYMBOLS = ["SYMBOL1"]


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

        with open(CSV_DIR / "SYMBOL1.csv", "w") as csvfile:
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
                "symbol": ["SYMBOL1"] * 4,
                "units": [100] * 4,
                "open_price": [104.0, 109.0, 105.0, 107.0],
                "close_price": [109.0, 105.0, 107.0, 108.0],
                "commission": [0.0] * 4,
                "side": ["SELL", "BUY", "SELL", "BUY"],
                "open_time": [
                    pd.to_datetime("2024-05-04"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                ],
                "pnl": [-500.0, -400.0, -200, 100.0],
                "id": [1, 2, 3, 4],
                "close_time": [
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-10"),
                ],
            },
        )
        expected_order_history = pd.DataFrame(
            data={
                "type": ["ORDER"] * 8,
                "timeindex": [
                    pd.to_datetime("2024-05-04"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-10"),
                ],
                "symbol": ["SYMBOL1"] * 8,
                "order_type": ["MKT"] * 8,
                "units": [100] * 8,
                "side": ["SELL", "BUY", "BUY", "SELL", "SELL", "BUY", "BUY", "SELL"],
                "status": ["EXECUTED"] * 8,
                "id": list(range(1, 9)),
                "pos_id": [1, 1, 2, 2, 3, 3, 4, 4],
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
