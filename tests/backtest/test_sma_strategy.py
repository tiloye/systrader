import csv
import os
import unittest
from pathlib import Path

import pandas as pd

from examples.sma_strategy import HistoricCSVDataHandler, SimBroker, SMAStrategy, Trader

CSV_DIR = Path(__file__).parent
SYMBOLS = ["SMA_SYMBOL1"]


class TestTraderBacktest(unittest.TestCase):
    def setUp(self):
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

        with open(CSV_DIR / "SMA_SYMBOL1.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            )
            for day in data:
                writer.writerow(day)

        self.data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        self.sim_broker = SimBroker()
        self.strategy = SMAStrategy(symbols=SYMBOLS)
        self.trader = Trader(
            data_handler=self.data_handler,
            broker=self.sim_broker,
            strategy=self.strategy,
        )
        self.trader.run()

    def tearDown(self):
        if os.path.exists(CSV_DIR / "SMA_SYMBOL1.csv"):
            os.remove(CSV_DIR / "SMA_SYMBOL1.csv")

    def test_position_and_order_history(self):
        pos_history = self.trader.account_history["positions"]
        order_history = self.trader.account_history["orders"]

        expected_pos_history = pd.DataFrame(
            data={
                "symbol": ["SMA_SYMBOL1"] * 4,
                "side": ["sell", "buy", "sell", "buy"],
                "units": [100] * 4,
                "open_price": [104.0, 109.0, 105.0, 107.0],
                "close_price": [109.0, 105.0, 107.0, 108.0],
                "commission": [0.0] * 4,
                "pnl": [-500.0, -400.0, -200, 100.0],
                "open_time": [
                    pd.to_datetime("2024-05-04"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                ],
                "close_time": [
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-10"),
                ],
                "id": [1, 3, 5, 7],
            },
        )
        expected_order_history = pd.DataFrame(
            data={
                "timestamp": [
                    pd.to_datetime("2024-05-04"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-06"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-07"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-09"),
                    pd.to_datetime("2024-05-10"),
                ],
                "symbol": ["SMA_SYMBOL1"] * 8,
                "order_type": ["mkt"] * 8,
                "units": [100] * 8,
                "side": ["sell", "buy", "buy", "sell", "sell", "buy", "buy", "sell"],
                "price": [None] * 8,
                "sl": [None] * 8,
                "tp": [None] * 8,
                "status": ["executed"] * 8,
                "order_id": list(range(1, 9)),
                "position_id": [1, 1, 3, 3, 5, 5, 7, 7],
                "request": [
                    "open",
                    "close",
                    "open",
                    "close",
                    "open",
                    "close",
                    "open",
                    "close",
                ],
            }
        )

        with self.subTest("Position History"):
            pd.testing.assert_frame_equal(pos_history, expected_pos_history)

        with self.subTest("Order History"):
            pd.testing.assert_frame_equal(order_history, expected_order_history)


if __name__ == "__main__":
    unittest.main()
