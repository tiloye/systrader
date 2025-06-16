import unittest
from pathlib import Path

import pandas as pd

from examples.buy_hold_strategy import (
    BuyAndHoldStrategy,
    HistoricCSVDataHandler,
    SimBroker,
    Trader,
)


class TestTraderBacktest(unittest.TestCase):
    def test_endtoend_buy_hold(self):
        CSV_DIR = Path(__file__).parent.parent.joinpath("data")
        SYMBOLS = ["SYMBOL1"]
        data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        sim_broker = SimBroker()
        strategy = BuyAndHoldStrategy(symbols=SYMBOLS)
        trader = Trader(data_handler=data_handler, broker=sim_broker, strategy=strategy)
        trader.run()

        pos_history = trader.account_history["positions"]
        order_history = trader.account_history["orders"]

        expected_pos_history = pd.DataFrame(
            data={
                "symbol": "SYMBOL1",
                "side": "buy",
                "units": 100,
                "open_price": 102.0,
                "close_price": 112.0,
                "commission": 0.0,
                "pnl": 1000.0,
                "open_time": pd.to_datetime("2024-05-03"),
                "close_time": pd.to_datetime("2024-05-07"),
                "id": 1,
            },
            index=[0],
        )
        expected_order_history = pd.DataFrame(
            data={
                "timestamp": [
                    pd.to_datetime("2024-05-03"),
                    pd.to_datetime("2024-05-07"),
                ],
                "symbol": ["SYMBOL1"] * 2,
                "order_type": ["mkt"] * 2,
                "units": [100] * 2,
                "side": ["buy", "sell"],
                "price": [None, None],
                "sl": [None, None],
                "tp": [None, None],
                "status": ["executed"] * 2,
                "order_id": [1, 2],
                "position_id": [1, 1],
                "request": ["open", "close"],
            }
        )

        with self.subTest("Position History"):
            pd.testing.assert_frame_equal(pos_history, expected_pos_history)

        with self.subTest("Order History"):
            pd.testing.assert_frame_equal(order_history, expected_order_history)


if __name__ == "__main__":
    unittest.main()
