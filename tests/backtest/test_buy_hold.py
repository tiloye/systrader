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
        sim_broker = SimBroker(data_handler=data_handler, commission=0.0)
        strategy = BuyAndHoldStrategy(
            symbols=SYMBOLS, data=data_handler, broker=sim_broker
        )
        trader = Trader(data_handler=data_handler, broker=sim_broker, strategy=strategy)
        trader.run()

        with self.subTest("attributes"):
            self.assertTrue(hasattr(trader, "events"))
            self.assertTrue(hasattr(trader, "broker"))
            self.assertTrue(hasattr(trader, "data_handler"))
            self.assertTrue(hasattr(trader, "strategy"))

        with self.subTest("order_position_history"):
            pos_history = trader.account_history["positions"]
            order_history = trader.account_history["orders"]

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


if __name__ == "__main__":
    unittest.main()
