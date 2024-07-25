import unittest
from pathlib import Path
from queue import Queue

import pandas as pd

from margin_trader.broker.sim_broker import NetPositionManager, SimBroker
from margin_trader.data_handlers import HistoricCSVDataHandler

CSV_DIR = Path(__file__).parent.parent.joinpath("data")
SYMBOLS = ["SYMBOL1"]


class TestSimBroker(unittest.TestCase):
    def setUp(self):
        self.event_queue = Queue()
        self.data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        self.data_handler.add_event_queue(self.event_queue)

        self.broker = SimBroker(
            balance=100_000.0, data_handler=self.data_handler, commission=0.5
        )
        self.broker.add_event_queue(self.event_queue)
        self.data_handler.update_bars()  # Add market event to the event queue

    def run_buy_sell_workflow(
        self,
        symbol="SYMBOL1",
        side="buy",
        units=100,
        exec_price="current",
        return_events=False,
    ):
        if exec_price == "current":
            if side == "buy":
                self.broker.buy(symbol, units=units)
                order_event = self.event_queue.get(False)
                fill_event = self.event_queue.get(False)
            else:
                self.broker.sell(symbol, units=units)
                order_event = self.event_queue.get(False)
                fill_event = self.event_queue.get(False)
        else:
            if side == "buy":
                self.broker.buy(symbol, units=units)
                self.broker.execute_pending_orders()
                order_event = self.event_queue.get(False)
                fill_event = self.event_queue.get(False)
            else:
                self.broker.buy(symbol, units=units)
                self.broker.execute_pending_orders()
                order_event = self.event_queue.get(False)
                fill_event = self.event_queue.get(False)

        if return_events:
            return order_event, fill_event

    def run_close_workflow(self, symbol="SYMBOL1", return_events=None):
        position = self.broker.get_position(symbol)
        self.broker.close(position, position.units)  # type: ignore
        order_event = self.event_queue.get(False)
        fill_event = self.event_queue.get(False)
        if return_events:
            return order_event, fill_event

    def run_bar_update_workflow(self):
        self.data_handler.update_bars()
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)

    def test_init(self):
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 100_000.0)
        self.assertIsInstance(self.broker.data_handler, HistoricCSVDataHandler)
        self.assertEqual(self.broker.events, self.event_queue)
        self.assertEqual(self.broker.leverage, 1)
        self.assertEqual(self.broker.commission, 0.5)
        self.assertIsInstance(self.broker.p_manager, NetPositionManager)
        self.assertEqual(self.broker._exec_price, "current")
        self.assertIsInstance(self.broker.pending_orders, Queue)
        self.assertListEqual(self.broker.account_history, [])

    def test_get_used_margin_no_positions(self):
        used_margin = self.broker.get_used_margin()
        self.assertEqual(used_margin, 0.0)

    def test_get_used_margin_open_positions(self):
        _ = self.event_queue.get(False)
        self.run_buy_sell_workflow()
        used_margin = self.broker.get_used_margin()

        self.assertEqual(used_margin, 10_200.0)

    def test_get_positions(self):
        self.assertDictEqual(self.broker.get_positions(), {})

    def test_get_position_history(self):
        self.assertListEqual(self.broker.get_positions_history(), [])

    def test_buy(self):
        _ = self.event_queue.get(False)  # Generate signal from market event
        order_event, fill_event = self.run_buy_sell_workflow(return_events=True)

        self.assertEqual(order_event.symbol, fill_event.symbol)
        self.assertEqual(order_event.order_id, fill_event.order_id)
        self.assertEqual(order_event.position_id, fill_event.position_id)
        self.assertEqual(order_event.side, fill_event.side)
        self.assertEqual(order_event.units, fill_event.units)
        self.assertIn(fill_event.symbol, self.broker.get_positions())

    def test_sell(self):
        _ = self.event_queue.get(False)
        order_event, fill_event = self.run_buy_sell_workflow(
            side="sell", return_events=True
        )

        self.assertEqual(order_event.symbol, fill_event.symbol)
        self.assertEqual(order_event.order_id, fill_event.order_id)
        self.assertEqual(order_event.position_id, fill_event.position_id)
        self.assertEqual(order_event.side, fill_event.side)
        self.assertEqual(order_event.units, fill_event.units)
        self.assertIn(fill_event.symbol, self.broker.get_positions())

    def test_reverse_order_not_rejected(self):
        # Reset broker balance
        self.broker.balance = 12000
        self.broker.equity = 12000
        self.broker.free_margin = 12000

        # Populate acct balance history
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)

        self.run_buy_sell_workflow()  # Price: 102.0, Cost: 10,200
        with self.subTest("Executed first order"):
            order = self.broker.order_history[-1]
            self.assertEqual(order.units, 100)
            self.assertEqual(order.status, "EXECUTED")
            self.assertEqual(order.order_id, 1)
            self.assertEqual(order.position_id, 1)

        self.run_buy_sell_workflow(units=200)
        with self.subTest("Executed reverse order"):
            order1 = self.broker.order_history[-2]
            order2 = self.broker.order_history[-1]

            self.assertEqual(order1.units, 100)
            self.assertEqual(order1.status, "EXECUTED")
            self.assertEqual(order1.order_id, 2)
            self.assertEqual(order1.position_id, 1)
            self.assertEqual(order2.units, 100)
            self.assertEqual(order2.status, "EXECUTED")
            self.assertEqual(order2.order_id, 2)
            self.assertEqual(order2.position_id, 2)

    def test_execute_pending_orders(self):
        self.broker._exec_price = "next"
        _ = self.event_queue.get(False)
        order_event, fill_event = self.run_buy_sell_workflow(
            exec_price="next", return_events=True
        )
        self.assertEqual(order_event.status, "EXECUTED")
        self.assertEqual(order_event.order_type, "MKT")
        self.assertEqual(fill_event.fill_price, 100.0)

    def test_execute_order_same_bar_close(self):
        _ = self.event_queue.get(False)
        _, fill_event = self.run_buy_sell_workflow(return_events=True)
        order_event = self.broker.order_history[-1]

        self.assertEqual(order_event.order_type, "MKT")
        self.assertEqual(order_event.order_id, 1)
        self.assertEqual(order_event.position_id, 1)
        self.assertEqual(order_event.side, "BUY")
        self.assertEqual(order_event.units, 100)
        self.assertEqual(order_event.timestamp.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(fill_event.side, "BUY")
        self.assertEqual(fill_event.timestamp.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(fill_event.fill_price, 102.0)
        self.assertEqual(fill_event.units, 100)

    def test_execute_order_next_bar_open(self):
        execution_price = "next"
        self.broker._exec_price = execution_price
        _ = self.event_queue.get(False)
        _, fill_event = self.run_buy_sell_workflow(
            exec_price=execution_price, return_events=True
        )

        position = self.broker.get_position(fill_event.symbol)

        self.assertEqual(position.open_time.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(position.fill_price, 100.0)
        self.assertEqual(position.last_price, 102.0)
        self.assertEqual(position.pnl, 200.0 - self.broker.commission)

    def test_close_all_open_positions_exec_current(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.run_buy_sell_workflow()
        self.run_bar_update_workflow()
        self.data_handler.continue_backtest = False
        self.broker.close_all_positions()
        closed_position = self.broker.get_positions_history()[-1]

        self.assertDictEqual(self.broker.get_positions(), {})
        self.assertEqual(closed_position.pnl, 399.0)
        self.assertEqual(closed_position.open_time.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(closed_position.close_time.strftime("%Y-%m-%d"), "2024-05-04")

    def test_close_all_open_positions_exec_next(self):
        execution_price = "next"
        self.broker._exec_price = execution_price
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.run_buy_sell_workflow(exec_price=execution_price)
        self.run_bar_update_workflow()
        self.data_handler.continue_backtest = False
        self.broker.close_all_positions()
        closed_position = self.broker.get_positions_history()[-1]

        self.assertDictEqual(self.broker.get_positions(), {})
        self.assertEqual(closed_position.pnl, 599.0)
        self.assertEqual(closed_position.open_time.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(closed_position.close_time.strftime("%Y-%m-%d"), "2024-05-04")

    def test_update_account_market_event(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)

        self.assertDictEqual(self.broker.get_positions(), {})
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 100_000.0)
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(
            recent_acct_history["timestamp"], self.data_handler.current_datetime
        )
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    def test_update_account_fill_event(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.run_buy_sell_workflow()
        acct_history = [
            {
                "timestamp": self.data_handler.current_datetime,
                "balance": 100_000.0,
                "equity": 100_000,
            }
        ]

        self.assertIn("SYMBOL1", self.broker.get_positions())
        self.assertEqual(self.broker.get_position("SYMBOL1").units, 100.0)
        self.assertEqual(self.broker.get_position("SYMBOL1").fill_price, 102.0)
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 89_800.0)
        self.assertEqual(self.broker.account_history, acct_history)

    def test_update_account_pnl(self):
        mkt_event = self.event_queue.get(False)
        self.run_buy_sell_workflow()
        self.data_handler.update_bars()
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)

        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_399.5)
        self.assertEqual(self.broker.free_margin, 90_199.5)
        self.assertNotEqual(self.broker.account_history, [])
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(
            recent_acct_history["timestamp"], self.data_handler.current_datetime
        )
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    def test_update_account_position_close(self):
        _ = self.event_queue.get(False)
        self.run_buy_sell_workflow()
        self.run_bar_update_workflow()
        self.run_close_workflow()

        self.assertNotIn("SYMBOL1", self.broker.get_positions())
        self.assertEqual("SYMBOL1", self.broker.get_positions_history()[-1].symbol)
        self.assertEqual(
            self.broker.get_positions_history()[-1].close_time,
            self.data_handler.current_datetime,
        )
        self.assertEqual(self.broker.balance, 100_399.0)
        self.assertEqual(self.broker.equity, 100_399.0)
        self.assertEqual(self.broker.free_margin, 100_399.0)
        self.assertNotEqual(self.broker.account_history, [])
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(
            recent_acct_history["timestamp"], self.data_handler.current_datetime
        )
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    def test_get_account_history(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.run_buy_sell_workflow()
        self.run_bar_update_workflow()
        self.run_close_workflow()

        account_history = self.broker.get_account_history()
        balance_equity_col = ["balance", "equity"]
        positions_col = [
            "symbol",
            "units",
            "open_price",
            "close_price",
            "commission",
            "pnl",
            "open_time",
            "close_time",
        ]
        orders_col = [
            "timestamp",
            "type",
            "symbol",
            "order_type",
            "units",
            "side",
            "status",
            "order_id",
            "position_id",
        ]

        self.assertIn("balance_equity", account_history)
        self.assertIn("positions", account_history)
        self.assertIn("orders", account_history)
        self.assertIsInstance(account_history["balance_equity"], pd.DataFrame)
        self.assertIsInstance(account_history["balance_equity"].index, pd.DatetimeIndex)
        self.assertIsInstance(account_history["positions"], pd.DataFrame)
        self.assertIsInstance(account_history["orders"], pd.DataFrame)
        self.assertTrue(
            set(balance_equity_col).issubset(account_history["balance_equity"].columns)
        )
        self.assertTrue(
            set(positions_col).issubset(account_history["positions"].columns)
        )
        self.assertTrue(set(orders_col).issubset(account_history["orders"].columns))


if __name__ == "__main__":
    unittest.main()
