import os
import csv
import unittest
import pandas as pd
from pathlib import Path
from queue import Queue
from margin_trader.data_source import HistoricCSVDataHandler
from margin_trader.broker.sim_broker import SimBroker, PositionManager

CSV_DIR = Path(__file__).parent
symbols = ["AAPL"]

class TestSimBroker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data = [
            ["2024-05-03", 100.0, 105.0, 98.0, 102.0, 102.0, 0],
            ["2024-05-04", 102.0, 108.0, 100.0, 106.0, 106.0, 0],
            ["2024-05-07", 106.0, 110.0, 104.0, 108.0, 108.0, 0],
            ["2024-05-08", 108.0, 112.0, 106.0, 110.0, 110.0, 0],
            ["2024-05-09", 110.0, 115.0, 108.0, 112.0, 112.0, 0]
        ]

        with open(CSV_DIR/"AAPL.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            )
            for day in data:
                writer.writerow(day)

    def setUp(self):
        self.event_queue = Queue()
        self.data_handler = HistoricCSVDataHandler(
            csv_dir = CSV_DIR,
            symbol_list = symbols
        )
        self.data_handler._add_event_queue(self.event_queue)
        
        self.broker = SimBroker(
            balance = 100_000.0,
            data_handler = self.data_handler,
            commission = 0.5
        )
        self.broker._add_event_queue(self.event_queue)
        self.data_handler.update_bars() # Add market event to the event queue

    def test_init(self):
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 100_000.0)
        self.assertIsInstance(self.broker.data_handler, HistoricCSVDataHandler)
        self.assertEqual(self.broker.events, self.event_queue)
        self.assertEqual(self.broker.leverage, 1)
        self.assertEqual(self.broker.commission, 0.5)
        self.assertIsInstance(self.broker.p_manager, PositionManager)
        self.assertEqual(self.broker._exec_price, "current")
        self.assertIsInstance(self.broker.pending_orders, Queue)
        acct_history = [
            {
                "timeindex": self.data_handler.current_datetime,
                "balance": 100_000.0,
                "equity": 100_000
            }
        ]
        self.assertListEqual(self.broker.account_history, acct_history)


    def test_buy(self):
        _ = self.event_queue.get(False) # Generate signal from market event
        symbol = "AAPL"
        self.broker.buy(symbol)
        event = self.event_queue.get(False)

        self.assertEqual(event.type, "ORDER")
        self.assertEqual(event.symbol, symbol)
        self.assertEqual(event.side, "BUY")
        self.assertEqual(event.units, 100)
        self.assertEqual(event.order_type, "MKT")

    def test_sell(self):
        _ = self.event_queue.get(False)
        symbol = "AAPL"
        self.broker.sell(symbol)
        event = self.event_queue.get(False)

        self.assertEqual(event.type, "ORDER")
        self.assertEqual(event.symbol, symbol)
        self.assertEqual(event.side, "SELL")
        self.assertEqual(event.units, 100)
        self.assertEqual(event.order_type, "MKT")

    def test_execute_order_same_bar_close(self):
        _ = self.event_queue.get(False)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)

        self.assertEqual(fill_event.type, "FILL")
        self.assertEqual(fill_event.side, "BUY")
        self.assertEqual(fill_event.timeindex.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(fill_event.fill_price, 102.0)

    def test_execute_order_next_bar_open(self):
        self.broker._exec_price = "next" # Change broker MKT execution price to next open
        _ = self.event_queue.get(False)
        self.broker.buy("AAPL")
        self.broker.check_pending_orders()
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)

        self.assertEqual(fill_event.timeindex.strftime("%Y-%m-%d"), "2024-05-03")
        self.assertEqual(fill_event.fill_price, 100.0)

    def test_get_used_margin_no_positions(self):
        used_margin = self.broker.get_used_margin()
        self.assertEqual(used_margin, 0.0)
    
    def test_get_used_margin_open_positions(self):
        _ = self.event_queue.get(False)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.p_manager.update_position_from_fill(fill_event)
        used_margin = self.broker.get_used_margin()

        self.assertEqual(used_margin, 10_200.0)

    def test_get_position(self):
        self.assertEqual(self.broker.get_position(symbols[0]), False)

    def test_get_position(self):
        self.assertDictEqual(self.broker.get_positions(), {})

    def test_get_position_history(self):
        self.assertListEqual(self.broker.get_positions_history(), [])

    def test_update_account_market_event(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.assertDictEqual(self.broker.get_positions(), {})
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 100_000.0)
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(recent_acct_history["timeindex"],
                             self.data_handler.current_datetime)
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    def test_update_account_fill_event(self):
        _ = self.event_queue.get(False)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)
        acct_history = [
            {
                "timeindex": self.data_handler.current_datetime,
                "balance": 100_000.0,
                "equity": 100_000
            }
        ]

        self.assertIn("AAPL", self.broker.get_positions())
        self.assertEqual(self.broker.get_position("AAPL").units, 100.0)
        self.assertEqual(self.broker.get_position("AAPL").fill_price, 102.0)
        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_000.0)
        self.assertEqual(self.broker.free_margin, 89_800.0)
        self.assertEqual(self.broker.account_history, acct_history)

    def test_update_account_pnl(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)
        self.data_handler.update_bars()
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)

        self.assertEqual(self.broker.balance, 100_000.0)
        self.assertEqual(self.broker.equity, 100_399.5)
        self.assertEqual(self.broker.free_margin, 90_199.5)
        self.assertNotEqual(self.broker.account_history, [])
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(recent_acct_history["timeindex"],
                             self.data_handler.current_datetime)
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    def test_update_account_position_close(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)
        self.data_handler.update_bars()
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.broker.close("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)

        self.assertNotIn("AAPL", self.broker.get_positions())
        self.assertEqual("AAPL", self.broker.get_positions_history()[-1].symbol)
        self.assertEqual(self.broker.get_positions_history()[-1].close_time,
                         self.data_handler.current_datetime)
        self.assertEqual(self.broker.balance, 100_399.0)
        self.assertEqual(self.broker.equity, 100_399.0)
        self.assertEqual(self.broker.free_margin, 100_399.0)
        self.assertNotEqual(self.broker.account_history, [])
        recent_acct_history = self.broker.account_history[-1]
        self.assertEqual(recent_acct_history["timeindex"],
                             self.data_handler.current_datetime)
        self.assertEqual(recent_acct_history["balance"], self.broker.balance)
        self.assertEqual(recent_acct_history["equity"], self.broker.equity)

    
    def test_check_pending_orders(self):
        self.broker._exec_price = "next" # Change broker MKT execution price to next open
        _ = self.event_queue.get(False)

        self.broker.buy("AAPL")
        self.broker.check_pending_orders()
        order_event = self.event_queue.get(False)
        self.assertEqual(order_event.status, "PENDING")
        self.assertEqual(order_event.order_type, "MKT")

    def test_get_account_history(self):
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.broker.buy("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)
        self.data_handler.update_bars()
        mkt_event = self.event_queue.get(False)
        self.broker.update_account(mkt_event)
        self.broker.close("AAPL")
        order_event = self.event_queue.get(False)
        self.broker.execute_order(order_event)
        fill_event = self.event_queue.get(False)
        self.broker.update_account(fill_event)
        account_history = self.broker.get_account_history()
        balance_equity_col = ["timeindex", "balance", "equity"]
        positions_col = ["symbol", "units", "open_price",
                         "close_price", "commission", "pnl", "open_time",
                         "close_time"]

        self.assertIn("balance_equity", account_history)
        self.assertIn("positions", account_history)
        self.assertIsInstance(account_history["balance_equity"], pd.DataFrame)
        self.assertIsInstance(account_history["positions"], pd.DataFrame)
        self.assertTrue(
            set(balance_equity_col).issubset(account_history["balance_equity"].columns)
        )
        self.assertTrue(
            set(positions_col).issubset(account_history["positions"].columns)
        )

    
    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR/"AAPL.csv"):
            os.remove(CSV_DIR/"AAPL.csv")


if __name__ == "__main__":
    unittest.main()