import unittest
from itertools import product
from pathlib import Path

import pandas as pd

from margin_trader.broker.fill import Fill
from margin_trader.broker.order import Order, OrderManager
from margin_trader.broker.position import HedgePositionManager, NetPositionManager
from margin_trader.broker.sim_broker import SimBroker
from margin_trader.constants import OrderSide, OrderStatus, OrderType
from margin_trader.data_handlers import HistoricCSVDataHandler
from margin_trader.event import (
    FILLEVENT,
    MARKETEVENT,
    ORDEREVENT,
    EventListener,
    EventManager,
)

CSV_DIR = Path(__file__).parent.parent.joinpath("data")
SYMBOLS = ["SYMBOL1"]


class BrokerEventListener(EventListener):
    def __init__(self):
        self.order_events = []
        self.fill_events = []

    def on_order(self, event: Order):
        self.order_events.append(event)

    def on_fill(self, event: Fill):
        self.fill_events.append(event)

    def update(self, event):
        if isinstance(event, Order):
            self.on_order(event)
        elif isinstance(event, Fill):
            self.on_fill(event)


class TestSimBroker(unittest.TestCase):
    def create_broker(
        self,
        listener=None,
        data_handler=None,
        balance=100_000.0,
        acct_mode="netting",
        leverage=1,
        commission=0.0,
        stop_out_level=0.2,
        exec_price="current",
    ):
        broker = SimBroker(
            balance=balance,
            acct_mode=acct_mode,
            leverage=leverage,
            commission=commission,
            stop_out_level=stop_out_level,
            exec_price=exec_price,
        )
        broker_event_manager = EventManager()
        broker.add_event_manager(broker_event_manager)
        broker.add_data_handler(data_handler)
        broker.event_manager.subscribe(ORDEREVENT, listener)
        broker.event_manager.subscribe(FILLEVENT, listener)
        return broker

    def test_init(self):
        for acct_mode, exec_price in product(
            ["netting", "hedging"], ["current", "next"]
        ):
            with self.subTest(acct_mode, exec_price=exec_price):
                data_event_manager = EventManager()
                data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
                data_handler.add_event_manager(data_event_manager)
                broker = self.create_broker(
                    data_handler=data_handler,
                    acct_mode=acct_mode,
                    exec_price=exec_price,
                )

                self.assertEqual(broker.balance, 100_000.0)
                self.assertEqual(broker.equity, 100_000.0)
                self.assertEqual(broker.free_margin, 100_000.0)
                self.assertIsInstance(broker.data_handler, HistoricCSVDataHandler)
                self.assertEqual(broker.leverage, 1)
                self.assertEqual(broker.commission, 0.0)
                self.assertEqual(broker._exec_price, exec_price)
                self.assertListEqual(broker.account_history, [])
                self.assertIsInstance(broker._order_manager, OrderManager)
                if acct_mode == "netting":
                    self.assertIsInstance(broker._p_manager, NetPositionManager)
                else:
                    self.assertIsInstance(broker._p_manager, HedgePositionManager)

    def test_buy_sell_mkt_order_execution(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                broker_listener = BrokerEventListener()
                data_event_manager = EventManager()
                data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
                data_handler.add_event_manager(data_event_manager)
                broker = self.create_broker(
                    listener=broker_listener,
                    data_handler=data_handler,
                    acct_mode=acct_mode,
                )
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)

                order_event = broker.get_order_history()[0]
                fill_event = broker_listener.fill_events[0]

                self.assertEqual(order_event.symbol, fill_event.symbol)
                self.assertEqual(order_event.order_id, fill_event.order_id)
                self.assertEqual(order_event.position_id, fill_event.position_id)
                self.assertEqual(order_event.side, fill_event.side)
                self.assertEqual(order_event.units, fill_event.units)
                self.assertEqual(broker.balance, 100_000.0)
                self.assertEqual(broker.equity, 100_000.0)
                self.assertEqual(broker.free_margin, 89_800.0)
                if acct_mode == "netting":
                    self.assertIn(fill_event.symbol, broker.get_positions())
                elif acct_mode == "hedging":
                    self.assertIn(fill_event.position_id, broker.get_positions())

    def test_buy_sell_pending_mkt_order_execution(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                broker_listener = BrokerEventListener()
                data_event_manager = EventManager()
                data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
                data_handler.add_event_manager(data_event_manager)
                broker = self.create_broker(
                    listener=broker_listener,
                    data_handler=data_handler,
                    acct_mode=acct_mode,
                    exec_price="next",
                )
                data_handler.event_manager.subscribe(MARKETEVENT, broker)
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)

                pending_order_event = broker_listener.order_events[0]
                self.assertEqual(pending_order_event.status, OrderStatus.PENDING)

                data_handler.update_bars()

                executed_order_event = broker.get_order_history()[0]
                fill_event = broker_listener.fill_events[0]
                position = broker.get_position(
                    fill_event.symbol
                    if acct_mode == "netting"
                    else fill_event.position_id
                )

                self.assertEqual(
                    pending_order_event.order_id, executed_order_event.order_id
                )
                self.assertEqual(pending_order_event.status, OrderStatus.EXECUTED)
                self.assertEqual(executed_order_event.status, OrderStatus.EXECUTED)
                self.assertNotIn(
                    executed_order_event.order_id, broker._order_manager.pending_orders
                )
                self.assertEqual(executed_order_event.symbol, fill_event.symbol)
                self.assertEqual(executed_order_event.order_id, fill_event.order_id)
                self.assertEqual(
                    executed_order_event.position_id, fill_event.position_id
                )
                self.assertEqual(executed_order_event.side, fill_event.side)
                self.assertEqual(executed_order_event.units, fill_event.units)
                self.assertEqual(fill_event.fill_price, 102.0)
                self.assertEqual(
                    position.pnl,
                    400.0 if fill_event.side == OrderSide.BUY else -400,
                )
                self.assertEqual(broker.balance, 100_000.0)
                self.assertEqual(
                    broker.equity,
                    100_400.0 if fill_event.side == OrderSide.BUY else 99_600.0,
                )
                self.assertEqual(
                    broker.free_margin,
                    90_200 if fill_event.side == OrderSide.BUY else 89_400.0,
                )

    def test_reverse_order_execution(self):
        for side in OrderSide:
            broker_listener = BrokerEventListener()
            data_event_manager = EventManager()
            data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
            data_handler.add_event_manager(data_event_manager)
            broker = self.create_broker(
                listener=broker_listener,
                data_handler=data_handler,
                balance=12000,
                acct_mode="netting",
            )
            data_handler.event_manager.subscribe(MARKETEVENT, broker)
            data_handler.update_bars()

            if side == OrderSide.BUY:
                broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
            else:
                broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)

            with self.subTest(f"{side}: initial order"):
                order = broker.get_order_history()[-1]
                fill = broker_listener.fill_events.pop(0)
                self.assertEqual(order.units, 100)
                self.assertEqual(order.status, OrderStatus.EXECUTED)
                self.assertEqual(order.order_id, fill.position_id)
                self.assertEqual(fill.symbol, SYMBOLS[0])
                self.assertEqual(fill.result, "open")
                self.assertEqual(fill.fill_price, 102.0)
                self.assertEqual(broker.balance, 12000.0)
                self.assertEqual(broker.equity, 12000.0)
                self.assertEqual(broker.free_margin, 1800.0)

            data_handler.update_bars()
            if side == OrderSide.BUY:
                broker.sell(symbol=SYMBOLS[0], units=200, order_type=OrderType.MARKET)
            else:
                broker.buy(symbol=SYMBOLS[0], units=200, order_type=OrderType.MARKET)
            with self.subTest(f"{side}: reverse order"):
                order1 = broker.get_order_history(N=2)[-2]
                order2 = broker.get_order_history(N=2)[-1]
                fill_event1 = broker_listener.fill_events.pop(0)
                fill_event2 = broker_listener.fill_events.pop(0)

                self.assertEqual(order1.units, 100)
                self.assertEqual(order2.units, 100)
                self.assertEqual(fill_event1.fill_price, 106.0)
                self.assertEqual(fill_event1.result, "close")
                self.assertEqual(fill_event2.fill_price, 106.0)
                self.assertEqual(fill_event2.result, "open")
                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 12400.0)
                    self.assertEqual(broker.equity, 12400.0)
                    self.assertEqual(broker.free_margin, 1800)
                else:
                    self.assertEqual(broker.balance, 11600.0)
                    self.assertEqual(broker.equity, 11600.0)
                    self.assertEqual(broker.free_margin, 1000)

    def test_pending_reverse_order_execution(self):
        for side in OrderSide:
            broker_listener = BrokerEventListener()
            data_event_manager = EventManager()
            data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
            data_handler.add_event_manager(data_event_manager)
            broker = self.create_broker(
                listener=broker_listener,
                data_handler=data_handler,
                balance=12000,
                acct_mode="netting",
                exec_price="next",
            )
            data_handler.event_manager.subscribe(MARKETEVENT, broker)
            data_handler.update_bars()

            if side == OrderSide.BUY:
                broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
            else:
                broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)

            data_handler.update_bars()
            with self.subTest(f"{side}: initial order"):
                order = broker.get_order_history()[-1]
                fill = broker_listener.fill_events.pop(0)
                self.assertEqual(order.units, 100)
                self.assertEqual(order.status, OrderStatus.EXECUTED)
                self.assertEqual(order.order_id, fill.position_id)
                self.assertEqual(fill.symbol, SYMBOLS[0])
                self.assertEqual(fill.result, "open")
                self.assertEqual(fill.fill_price, 102.0)
                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 12000.0)
                    self.assertEqual(broker.equity, 12400.0)
                    self.assertEqual(broker.free_margin, 2200.0)
                else:
                    self.assertEqual(broker.balance, 12000.0)
                    self.assertEqual(broker.equity, 11600.0)
                    self.assertEqual(broker.free_margin, 1400.0)

            if side == OrderSide.BUY:
                broker.sell(symbol=SYMBOLS[0], units=200, order_type=OrderType.MARKET)
            else:
                broker.buy(symbol=SYMBOLS[0], units=200, order_type=OrderType.MARKET)

            data_handler.update_bars()
            with self.subTest(f"{side}: reverse order"):
                order1 = broker.get_order_history(N=2)[-2]
                order2 = broker.get_order_history(N=2)[-1]
                fill_event1 = broker_listener.fill_events.pop(0)
                fill_event2 = broker_listener.fill_events.pop(0)

                self.assertEqual(order1.units, 100)
                self.assertEqual(order2.units, 100)
                self.assertEqual(fill_event1.fill_price, 106.0)
                self.assertEqual(fill_event1.result, "close")
                self.assertEqual(fill_event2.fill_price, 106.0)
                self.assertEqual(fill_event2.result, "open")
                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 12400.0)
                    self.assertEqual(broker.equity, 12200.0)
                    self.assertEqual(broker.free_margin, 1600)
                else:
                    self.assertEqual(broker.balance, 11600.0)
                    self.assertEqual(broker.equity, 11800.0)
                    self.assertEqual(broker.free_margin, 1200)

    def test_buy_sell_mkt_order_rejected(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                broker_listener = BrokerEventListener()
                data_event_manager = EventManager()
                data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
                data_handler.add_event_manager(data_event_manager)
                broker = self.create_broker(
                    listener=broker_listener,
                    data_handler=data_handler,
                    balance=10000,
                    acct_mode=acct_mode,
                )
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)

                notified_order = broker_listener.order_events[0]
                historical_order = broker.get_order_history()[-1]

                self.assertEqual(notified_order.status, OrderStatus.REJECTED)
                self.assertEqual(historical_order.status, OrderStatus.REJECTED)
                self.assertEqual(notified_order.order_id, historical_order.order_id)

    def test_close_buy_sell_position(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                broker_listener = BrokerEventListener()
                data_event_manager = EventManager()
                data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
                data_handler.add_event_manager(data_event_manager)
                broker = self.create_broker(
                    listener=broker_listener,
                    data_handler=data_handler,
                    acct_mode=acct_mode,
                )
                data_handler.event_manager.subscribe(MARKETEVENT, broker)
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=SYMBOLS[0], order_type=OrderType.MARKET)
                data_handler.update_bars()

                if acct_mode == "netting":
                    position = broker.get_position(SYMBOLS[0])
                else:
                    position = broker.get_position(
                        broker_listener.fill_events[0].position_id
                    )
                broker.close(position)

                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 100_400.0)
                    self.assertEqual(broker.equity, 100_400.0)
                    self.assertEqual(broker.free_margin, 100_400.0)
                else:
                    self.assertEqual(broker.balance, 99_600.0)
                    self.assertEqual(broker.equity, 99_600.0)
                    self.assertEqual(broker.free_margin, 99_600.0)

    def test_close_all_position(self):
        broker_listener = BrokerEventListener()
        data_event_manager = EventManager()
        data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        data_handler.add_event_manager(data_event_manager)
        broker = self.create_broker(
            listener=broker_listener,
            data_handler=data_handler,
            acct_mode="hedging",
        )
        data_handler.event_manager.subscribe(MARKETEVENT, broker)
        data_handler.update_bars()

        broker.buy(symbol=SYMBOLS[0])
        broker.buy(symbol=SYMBOLS[0])
        broker.buy(symbol=SYMBOLS[0])

        with self.subTest("Open positions == 3"):
            positions = broker.get_positions()
            self.assertEqual(len(positions), 3)

        broker.close_all_positions()
        with self.subTest("Closed all positions"):
            positions = broker.get_positions()
            self.assertEqual(len(positions), 0)

    def test_account_history_update(self):
        broker_listener = BrokerEventListener()
        data_event_manager = EventManager()
        data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        data_handler.add_event_manager(data_event_manager)
        broker = self.create_broker(listener=broker_listener, data_handler=data_handler)
        data_handler.event_manager.subscribe(MARKETEVENT, broker)
        data_handler.update_bars()
        broker.buy(symbol=SYMBOLS[0])

        for _ in range(4):
            data_handler.update_bars()

        position = broker.get_position(SYMBOLS[0])
        broker.close(position)

        expected_account_history = [
            {
                "timestamp": pd.Timestamp("2024-05-03"),
                "balance": 100_000.0,
                "equity": 100_000.0,
            },
            {
                "timestamp": pd.Timestamp("2024-05-04"),
                "balance": 100_000.0,
                "equity": 100_400.0,
            },
            {
                "timestamp": pd.Timestamp("2024-05-05"),
                "balance": 100_000.0,
                "equity": 100_600.0,
            },
            {
                "timestamp": pd.Timestamp("2024-05-06"),
                "balance": 100_000.0,
                "equity": 100_800.0,
            },
            {
                "timestamp": pd.Timestamp("2024-05-07"),
                "balance": 101_000.0,
                "equity": 101_000.0,
            },
        ]

        self.assertListEqual(broker.account_history, expected_account_history)

    def test_get_account_history(self):
        broker_listener = BrokerEventListener()
        data_event_manager = EventManager()
        data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=SYMBOLS)
        data_handler.add_event_manager(data_event_manager)
        broker = self.create_broker(listener=broker_listener, data_handler=data_handler)
        data_handler.event_manager.subscribe(MARKETEVENT, broker)
        data_handler.update_bars()
        broker.buy(symbol=SYMBOLS[0])

        for _ in range(4):
            data_handler.update_bars()

        position = broker.get_position(SYMBOLS[0])
        broker.close(position)

        expected_balance_equity = pd.DataFrame(
            data=[
                {
                    "timestamp": pd.Timestamp("2024-05-03"),
                    "balance": 100_000.0,
                    "equity": 100_000.0,
                },
                {
                    "timestamp": pd.Timestamp("2024-05-04"),
                    "balance": 100_000.0,
                    "equity": 100_400.0,
                },
                {
                    "timestamp": pd.Timestamp("2024-05-05"),
                    "balance": 100_000.0,
                    "equity": 100_600.0,
                },
                {
                    "timestamp": pd.Timestamp("2024-05-06"),
                    "balance": 100_000.0,
                    "equity": 100_800.0,
                },
                {
                    "timestamp": pd.Timestamp("2024-05-07"),
                    "balance": 101_000.0,
                    "equity": 101_000.0,
                },
            ]
        )
        expected_balance_equity.set_index("timestamp", inplace=True)

        expected_pos_history = pd.DataFrame(
            data={
                "symbol": SYMBOLS[0],
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
                "symbol": [SYMBOLS[0]] * 2,
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

        acct_history = broker.get_account_history()

        with self.subTest("Balance and Equity"):
            pd.testing.assert_frame_equal(
                expected_balance_equity, acct_history["balance_equity"]
            )

        with self.subTest("Position History"):
            pd.testing.assert_frame_equal(
                expected_pos_history, acct_history["positions"]
            )

        with self.subTest("Order History"):
            pd.testing.assert_frame_equal(
                expected_order_history, acct_history["orders"]
            )

    # def test_buy_limit_submitted(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="LMT", price=99.0)
    #     order = self.broker.pending_orders.get(False)

    #     self.assertEqual(order.order_type, "LMT")
    #     self.assertEqual(order.units, 100)
    #     self.assertEqual(order.price, 99.0)

    # def test_sell_limit_submitted(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="LMT", price=105.0)
    #     order = self.broker.pending_orders.get(False)

    #     self.assertEqual(order.order_type, "LMT")
    #     self.assertEqual(order.units, 100)
    #     self.assertEqual(order.price, 105.0)

    # def test_buy_stop_submitted(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="STP", price=104.0)
    #     order = self.broker.pending_orders.get(False)

    #     self.assertEqual(order.order_type, "STP")
    #     self.assertEqual(order.units, 100)
    #     self.assertEqual(order.price, 104.0)

    # def test_sell_stop_submitted(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="STP", price=101.0)
    #     order = self.broker.pending_orders.get(False)

    #     self.assertEqual(order.order_type, "STP")
    #     self.assertEqual(order.units, 100)
    #     self.assertEqual(order.price, 101.0)

    # def test_execute_pending_buy_lmt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="LMT", price=101.0)
    #     self.data_handler.update_bars()
    #     _ = self.event_queue.get(False)
    #     self.broker.execute_pending_orders()
    #     order_event = self.event_queue.get(False)
    #     fill_event = self.event_queue.get(False)
    #     position = self.broker.get_position(fill_event.symbol)

    #     self.assertEqual(order_event.status, "EXECUTED")
    #     self.assertLess(order_event.timestamp, fill_event.timestamp)
    #     self.assertEqual(fill_event.symbol, order_event.symbol)
    #     self.assertEqual(fill_event.side, order_event.side)
    #     self.assertEqual(fill_event.fill_price, order_event.price)
    #     self.assertEqual(fill_event.units, order_event.units)
    #     self.assertEqual(position.pnl, 500.0 - self.broker.commission)

    # def test_execute_pending_sell_lmt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="LMT", price=105.0)
    #     self.data_handler.update_bars()
    #     _ = self.event_queue.get(False)
    #     self.broker.execute_pending_orders()
    #     order_event = self.event_queue.get(False)
    #     fill_event = self.event_queue.get(False)
    #     position = self.broker.get_position(fill_event.symbol)

    #     self.assertEqual(order_event.status, "EXECUTED")
    #     self.assertLess(order_event.timestamp, fill_event.timestamp)
    #     self.assertEqual(fill_event.symbol, order_event.symbol)
    #     self.assertEqual(fill_event.side, order_event.side)
    #     self.assertEqual(fill_event.fill_price, order_event.price)
    #     self.assertEqual(fill_event.units, order_event.units)
    #     self.assertEqual(position.pnl, -100.0 - self.broker.commission)

    # def test_execute_pending_buy_stp_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="STP", price=104.0)
    #     self.data_handler.update_bars()
    #     _ = self.event_queue.get(False)
    #     self.broker.execute_pending_orders()
    #     order_event = self.event_queue.get(False)
    #     fill_event = self.event_queue.get(False)
    #     position = self.broker.get_position(fill_event.symbol)

    #     self.assertEqual(order_event.status, "EXECUTED")
    #     self.assertLess(order_event.timestamp, fill_event.timestamp)
    #     self.assertEqual(fill_event.symbol, order_event.symbol)
    #     self.assertEqual(fill_event.side, order_event.side)
    #     self.assertEqual(fill_event.fill_price, order_event.price)
    #     self.assertEqual(fill_event.units, order_event.units)
    #     self.assertEqual(position.pnl, 200.0 - self.broker.commission)

    # def test_execute_pending_sell_stp_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="STP", price=101.0)
    #     self.data_handler.update_bars()
    #     _ = self.event_queue.get(False)
    #     self.broker.execute_pending_orders()
    #     order_event = self.event_queue.get(False)
    #     fill_event = self.event_queue.get(False)
    #     position = self.broker.get_position(fill_event.symbol)

    #     self.assertEqual(order_event.status, "EXECUTED")
    #     self.assertLess(order_event.timestamp, fill_event.timestamp)
    #     self.assertEqual(fill_event.symbol, order_event.symbol)
    #     self.assertEqual(fill_event.side, order_event.side)
    #     self.assertEqual(fill_event.fill_price, order_event.price)
    #     self.assertEqual(fill_event.units, order_event.units)
    #     self.assertEqual(position.pnl, -500.0 - self.broker.commission)

    # def test_close_all_open_positions_exec_current(self):
    #     mkt_event = self.event_queue.get(False)
    #     self.broker.update_account(mkt_event)
    #     self.run_buy_sell_workflow()
    #     self.run_bar_update_workflow()
    #     self.data_handler.continue_backtest = False
    #     self.broker.close_all_positions()
    #     closed_position = self.broker.get_positions_history()[-1]

    #     self.assertDictEqual(self.broker.get_positions(), {})
    #     self.assertEqual(closed_position.pnl, 399.0)
    #     self.assertEqual(closed_position.open_time.strftime("%Y-%m-%d"), "2024-05-03")
    #     self.assertEqual(closed_position.close_time.strftime("%Y-%m-%d"), "2024-05-04")

    # def test_close_all_open_positions_exec_next(self):
    #     execution_price = "next"
    #     self.broker._exec_price = execution_price
    #     mkt_event = self.event_queue.get(False)
    #     self.broker.update_account(mkt_event)
    #     self.run_buy_sell_workflow(exec_price=execution_price)
    #     self.run_bar_update_workflow()
    #     self.data_handler.continue_backtest = False
    #     self.broker.close_all_positions()
    #     closed_position = self.broker.get_positions_history()[-1]

    #     self.assertDictEqual(self.broker.get_positions(), {})
    #     self.assertEqual(closed_position.pnl, 599.0)
    #     self.assertEqual(closed_position.open_time.strftime("%Y-%m-%d"), "2024-05-04")
    #     self.assertEqual(closed_position.close_time.strftime("%Y-%m-%d"), "2024-05-05")

    # def test_sl_tp_order_submitted_for_buy_mkt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], sl=99.0, tp=105.0)
    #     position = self.broker.get_position(SYMBOLS[0])
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "SELL")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 99.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "SELL")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 105.0)
    #     self.assertEqual(position.id, sl_order.position_id)
    #     self.assertEqual(position.id, tp_order.position_id)

    # def test_sl_tp_order_submitted_for_sell_mkt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], sl=104.0, tp=95.0)
    #     position = self.broker.get_position(SYMBOLS[0])
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "BUY")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 104.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "BUY")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 95.0)
    #     self.assertEqual(position.id, sl_order.position_id)
    #     self.assertEqual(position.id, tp_order.position_id)

    # def test_sl_tp_order_submitted_for_buy_lmt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="LMT", price=100.0, sl=95.0, tp=105.0)
    #     lmt_order = self.broker.pending_orders.get(False)
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "SELL")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 95.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "SELL")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 105.0)
    #     self.assertEqual(lmt_order.symbol, sl_order.symbol)
    #     self.assertEqual(lmt_order.symbol, tp_order.symbol)
    #     self.assertEqual(lmt_order.order_id, sl_order.order_id)
    #     self.assertEqual(lmt_order.order_id, tp_order.order_id)

    # def test_sl_tp_order_submitted_for_sell_lmt_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="LMT", price=105.0, sl=110.0, tp=100.0)
    #     lmt_order = self.broker.pending_orders.get(False)
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "BUY")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 110.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "BUY")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 100.0)
    #     self.assertEqual(lmt_order.symbol, sl_order.symbol)
    #     self.assertEqual(lmt_order.symbol, tp_order.symbol)
    #     self.assertEqual(lmt_order.order_id, sl_order.order_id)
    #     self.assertEqual(lmt_order.order_id, tp_order.order_id)

    # def test_sl_tp_order_submitted_for_buy_stp_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.buy(SYMBOLS[0], order_type="STP", price=105.0, sl=100.0, tp=110.0)
    #     stp_order = self.broker.pending_orders.get(False)
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "SELL")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 100.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "SELL")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 110.0)
    #     self.assertEqual(stp_order.symbol, sl_order.symbol)
    #     self.assertEqual(stp_order.symbol, tp_order.symbol)
    #     self.assertEqual(stp_order.order_id, sl_order.order_id)
    #     self.assertEqual(stp_order.order_id, tp_order.order_id)

    # def test_sl_tp_order_submitted_for_sell_stp_order(self):
    #     _ = self.event_queue.get(False)
    #     self.broker.sell(SYMBOLS[0], order_type="STP", price=100.0, sl=105.0, tp=95.0)
    #     stp_order = self.broker.pending_orders.get(False)
    #     sl_order = self.broker.pending_orders.get(False)
    #     tp_order = self.broker.pending_orders.get(False)

    #     self.assertEqual(sl_order.order_type, "STP")
    #     self.assertEqual(sl_order.side, "BUY")
    #     self.assertEqual(sl_order.request, "close")
    #     self.assertEqual(sl_order.price, 105.0)
    #     self.assertEqual(tp_order.order_type, "LMT")
    #     self.assertEqual(tp_order.side, "BUY")
    #     self.assertEqual(tp_order.request, "close")
    #     self.assertEqual(tp_order.price, 95.0)
    #     self.assertEqual(stp_order.symbol, sl_order.symbol)
    #     self.assertEqual(stp_order.symbol, tp_order.symbol)
    #     self.assertEqual(stp_order.order_id, sl_order.order_id)
    #     self.assertEqual(stp_order.order_id, tp_order.order_id)

    # def test_sl_mkt_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], sl=100.0)
    #         order = self.broker.order_history[-1]
    #         position = self.broker.get_position(SYMBOLS[0])
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")
    #         self.assertEqual(sl_order.position_id, position.id)

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], sl=104.0)
    #         order = self.broker.order_history[-1]
    #         position = self.broker.get_position(SYMBOLS[0])
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")
    #         self.assertEqual(sl_order.position_id, position.id)

    # def test_sl_lmt_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], order_type="LMT", price=101.0, sl=100.0)
    #         order = self.broker.pending_orders.get(False)
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], order_type="LMT", price=104.0, sl=106.0)
    #         order = self.broker.pending_orders.get(False)
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")

    # def test_sl_stp_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], order_type="STP", price=103.0, sl=100.0)
    #         order = self.broker.pending_orders.get(False)
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], order_type="STP", price=101.0, sl=103.0)
    #         order = self.broker.pending_orders.get(False)
    #         sl_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, sl_order.order_id)
    #         self.assertEqual(sl_order.price, order.sl)
    #         self.assertIsNone(order.tp)
    #         self.assertEqual(sl_order.order_type, "STP")

    # def test_tp_mkt_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], tp=105.0)
    #         order = self.broker.order_history[-1]
    #         position = self.broker.get_position(SYMBOLS[0])
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")
    #         self.assertEqual(tp_order.position_id, position.id)

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], tp=95.0)
    #         order = self.broker.order_history[-1]
    #         position = self.broker.get_position(SYMBOLS[0])
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")
    #         self.assertEqual(tp_order.position_id, position.id)

    # def test_tp_lmt_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], order_type="LMT", price=101.0, tp=105.0)
    #         order = self.broker.pending_orders.get(False)
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], order_type="LMT", price=104.0, tp=100.0)
    #         order = self.broker.pending_orders.get(False)
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")

    # def test_tp_stp_order_submitted(self):
    #     _ = self.event_queue.get(False)

    #     with self.subTest("BUY"):
    #         self.broker.buy(SYMBOLS[0], order_type="STP", price=103.0, tp=105.0)
    #         order = self.broker.pending_orders.get(False)
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")

    #     self.broker.reset()
    #     with self.subTest("SELL"):
    #         self.broker.sell(SYMBOLS[0], order_type="STP", price=101.0, tp=95.0)
    #         order = self.broker.pending_orders.get(False)
    #         tp_order = self.broker.pending_orders.get(False)

    #         self.assertEqual(order.order_id, tp_order.order_id)
    #         self.assertEqual(tp_order.price, order.tp)
    #         self.assertIsNone(order.sl)
    #         self.assertEqual(tp_order.order_type, "LMT")

    # def test_get_used_margin_no_positions(self):
    #     broker = self.create_broker()
    #     used_margin = broker.get_used_margin()
    #     self.assertEqual(used_margin, 0.0)

    # def test_get_used_margin_open_positions(self):
    #     _ = self.event_queue.get(False)
    #     self.run_buy_sell_workflow()
    #     used_margin = self.broker.get_used_margin()

    #     self.assertEqual(used_margin, 10_200.0)

    # def test_get_positions(self):
    #     self.assertDictEqual(self.broker.get_positions(), {})

    # def test_get_position_history(self):
    #     self.assertListEqual(self.broker.get_positions_history(), [])


if __name__ == "__main__":
    unittest.main()
