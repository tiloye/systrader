# mypy: disable-error-code=union-attr
import datetime as dt
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import Mock

from margin_trader.broker.order import (
    BracketOrder,
    CoverOrder,
    Order,
    OrderManager,
    ReverseOrder,
)
from margin_trader.broker.position import Position
from margin_trader.constants import OrderSide, OrderStatus, OrderType
from margin_trader.errors import (
    LimitOrderError,
    OrderError,
    StopLossPriceError,
    StopOrderError,
    TakeProfitPriceError,
)

SYMBOL = "GOOG"
TIMESTAMP = dt.datetime(2023, 1, 1, 12, 0, 0)


class TestOrder(unittest.TestCase):
    def test_execute_buy(self):
        for order_type, price in zip(
            [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP], [None, 100.0, 105.0]
        ):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=dt.datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.BUY,
                    price=price,
                )
                order.execute()
                self.assertEqual(order.status, OrderStatus.EXECUTED)

    def test_execute_sell(self):
        for order_type, price in zip(
            [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP], [None, 105.0, 100.0]
        ):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=dt.datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.SELL,
                    price=price,
                )
                order.execute()
                self.assertEqual(order.status, OrderStatus.EXECUTED)

    def test_reject_buy(self):
        for order_type, price in zip(OrderType, [None, 100.0, 105.0]):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=dt.datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.BUY,
                    price=price,
                )
                order.reject()
                self.assertEqual(order.status, OrderStatus.REJECTED)

    def test_reject_sell(self):
        for order_type, price in zip(OrderType, [None, 105.0, 100.0]):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=dt.datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.SELL,
                    price=price,
                )
                order.reject()
                self.assertEqual(order.status, OrderStatus.REJECTED)

    def test_print_order(self):
        order = Order(
            timestamp=dt.datetime(2023, 1, 1, 12, 0, 0),
            symbol=SYMBOL,
            order_type=OrderType.MARKET,
            units=100,
            side=OrderSide.BUY,
        )
        expected_output = (
            f"Order: Symbol={SYMBOL}, Type=mkt, units=100, Direction=buy\n"
        )
        with StringIO() as out, redirect_stdout(out):
            order.print_order()
            output = out.getvalue()
            self.assertEqual(expected_output, output)


class TestOrderManager(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = Mock()
        self.broker.data_handler.current_datetime = TIMESTAMP
        self.broker.data_handler.get_latest_price.return_value = 102.0
        self.broker.get_position.return_value = None
        self.order_manager = OrderManager(broker=self.broker)

    def test_create_mkt_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side in OrderSide:
                with self.subTest(msg=f"{acct_mode} account", side=side):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order = self.order_manager.create_order(
                        symbol=SYMBOL, order_type=OrderType.MARKET, side=side
                    )

                    self.assertEqual(order.timestamp, TIMESTAMP)
                    self.assertEqual(order.symbol, SYMBOL)
                    self.assertEqual(order.request, "open")
                    self.assertEqual(order.order_id, 1)
                    self.assertEqual(order.position_id, 1)
                    self.assertEqual(order.side, side)
                    self.assertEqual(order.units, 100)
                    self.assertIn(order, self.order_manager.history)

    def test_create_lmt_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price in zip(OrderSide, [100.0, 104.0]):
                with self.subTest(msg=f"{acct_mode} account", side=side):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        side=side,
                        price=price,
                    )
                    order = self.order_manager.pending_orders[order_id]

                    self.assertEqual(order.order_type, OrderType.LIMIT)
                    self.assertEqual(order.side, side)
                    self.assertEqual(order.price, price)
                    self.assertEqual(order.order_id, 1)
                    self.assertIn(order, self.order_manager.history)

    def test_create_stp_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price in zip(OrderSide, [104.0, 100.0]):
                with self.subTest(msg=f"{acct_mode} account", side=side):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        side=side,
                        price=price,
                    )
                    order = self.order_manager.pending_orders[order_id]

                    self.assertEqual(order.order_type, OrderType.STOP)
                    self.assertEqual(order.side, side)
                    self.assertEqual(order.price, price)
                    self.assertEqual(order.order_id, 1)
                    self.assertIn(order, self.order_manager.history)

    def test_create_pending_mkt_order(self):
        # When broker account is set to execute trades at next available price, then
        # market orders will be added to pending orders

        self.broker._exec_price = "next"
        for acct_mode in ["netting", "hedging"]:
            self.broker.acct_mode = acct_mode
            for side, id_ in zip(OrderSide, [1, 2]):
                with self.subTest(msg=f"{acct_mode} account", side=side):
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL, order_type=OrderType.MARKET, side=side
                    )
                    order = self.order_manager.pending_orders[order_id]

                    self.assertEqual(order_id, id_)
                    self.assertEqual(order.symbol, SYMBOL)
                    self.assertEqual(order.request, "open")
                    self.assertEqual(order.position_id, id_)
                    self.assertEqual(order.side, side)
                    self.assertEqual(order.units, 100)
                    self.assertIn(order, self.order_manager.history)
            self.order_manager.reset()

    def test_create_mkt_cover_order_sl(self):
        for acct_mode in ["netting", "hedging"]:
            self.broker.acct_mode = acct_mode
            for side, sl_price in zip(OrderSide, [100.0, 104.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, sl_price=sl_price
                ):
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        sl=sl_price,
                    )
                    order = self.order_manager.pending_orders[order_id]
                    porder = order.primary_order
                    corder = order.cover_order

                    self.assertIsInstance(order, CoverOrder)
                    self.assertEqual(porder.timestamp, corder.timestamp)
                    self.assertEqual(porder.symbol, corder.symbol)
                    self.assertEqual(porder.order_id, corder.order_id)
                    self.assertEqual(porder.position_id, corder.position_id)
                    self.assertEqual(porder.units, corder.units)
                    self.assertEqual(porder.order_type, OrderType.MARKET)
                    self.assertEqual(porder.request, "open")
                    self.assertEqual(corder.request, "close")
                    self.assertEqual(corder.order_type, OrderType.STOP)
                    self.assertEqual(corder.price, sl_price)
                    self.assertIn(porder, self.order_manager.history)
                    self.assertIn(corder, self.order_manager.history)
                    if porder.side == OrderSide.BUY:
                        self.assertEqual(corder.side, OrderSide.SELL)
                    else:
                        self.assertEqual(corder.side, OrderSide.BUY)
            self.order_manager.reset()

    def test_create_lmt_cover_order_sl(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, sl_price in zip(OrderSide, [100.0, 104.0], [95.0, 106.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=sl_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        side=side,
                        price=price,
                        sl=sl_price,
                    )
                    c_order = self.order_manager.pending_orders[order_id]
                    p_order = c_order.primary_order
                    sl_order = c_order.cover_order

                    self.assertIsInstance(c_order, CoverOrder)
                    self.assertIsNotNone(p_order)
                    self.assertEqual(p_order.timestamp, sl_order.timestamp)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.position_id, sl_order.position_id)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.order_type, OrderType.LIMIT)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)

    def test_create_stp_cover_order_sl(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, sl_price in zip(OrderSide, [104.0, 100.0], [100.0, 105.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=sl_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        side=side,
                        price=price,
                        sl=sl_price,
                    )
                    c_order = self.order_manager.pending_orders[order_id]
                    p_order = c_order.primary_order
                    sl_order = c_order.cover_order

                    self.assertIsInstance(c_order, CoverOrder)
                    self.assertIsNotNone(p_order)
                    self.assertEqual(p_order.timestamp, sl_order.timestamp)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.position_id, sl_order.position_id)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.order_type, OrderType.STOP)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)

    def test_create_mkt_cover_order_tp(self):
        for acct_mode in ["netting", "hedging"]:
            for side, tp_price in zip(OrderSide, [104.0, 100.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=tp_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        tp=tp_price,
                    )
                    order = self.order_manager.pending_orders[order_id]
                    p_order = order.primary_order
                    tp_order = order.cover_order

                    self.assertIsInstance(order, CoverOrder)
                    self.assertEqual(p_order.timestamp, tp_order.timestamp)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.position_id, tp_order.position_id)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.MARKET)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_create_lmt_cover_order_tp(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, tp_price in zip(OrderSide, [100.0, 104.0], [104.0, 100.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=tp_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        side=side,
                        price=price,
                        tp=tp_price,
                    )
                    c_order = self.order_manager.pending_orders[order_id]
                    p_order = c_order.primary_order
                    tp_order = c_order.cover_order

                    self.assertIsInstance(c_order, CoverOrder)
                    self.assertIsNotNone(p_order)
                    self.assertEqual(p_order.timestamp, tp_order.timestamp)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.position_id, tp_order.position_id)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.LIMIT)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_create_stp_cover_order_tp(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, tp_price in zip(OrderSide, [104.0, 100.0], [108.0, 95.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=tp_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        side=side,
                        price=price,
                        tp=tp_price,
                    )
                    c_order = self.order_manager.pending_orders[order_id]
                    p_order = c_order.primary_order
                    tp_order = c_order.cover_order

                    self.assertIsInstance(c_order, CoverOrder)
                    self.assertIsNotNone(p_order)
                    self.assertEqual(p_order.timestamp, tp_order.timestamp)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.position_id, tp_order.position_id)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.STOP)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_create_pending_mkt_cover_order_sl(self):
        for acct_mode in ["netting", "hedging"]:
            for side, sl_price in zip(OrderSide, [100.0, 104.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, sl_price=sl_price
                ):
                    self.setUp()

                    self.broker.acct_mode = acct_mode
                    self.broker._exec_price = "next"
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        sl=sl_price,
                    )
                    corder = self.order_manager.pending_orders[order_id]
                    p_order = corder.primary_order
                    sl_order = corder.cover_order

                    self.assertIsInstance(corder, CoverOrder)
                    self.assertEqual(p_order.timestamp, sl_order.timestamp)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.position_id, sl_order.position_id)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.order_type, OrderType.MARKET)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)

    def test_create_pending_mkt_cover_order_tp(self):
        self.broker._exec_price = "next"
        for acct_mode in ["netting", "hedging"]:
            self.broker.acct_mode = acct_mode
            for side, tp_price in zip(OrderSide, [104.0, 100.0]):
                with self.subTest(
                    msg=f"{acct_mode} account", side=side, tp_price=tp_price
                ):
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        tp=tp_price,
                    )
                    corder = self.order_manager.pending_orders[order_id]
                    p_order = corder.primary_order
                    tp_order = corder.cover_order

                    self.assertIsInstance(corder, CoverOrder)
                    self.assertEqual(p_order.timestamp, tp_order.timestamp)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.position_id, tp_order.position_id)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.MARKET)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(tp_order.side, OrderSide.BUY)
            self.order_manager.reset()

    def test_create_mkt_bracket_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side, tp_price, sl_price in zip(
                OrderSide, [104.0, 100.0], [100.0, 104.0]
            ):
                with self.subTest(
                    msg=f"{acct_mode} account",
                    side=side,
                    tp_price=tp_price,
                    sl_price=sl_price,
                ):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        tp=tp_price,
                        sl=sl_price,
                    )
                    border = self.order_manager.pending_orders[order_id]
                    p_order = border.primary_order
                    sl_order = border.stop_order
                    tp_order = border.limit_order

                    self.assertIsInstance(border, BracketOrder)
                    self.assertEqual(p_order.order_id, border.id)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.MARKET)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    self.assertIn
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_create_pending_mkt_bracket_order(self):
        self.broker._exec_price = "next"
        for acct_mode in ["netting", "hedging"]:
            self.broker.acct_mode = acct_mode
            for side, tp_price, sl_price in zip(
                OrderSide, [104.0, 100.0], [100.0, 104.0]
            ):
                with self.subTest(
                    msg=f"{acct_mode} account",
                    side=side,
                    tp_price=tp_price,
                    sl_price=sl_price,
                ):
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.MARKET,
                        side=side,
                        tp=tp_price,
                        sl=sl_price,
                    )
                    border = self.order_manager.pending_orders[order_id]
                    p_order = border.primary_order
                    sl_order = border.stop_order
                    tp_order = border.limit_order

                    self.assertIsInstance(border, BracketOrder)
                    self.assertEqual(p_order.order_id, border.id)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.MARKET)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)
                        self.assertEqual(tp_order.side, OrderSide.BUY)
            self.order_manager.reset()

    def test_create_lmt_bracket_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, tp_price, sl_price in zip(
                OrderSide, [100.0, 104.0], [104.0, 100.0], [95.0, 106.0]
            ):
                with self.subTest(
                    msg=f"{acct_mode} account",
                    side=side,
                    price=price,
                    tp_price=tp_price,
                    sl_price=sl_price,
                ):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        side=side,
                        price=price,
                        tp=tp_price,
                        sl=sl_price,
                    )
                    border = self.order_manager.pending_orders[order_id]
                    p_order = border.primary_order
                    sl_order = border.stop_order
                    tp_order = border.limit_order

                    self.assertIsInstance(border, BracketOrder)
                    self.assertEqual(p_order.order_id, border.id)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.LIMIT)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_create_stp_bracket_order(self):
        for acct_mode in ["netting", "hedging"]:
            for side, price, tp_price, sl_price in zip(
                OrderSide, [104.0, 100.0], [106.0, 95.0], [102.0, 102.0]
            ):
                with self.subTest(
                    msg=f"{acct_mode} account",
                    side=side,
                    price=price,
                    tp_price=tp_price,
                    sl_price=sl_price,
                ):
                    self.setUp()
                    self.broker.acct_mode = acct_mode
                    order_id = self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        side=side,
                        price=price,
                        tp=tp_price,
                        sl=sl_price,
                    )
                    border = self.order_manager.pending_orders[order_id]
                    p_order = border.primary_order
                    sl_order = border.stop_order
                    tp_order = border.limit_order

                    self.assertIsInstance(border, BracketOrder)
                    self.assertEqual(p_order.order_id, border.id)
                    self.assertEqual(p_order.order_id, sl_order.order_id)
                    self.assertEqual(p_order.order_id, tp_order.order_id)
                    self.assertEqual(p_order.symbol, sl_order.symbol)
                    self.assertEqual(p_order.symbol, tp_order.symbol)
                    self.assertEqual(p_order.units, sl_order.units)
                    self.assertEqual(p_order.units, tp_order.units)
                    self.assertEqual(p_order.order_type, OrderType.STOP)
                    self.assertEqual(p_order.request, "open")
                    self.assertEqual(sl_order.request, "close")
                    self.assertEqual(tp_order.request, "close")
                    self.assertEqual(sl_order.order_type, OrderType.STOP)
                    self.assertEqual(tp_order.order_type, OrderType.LIMIT)
                    self.assertEqual(sl_order.price, sl_price)
                    self.assertEqual(tp_order.price, tp_price)
                    self.assertIn(p_order, self.order_manager.history)
                    self.assertIn(sl_order, self.order_manager.history)
                    self.assertIn(tp_order, self.order_manager.history)
                    if p_order.side == OrderSide.BUY:
                        self.assertEqual(sl_order.side, OrderSide.SELL)
                        self.assertEqual(tp_order.side, OrderSide.SELL)
                    else:
                        self.assertEqual(sl_order.side, OrderSide.BUY)
                        self.assertEqual(tp_order.side, OrderSide.BUY)

    def test_net_acct_reversal_order(self):
        # Reversing a position by placing a reverse order 2 times more than the position
        # units (quantity)
        self.broker.acct_mode = "netting"

        for broker_exec_price in ["current", "next"]:
            self.broker._exec_price = broker_exec_price
            for side, reverse_side in OrderSide, [OrderSide.SELL, OrderSide.BUY]:
                for units, reverse_units in [100, 200], [100, 150]:
                    with self.subTest(
                        f"{broker_exec_price}_{side}",
                        reverse_side=reverse_side.value,
                        units=units,
                        reverse_units=reverse_units,
                    ):
                        # Increase current order id value to next value by placing an order
                        _ = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=side,
                            units=units,
                        )

                        # Create an existing position
                        self.broker.get_position.return_value = Position(
                            timestamp=TIMESTAMP,
                            symbol=SYMBOL,
                            units=units,
                            fill_price=102.0,
                            side=side,
                            commission=0.0,
                            id_=1,
                        )
                        position = self.broker.get_position(SYMBOL)

                        # Create reverse order
                        rorder = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=reverse_side,
                            units=reverse_units,
                        )
                        if broker_exec_price == "next":  # rorder == order_id
                            rorder = self.order_manager.pending_orders[rorder]
                        open_order = rorder.open_order
                        close_order = rorder.close_order

                        self.assertIsInstance(rorder, ReverseOrder)
                        self.assertEqual(rorder.id, 2)
                        self.assertEqual(open_order.order_id, 2)
                        self.assertEqual(close_order.order_id, 2)
                        self.assertEqual(open_order.position_id, 2)
                        self.assertEqual(close_order.position_id, 1)
                        self.assertEqual(open_order.request, "open")
                        self.assertEqual(close_order.request, "close")
                        self.assertEqual(open_order.units, reverse_units - units)
                        self.assertEqual(close_order.units, position.units)
                        self.assertEqual(open_order.side, reverse_side)
                        self.assertEqual(close_order.side, reverse_side)
                        self.assertIn(open_order, self.order_manager.history)
                        self.assertIn(close_order, self.order_manager.history)
                    self.order_manager.reset()

    def test_net_acct_full_partial_close_order(self):
        # Fully/paritial closing of position by calling broker's buy/sell method
        self.broker.acct_mode = "netting"

        for broker_exec_price in ["current", "next"]:
            self.broker._exec_price = broker_exec_price
            for side, close_side in OrderSide, [OrderSide.SELL, OrderSide.BUY]:
                for close_units in [100, 50]:
                    with self.subTest(
                        msg=broker_exec_price, open=side.value, close=close_side.value
                    ):
                        # Increase current order id value to next value by placing an order
                        _ = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=side,
                            units=100,
                        )

                        # Create an existing position
                        self.broker.get_position.return_value = Position(
                            timestamp=TIMESTAMP,
                            symbol=SYMBOL,
                            units=100,
                            fill_price=102.0,
                            side=side,
                            commission=0.0,
                            id_=1,
                        )
                        position = self.broker.get_position(SYMBOL)

                        # Create close order
                        order = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=close_side,
                            units=close_units,
                        )
                        if broker_exec_price == "next":  # order == order_id
                            order = self.order_manager.pending_orders[order]

                        self.assertIsInstance(order, Order)
                        self.assertEqual(order.order_id, 2)
                        self.assertEqual(order.position_id, position.id)
                        self.assertEqual(order.side, close_side)
                        self.assertEqual(order.request, "close")
                    self.order_manager.reset()

    def test_close_order_request(self):
        # Closing a position by directly calling the broker's close method
        for acct_mode in ["netting", "hedging"]:
            self.broker.acct_mode = acct_mode
            for broker_exec_price in ["current", "next"]:
                self.broker._exec_price = broker_exec_price
                for open_side, close_side in OrderSide, [OrderSide.SELL, OrderSide.BUY]:
                    with self.subTest(
                        msg=f"{acct_mode}_{broker_exec_price}",
                        open_side=open_side.value,
                        close_side=close_side.value,
                    ):
                        # Increase current order id value to next value
                        _ = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=open_side,
                            units=100,
                        )

                        # Create an existing position
                        self.broker.get_position.return_value = Position(
                            timestamp=TIMESTAMP,
                            symbol=SYMBOL,
                            units=100,
                            fill_price=102.0,
                            side=open_side,
                            commission=0.0,
                            id_=1,
                        )
                        position = self.broker.get_position(SYMBOL)

                        # Create close order
                        order = self.order_manager.create_order(
                            symbol=SYMBOL,
                            order_type=OrderType.MARKET,
                            side=close_side,
                            position_id=position.id,
                        )
                        if broker_exec_price == "next":  # order == order_id
                            order = self.order_manager.pending_orders[order]

                        self.assertIsInstance(order, Order)
                        self.assertEqual(order.order_id, 2)
                        self.assertEqual(order.position_id, position.id)
                        self.assertEqual(order.side, close_side)
                        self.assertEqual(order.request, "close")
                    self.order_manager.reset()


class TestOrderManagerErrors(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = Mock()
        self.broker.data_handler.current_datetime = TIMESTAMP
        self.broker.data_handler.get_latest_price.return_value = 102.0
        self.order_manager = OrderManager(broker=self.broker)

    def test_invalid_order(self):
        for side in OrderSide:
            with self.subTest(side):
                with self.assertRaises(OrderError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type="invalid_order_type",  # type: ignore
                        units=100,
                        side=side,
                    )

    def test_buy_stop_error(self):
        for stp_price in [None, 102.0, 100.0]:
            with self.subTest(stp_price=stp_price):
                with self.assertRaises(StopOrderError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        units=100,
                        side=OrderSide.BUY,
                        price=stp_price,
                    )

    def test_sell_stop_error(self):
        for stp_price in [None, 102.0, 104.0]:
            with self.subTest(stp_price=stp_price):
                with self.assertRaises(StopOrderError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.STOP,
                        units=100,
                        side=OrderSide.SELL,
                        price=stp_price,
                    )

    def test_buy_limit_error(self):
        for msg, lmt_price in zip(
            ["No price", "Equal current price", "Above current price"],
            [None, 102.0, 104.0],
        ):
            with self.subTest(msg=msg, lmt_price=lmt_price):
                with self.assertRaises(LimitOrderError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        units=100,
                        side=OrderSide.BUY,
                        price=lmt_price,
                    )

    def test_sell_limit_error(self):
        for msg, lmt_price in zip(
            ["No price", "Equal current price", "Below current price"],
            [None, 102.0, 100.0],
        ):
            with self.subTest(msg=msg, lmt_price=lmt_price):
                with self.assertRaises(LimitOrderError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=OrderType.LIMIT,
                        units=100,
                        side=OrderSide.SELL,
                        price=lmt_price,
                    )

    def test_buy_sl_price_error(self):
        for order_type, price in zip(OrderType, [None, 101.0, 103.0]):
            with self.subTest(f"{order_type}: sl equals current price"):
                with self.assertRaises(StopLossPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.BUY,
                        price=price,
                        sl=(
                            self.order_manager.broker.data_handler.get_latest_price(
                                SYMBOL
                            )
                            if price is None
                            else price
                        ),
                    )

            with self.subTest(f"{order_type}: sl is greater than price"):
                with self.assertRaises(StopLossPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.BUY,
                        price=price,
                        sl=105.0,
                    )

    def test__buy_tp_price_error(self):
        for order_type, price in zip(OrderType, [None, 101.0, 103.0]):
            with self.subTest(f"{order_type}: tp equals price"):
                with self.assertRaises(TakeProfitPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.BUY,
                        price=price,
                        tp=(
                            self.order_manager.broker.data_handler.get_latest_price(
                                SYMBOL
                            )
                            if price is None
                            else price
                        ),
                    )

            with self.subTest(f"{order_type}: tp is less than price"):
                with self.assertRaises(TakeProfitPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.BUY,
                        price=price,
                        tp=100.0,
                    )

    def test__sell_sl_price_error(self):
        for order_type, price in zip(OrderType, [None, 103.0, 101.0]):
            with self.subTest(f"{order_type}: sl equals price"):
                with self.assertRaises(StopLossPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.SELL,
                        price=price,
                        sl=(
                            self.order_manager.broker.data_handler.get_latest_price(
                                SYMBOL
                            )
                            if price is None
                            else price
                        ),
                    )

            with self.subTest(f"{order_type}: sl is less than price"):
                with self.assertRaises(StopLossPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.SELL,
                        price=price,
                        sl=100.0,
                    )

    def test__sell_tp_price_error(self):
        for order_type, price in zip(OrderType, [None, 103.0, 101.0]):
            with self.subTest(f"{order_type}: tp equals price"):
                with self.assertRaises(TakeProfitPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.SELL,
                        price=price,
                        tp=(
                            self.order_manager.broker.data_handler.get_latest_price(
                                SYMBOL
                            )
                            if price is None
                            else price
                        ),
                    )

            with self.subTest(f"{order_type}: tp is greater than price"):
                with self.assertRaises(TakeProfitPriceError):
                    self.order_manager.create_order(
                        symbol=SYMBOL,
                        order_type=order_type,
                        units=100,
                        side=OrderSide.SELL,
                        price=price,
                        tp=105.0,
                    )


if __name__ == "__main__":
    unittest.main()
