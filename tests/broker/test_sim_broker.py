import unittest
from itertools import product
from pathlib import Path

import pandas as pd

from systrader.broker.order import OrderManager
from systrader.broker.position import HedgePositionManager, NetPositionManager
from systrader.broker.sim_broker import SimBroker
from systrader.constants import OrderSide, OrderStatus, OrderType
from systrader.datahandler import HistoricCSVDataHandler
from systrader.event import FILLEVENT, MARKETEVENT, ORDEREVENT, EventManager

CSV_DIR = Path(__file__).parent.parent.joinpath("data")


class TestSimBroker(unittest.TestCase):
    def create_broker(
        self,
        balance=100_000.0,
        acct_mode="netting",
        leverage=1,
        commission=0.0,
        stop_out_level=0.2,
        trading_price="close",
        listener=None,
    ):
        broker = SimBroker(
            balance=balance,
            acct_mode=acct_mode,
            leverage=leverage,
            commission=commission,
            stop_out_level=stop_out_level,
            trading_price=trading_price,
        )
        broker_event_manager = EventManager()
        broker.add_event_manager(broker_event_manager)
        if listener:
            broker.event_manager.subscribe(ORDEREVENT, listener)
            broker.event_manager.subscribe(FILLEVENT, listener)
        return broker

    def create_data_handler(self, symbol):
        if isinstance(symbol, str):
            symbol = [symbol]

        data_event_manager = EventManager()
        data_handler = HistoricCSVDataHandler(csv_dir=CSV_DIR, symbols=symbol)
        data_handler.add_event_manager(data_event_manager)
        return data_handler

    def setup_data_handler_and_broker(self, data_handler_kwargs, broker_kwargs):
        data_handler = self.create_data_handler(**data_handler_kwargs)
        broker = self.create_broker(**broker_kwargs)
        broker.add_data_handler(data_handler)
        data_handler.event_manager.subscribe(MARKETEVENT, broker)
        return data_handler, broker

    def test_init(self):
        for acct_mode, trading_price in product(
            ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(acct_mode, trading_price=trading_price):
                data_handler_kwargs = {"symbol": "SYMBOL1"}
                broker_kwargs = {
                    "acct_mode": acct_mode,
                    "trading_price": trading_price,
                }
                _, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )

                self.assertEqual(broker.balance, 100_000.0)
                self.assertEqual(broker.equity, 100_000.0)
                self.assertEqual(broker.free_margin, 100_000.0)
                self.assertIsInstance(broker.data_handler, HistoricCSVDataHandler)
                self.assertEqual(broker.leverage, 1)
                self.assertEqual(broker.commission, 0.0)
                self.assertEqual(broker._trading_price, trading_price)
                self.assertListEqual(broker.account_history, [])
                self.assertIsInstance(broker._order_manager, OrderManager)
                if acct_mode == "netting":
                    self.assertIsInstance(broker._p_manager, NetPositionManager)
                else:
                    self.assertIsInstance(broker._p_manager, HedgePositionManager)

    def test_reverse_order_execution_on_close(self):
        for side in OrderSide:
            symbol = "SYMBOL1"
            data_handler_kwargs = {"symbol": symbol}
            broker_kwargs = {"acct_mode": "netting", "balance": 12000}
            data_handler, broker = self.setup_data_handler_and_broker(
                data_handler_kwargs, broker_kwargs
            )

            data_handler.update_bars()
            if side == OrderSide.BUY:
                broker.buy(symbol=symbol, order_type=OrderType.MARKET)
            else:
                broker.sell(symbol=symbol, order_type=OrderType.MARKET)

            with self.subTest(f"{side}: initial order"):
                self.assertEqual(broker.balance, 12000.0)
                self.assertEqual(broker.equity, 12000.0)
                self.assertEqual(broker.free_margin, 1800.0)

            data_handler.update_bars()
            if side == OrderSide.BUY:
                broker.sell(symbol="SYMBOL1", units=200, order_type=OrderType.MARKET)
            else:
                broker.buy(symbol="SYMBOL1", units=200, order_type=OrderType.MARKET)

            with self.subTest(f"{side}: reverse order"):
                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 12400.0)
                    self.assertEqual(broker.equity, 12400.0)
                    self.assertEqual(broker.free_margin, 1800)
                else:
                    self.assertEqual(broker.balance, 11600.0)
                    self.assertEqual(broker.equity, 11600.0)
                    self.assertEqual(broker.free_margin, 1000)

    def test_reverse_order_execution_on_open(self):
        for side in OrderSide:
            symbol = "SYMBOL1"
            data_handler_kwargs = {"symbol": symbol}
            broker_kwargs = {
                "acct_mode": "netting",
                "balance": 12000,
                "trading_price": "open",
            }
            data_handler, broker = self.setup_data_handler_and_broker(
                data_handler_kwargs, broker_kwargs
            )

            data_handler.update_bars()
            if side == OrderSide.BUY:
                broker.buy(symbol=symbol, order_type=OrderType.MARKET)
            else:
                broker.sell(symbol=symbol, order_type=OrderType.MARKET)

            with self.subTest(f"{side}: initial order"):
                self.assertEqual(broker.balance, 12000.0)
                self.assertEqual(broker.equity, 12000.0)
                self.assertEqual(broker.free_margin, 2000.0)

            data_handler.update_bars()
            if side == OrderSide.BUY:
                broker.sell(symbol=symbol, units=200, order_type=OrderType.MARKET)
            else:
                broker.buy(symbol=symbol, units=200, order_type=OrderType.MARKET)

            with self.subTest(f"{side}: reverse order"):
                if side == OrderSide.BUY:
                    self.assertEqual(broker.balance, 12200.0)
                    self.assertEqual(broker.equity, 12200.0)
                    self.assertEqual(broker.free_margin, 2000)
                else:
                    self.assertEqual(broker.balance, 11800.0)
                    self.assertEqual(broker.equity, 11800.0)
                    self.assertEqual(broker.free_margin, 1600)

    def test_close_buy_sell_position(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.MARKET)
                data_handler.update_bars()

                if acct_mode == "netting":
                    position = broker._p_manager.positions[symbol]
                else:
                    position = broker._p_manager.positions[1]
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
        symbol = "SYMBOL1"
        data_handler_kwargs = {"symbol": symbol}
        broker_kwargs = {"acct_mode": "hedging"}
        data_handler, broker = self.setup_data_handler_and_broker(
            data_handler_kwargs, broker_kwargs
        )
        data_handler.update_bars()

        broker.buy(symbol=symbol)
        broker.buy(symbol=symbol)
        broker.buy(symbol=symbol)

        with self.subTest("Open positions == 3"):
            positions = broker._p_manager.positions
            self.assertEqual(len(positions), 3)

        broker.close_all_positions()
        with self.subTest("Closed all positions"):
            positions = broker.get_positions()
            self.assertEqual(len(positions), 0)

    def test_buy_sell_mkt_order_rejected(self):
        for side, acct_mode in product(OrderSide, ["netting", "hedging"]):
            with self.subTest(side, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "balance": 10000}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.MARKET)

                order = broker._order_manager.history[-1]

                self.assertEqual(order.status, OrderStatus.REJECTED)

    def test_buy_sell_mkt_order_execution(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":  # Create an order on next bar open
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.MARKET)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.MARKET)

                self.assertIn(
                    "SYMBOL1" if acct_mode == "netting" else 1,
                    broker._p_manager.positions,
                )
                self.assertEqual(broker.balance, 100_000.0)
                self.assertEqual(broker.equity, 100_000.0)
                self.assertEqual(broker.free_margin, 89_800.0)

    def assert_cover_order_execution(self, broker, expected_balance):
        porder, corder = broker._order_manager.history[-2:]
        pending_orders = broker._order_manager.pending_orders
        positions = broker._p_manager.positions

        self.assertEqual(porder.status, OrderStatus.EXECUTED)
        self.assertEqual(corder.status, OrderStatus.EXECUTED)
        self.assertNotIn(porder, pending_orders)
        self.assertNotIn(
            porder.symbol if broker.acct_mode == "netting" else 1,
            positions,
        )
        self.assertEqual(broker.balance, expected_balance)
        self.assertEqual(broker.equity, broker.balance)

    def test_buy_sell_mkt_cover_sl_triggered(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.MARKET, sl=100.0)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.MARKET, sl=104.0)

                if trading_price == "close":
                    # SL should trigger on next bar when trading a close
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 99_800.0)

    def test_buy_sell_mkt_cover_tp_triggered(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == "buy":
                    broker.buy(symbol=symbol, order_type=OrderType.MARKET, tp=104.0)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.MARKET, tp=100.0)

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 100_200)

    def assert_bracket_order_execution(
        self, broker, expected_balance, triggered_price="sl"
    ):
        porder, sl_order, tp_order = broker._order_manager.history[-3:]
        pending_orders = broker._order_manager.pending_orders
        positions = broker._p_manager.positions

        if triggered_price == "sl":
            self.assertEqual(sl_order.status, OrderStatus.EXECUTED)
            self.assertEqual(tp_order.status, OrderStatus.CANCELED)
        else:
            self.assertEqual(tp_order.status, OrderStatus.EXECUTED)
            self.assertEqual(sl_order.status, OrderStatus.CANCELED)
        self.assertEqual(porder.status, OrderStatus.EXECUTED)
        self.assertNotIn(porder, pending_orders)
        self.assertNotIn(
            porder.symbol if broker.acct_mode == "netting" else 1,
            positions,
        )
        self.assertEqual(broker.balance, expected_balance)
        self.assertEqual(broker.equity, broker.balance)

    def test_buy_sell_mkt_bracket_order_triggers_sl(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=101.0,
                        tp=103.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=103.0,
                        tp=101.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_900)

    def test_buy_sell_mkt_bracket_order_triggers_sl_on_future_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=98.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=106.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_600)

    def test_buy_sell_mkt_bracket_order_triggers_tp(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=99.0,
                        tp=104.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=105.0,
                        tp=100.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_200, "tp")

    def test_buy_sell_mkt_bracket_order_triggers_tp_on_future_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_kwargs = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_kwargs, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=97.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        sl=107.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_400.0, "tp")

    def test_buy_sell_lmt_order_execution(self):
        # Test execution of limit order when trading at open or close price of bars

        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.LIMIT, price=101.0)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.LIMIT, price=103.0)
                if trading_price == "close":
                    data_handler.update_bars()

                order = broker._order_manager.history[-1]
                pending_orders = broker._order_manager.pending_orders
                position = broker._p_manager.positions[
                    symbol if acct_mode == "netting" else order.order_id
                ]

                self.assertEqual(order.status, OrderStatus.EXECUTED)
                self.assertEqual(order.order_id, position.id)
                self.assertNotIn(order, pending_orders)
                self.assertEqual(broker.balance, 100_000)
                if side == OrderSide.BUY:
                    self.assertEqual(
                        broker.equity, 100_500 if trading_price == "close" else 100_000
                    )
                else:
                    self.assertEqual(
                        broker.equity, 99_700 if trading_price == "close" else 100_000
                    )

    def test_buy_sell_lmt_cover_sl_triggered_on_same_bar(self):
        # Test sl of limit order executed on same bar the limit order was executed on

        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=100.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=104.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 99_900)

    def test_buy_sell_lmt_cover_sl_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=98.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=106.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_cover_order_execution(broker, 99_700)

    def test_buy_sell_lmt_cover_tp_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == "buy":
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        tp=103.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        tp=101.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 100_200)

    def test_buy_sell_lmt_cover_tp_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        tp=105.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        tp=99.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_cover_order_execution(broker, 100_400)

    def test_buy_sell_lmt_bracket_sl_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )
                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=100.0,
                        tp=102.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=104.0,
                        tp=102.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_900)

    def test_buy_sell_lmt_bracket_sl_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=98.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=106.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_700)

    def test_buy_sell_lmt_bracket_tp_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=99.0,
                        tp=104.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=105.0,
                        tp=100.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_300, "tp")

    def test_buy_sell_lmt_bracket_tp_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=101.0,
                        sl=97.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.LIMIT,
                        price=103.0,
                        sl=107.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_500, "tp")

    def test_buy_sell_stp_order_execution(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(symbol=symbol, order_type=OrderType.STOP, price=103.0)
                else:
                    broker.sell(symbol=symbol, order_type=OrderType.STOP, price=101.0)

                if trading_price == "close":
                    data_handler.update_bars()

                order = broker._order_manager.history[-1]
                pending_orders = broker._order_manager.pending_orders
                position = broker._p_manager.positions[
                    symbol if acct_mode == "netting" else 1
                ]

                self.assertEqual(order.status, OrderStatus.EXECUTED)
                self.assertEqual(order.order_id, position.id)
                self.assertNotIn(order, pending_orders)
                self.assertEqual(broker.balance, 100_000)
                if side == OrderSide.BUY:
                    self.assertEqual(
                        broker.equity, 100_300 if trading_price == "close" else 100_000
                    )
                else:
                    self.assertEqual(
                        broker.equity, 99_500 if trading_price == "close" else 100_000
                    )

    def test_buy_sell_stp_cover_sl_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=100.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=104.0,
                    )
                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 99_700)

    def test_buy_sell_stp_cover_sl_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=98.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=106.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_cover_order_execution(broker, 99_500)

    def test_buy_sell_stp_cover_tp_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == "buy":
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        tp=104.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        tp=100.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_cover_order_execution(broker, 100_100)

    def test_buy_sell_stp_cover_tp_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_cover_order_execution(broker, 100_300)

    def test_buy_sell_stp_bracket_sl_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=100.0,
                        tp=104.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=104.0,
                        tp=100.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_700)

    def test_buy_sell_stp_bracket_sl_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=98.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=106.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 99_500)

    def test_buy_sell_stp_bracket_tp_triggered_on_same_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=99.0,
                        tp=104.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=105.0,
                        tp=100.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_100, "tp")

    def test_buy_sell_stp_bracket_tp_triggered_on_next_bar(self):
        for side, acct_mode, trading_price in product(
            OrderSide, ["netting", "hedging"], ["open", "close"]
        ):
            with self.subTest(side, trading_price=trading_price, acct_mode=acct_mode):
                symbol = "SYMBOL3"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode, "trading_price": trading_price}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )

                if trading_price == "open":
                    data_handler.update_bars()
                data_handler.update_bars()

                if side == OrderSide.BUY:
                    broker.buy(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=103.0,
                        sl=97.0,
                        tp=106.0,
                    )
                else:
                    broker.sell(
                        symbol=symbol,
                        order_type=OrderType.STOP,
                        price=101.0,
                        sl=107.0,
                        tp=98.0,
                    )

                if trading_price == "close":
                    data_handler.update_bars()
                data_handler.update_bars()

                self.assert_bracket_order_execution(broker, 100_300, "tp")

    def test_account_history_update(self):
        for acct_mode in ["netting", "hedging"]:
            with self.subTest(acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )
                data_handler.update_bars()
                broker.buy(symbol=symbol)

                for _ in range(4):
                    data_handler.update_bars()

                position = broker._p_manager.positions[
                    symbol if acct_mode == "netting" else 1
                ]
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
        for acct_mode in ["netting", "hedging"]:
            with self.subTest(acct_mode=acct_mode):
                symbol = "SYMBOL1"
                data_handler_arg = {"symbol": symbol}
                broker_kwargs = {"acct_mode": acct_mode}
                data_handler, broker = self.setup_data_handler_and_broker(
                    data_handler_arg, broker_kwargs
                )
                data_handler.update_bars()
                broker.buy(symbol=symbol)

                for _ in range(4):
                    data_handler.update_bars()

                position = broker._p_manager.positions[
                    symbol if acct_mode == "netting" else 1
                ]
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
                        "symbol": symbol,
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
                        "symbol": [symbol] * 2,
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


if __name__ == "__main__":
    unittest.main()
