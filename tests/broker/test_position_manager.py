import datetime as dt
import unittest
from unittest.mock import patch

from margin_trader.broker.fill import Fill
from margin_trader.broker.position import (
    HedgePositionManager,
    NetPositionManager,
    Position,
)
from margin_trader.constants import OrderSide

MOCK_SOURCE = "margin_trader.broker.sim_broker.SimBroker"
SYMBOL = "SYMBOL1"


class TestNetPositionManager(unittest.TestCase):
    @patch(MOCK_SOURCE)
    def setUp(self, mock_broker):
        mock_broker.data_handler.get_latest_price.return_value = 160.0

        self.manager = NetPositionManager(broker=mock_broker)
        self.event = Fill(
            timestamp=dt.datetime(2024, 5, 6),
            symbol=SYMBOL,
            units=100,
            side=OrderSide.BUY,
            fill_price=150.0,
            commission=0.5,
            result="open",
            order_id=1,
        )
        self.position = Position(
            self.event.timestamp,
            self.event.symbol,
            self.event.units,
            self.event.fill_price,
            self.event.commission,
            self.event.side,
            self.event.order_id,
        )

    def test_init(self):
        self.assertDictEqual(self.manager.positions, {})
        self.assertListEqual(self.manager.history, [])

    def test_open_new_position(self):
        self.manager.update_position_on_fill(self.event)
        self.assertIn(self.position.symbol, self.manager.positions)

    def test_add_to_existing_position(self):
        self.manager.update_position_on_fill(self.event)
        new_event = Fill(
            timestamp=dt.datetime(2024, 5, 7),
            symbol=SYMBOL,
            units=50,
            side=OrderSide.BUY,
            fill_price=160.0,
            commission=0.5,
            result="open",
            order_id=2,
        )
        self.manager.update_position_on_fill(new_event)
        position = self.manager.positions[SYMBOL]

        self.assertEqual(position.units, 150.0)
        self.assertAlmostEqual(position.fill_price, 153.33, 2)

    def test_close_position_by_a_close_order(self):
        self.manager.update_position_on_fill(self.event)
        new_fill = self.event
        new_fill.timestamp = dt.datetime(2024, 5, 7)
        new_fill.side = OrderSide.SELL
        new_fill.result = "close"
        self.manager.update_position_on_fill(new_fill)
        closed_pos = self.manager.history[-1]

        self.assertNotIn(self.position.symbol, self.manager.positions)
        self.assertEqual(len(self.manager.history), 1)
        self.assertEqual(closed_pos.symbol, self.position.symbol)
        self.assertEqual(closed_pos.commission, 2 * self.position.commission)
        self.assertEqual(closed_pos.close_time, new_fill.timestamp)

    def test_close_position_by_an_open_order(self):
        self.manager.update_position_on_fill(self.event)
        new_fill = self.event
        new_fill.timestamp = dt.datetime(2024, 5, 7)
        new_fill.side = OrderSide.SELL
        new_fill.result = "open"
        self.manager.update_position_on_fill(new_fill)
        closed_pos = self.manager.history[-1]

        self.assertNotIn(self.position.symbol, self.manager.positions)
        self.assertEqual(len(self.manager.history), 1)
        self.assertEqual(closed_pos.symbol, self.position.symbol)
        self.assertEqual(closed_pos.commission, 2 * self.position.commission)
        self.assertEqual(closed_pos.close_time, new_fill.timestamp)

    def test_partial_close(self):
        self.manager.update_position_on_fill(self.event)
        new_event = Fill(
            timestamp=dt.datetime(2024, 5, 7),
            symbol=SYMBOL,
            units=50,
            side=OrderSide.SELL,
            fill_price=160.0,
            commission=0.5,
            result="close",
            order_id=0,
        )
        self.manager.update_position_on_fill(new_event)
        position = self.manager.positions[SYMBOL]
        hist_partial_close = self.manager.history[-1]

        self.assertEqual(position.units, 50)
        self.assertEqual(position.pnl, 499.5)
        self.assertEqual(hist_partial_close.units, 50)
        self.assertEqual(hist_partial_close.pnl, 499.0)

    def test_update_position_on_market(self):
        self.manager.update_position_on_fill(self.event)
        self.manager.update_position_on_market()
        self.assertEqual(self.manager.positions[self.event.symbol].pnl, 999.5)

    def test_get_total_pnl(self):
        s1_event = self.event
        s2_event = Fill(
            timestamp=dt.datetime(2024, 5, 6),
            symbol="SYMBOL2",
            units=100,
            side=OrderSide.BUY,
            fill_price=150.0,
            commission=0.5,
            result="open",
            order_id=1,
        )
        self.manager.update_position_on_fill(s1_event)
        self.manager.update_position_on_fill(s2_event)
        self.manager.update_position_on_market()

        total_pnl = self.manager.get_total_pnl()
        self.assertEqual(total_pnl, 1999.0)

    def test_get_position(self):
        self.manager.update_position_on_fill(self.event)
        self.assertEqual(self.manager.get_position(SYMBOL), self.position)


class TestHedgePositionManager(unittest.TestCase):
    @patch(MOCK_SOURCE)
    def setUp(self, mock_broker):
        mock_broker.data_handler.get_latest_price.return_value = 160.0

        self.manager = HedgePositionManager(broker=mock_broker)
        self.event = Fill(
            timestamp=dt.datetime(2024, 5, 6),
            symbol=SYMBOL,
            units=100,
            side=OrderSide.BUY,
            fill_price=150.0,
            commission=0.5,
            result="open",
            order_id=1,
        )
        self.position = Position(
            self.event.timestamp,
            self.event.symbol,
            self.event.units,
            self.event.fill_price,
            self.event.commission,
            self.event.side,
            self.event.order_id,
        )

    def test_open_new_position(self):
        """Checks that new position was added to the dictionary of open positions"""
        self.manager.update_position_on_fill(self.event)
        self.assertIn(self.position.id, self.manager.positions)
        self.assertIn(self.position.symbol, self.manager.position_grp)
        self.assertIn(self.position.id, self.manager.position_grp[self.event.symbol])

    def test_add_new_position_for_same_symbol(self):
        self.manager.update_position_on_fill(self.event)
        new_event = Fill(
            timestamp=dt.datetime(2024, 5, 7),
            symbol=SYMBOL,
            units=50,
            side=OrderSide.BUY,
            fill_price=160.0,
            commission=0.5,
            result="open",
            order_id=2,
        )
        self.manager.update_position_on_fill(new_event)

        self.assertEqual(len(self.manager.positions), 2)
        self.assertIn(new_event.order_id, self.manager.positions)
        self.assertIn(new_event.order_id, self.manager.position_grp[new_event.symbol])

    def test_close_position(self):
        self.manager.update_position_on_fill(self.event)
        new_fill = Fill(
            timestamp=dt.datetime(2024, 5, 7),
            symbol=SYMBOL,
            units=self.position.units,
            side=OrderSide.SELL,
            fill_price=160.0,
            commission=0.5,
            result="close",
            order_id=2,
            position_id=1,
        )
        self.manager.update_position_on_fill(new_fill)
        closed_pos = self.manager.history[-1]

        self.assertNotIn(self.position.id, self.manager.positions)
        self.assertNotIn(self.position.symbol, self.manager.position_grp)
        self.assertEqual(len(self.manager.history), 1)
        self.assertEqual(closed_pos.id, self.position.id)
        self.assertEqual(closed_pos.commission, 2 * self.position.commission)
        self.assertEqual(closed_pos.close_time, new_fill.timestamp)

    def test_get_position(self):
        self.manager.update_position_on_fill(self.event)
        self.assertIsInstance(self.manager.get_position(SYMBOL), list)
        self.assertEqual(self.manager.get_position(1), self.position)


if __name__ == "__main__":
    unittest.main()
