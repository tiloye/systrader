import unittest
from margin_trader.event import FillEvent
from margin_trader.broker.sim_broker import Position
from margin_trader.broker.sim_broker import PositionManager

class TestPositionManager(unittest.TestCase):

    def setUp(self):
        self.manager = PositionManager()
        self.event = FillEvent(
            timeindex="2024-05-06",
            symbol="AAPL",
            units=100,
            side="BUY",
            fill_price=150.0,
            commission=0.5
        )
        self.position = Position(
            self.event.timeindex,
            self.event.symbol,
            self.event.units,
            self.event.fill_price,
            self.event.commission,
            self.event.side
        )

    def test_init(self):
        self.assertDictEqual(self.manager.positions, {})
        self.assertListEqual(self.manager.history, [])
    
    def test_update_position_from_fill_open(self):
        self.manager.update_position_from_fill(self.event)
        self.assertIn(self.position.symbol, self.manager.positions)

    def test_update_position_from_fill_close(self):
        self.manager.update_position_from_fill(self.event)
        new_fill = self.event
        new_fill.timeindex = "2024-05-07"
        new_fill.result = "close"
        self.manager.update_position_from_fill(new_fill)
        closed_pos = self.manager.history[-1]

        self.assertNotIn(self.position.symbol, self.manager.positions)
        self.assertEqual(len(self.manager.history), 1)
        self.assertEqual(closed_pos.symbol, self.position.symbol)
        self.assertEqual(closed_pos.commission, 2 * self.position.commission)
        self.assertEqual(closed_pos.close_time, "2024-05-07")

    def test_update_pnl(self):
        self.manager.update_position_from_fill(self.event)
        symbol = self.event.symbol
        new_price = 160.0
        self.manager.update_pnl(symbol, new_price)
        self.assertEqual(self.manager.positions[symbol].pnl, 999.5)

    def test_get_total_pnl(self):
        # AAPL position
        aapl_event = self.event
        self.manager.update_position_from_fill(aapl_event)
        aapl_new_price = 160.0
        self.manager.update_pnl(aapl_event.symbol, aapl_new_price)

        # MSFT position
        msft_event = self.event
        msft_event.symbol = "MSFT"
        msft_event.fill_price = 80.0
        self.manager.update_position_from_fill(msft_event)
        msft_new_price = 85.0
        self.manager.update_pnl(msft_event.symbol, msft_new_price)

        total_pnl = self.manager.get_total_pnl()
        self.assertEqual(total_pnl, 1499.0)


if __name__ == "__main__":
    unittest.main()