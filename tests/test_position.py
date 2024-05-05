import unittest
from margin_trader.broker.sim_broker import Position

class TestPosition(unittest.TestCase):

    def test_init(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY"
        )
        self.assertEqual(position.symbol, "AAPL")
        self.assertEqual(position.units, 100)
        self.assertEqual(position.fill_price, 150.0)
        self.assertEqual(position.last_price, 150.0)
        self.assertEqual(position.commission, 0.5)
        self.assertEqual(position.side, "BUY")
        self.assertEqual(position._cost, 15000.0)
        self.assertEqual(position.pnl, 0)

    def test_update_last_price(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY"
        )
        new_price = 160.0
        position.update_last_price(new_price)
        self.assertEqual(position.last_price, new_price)

    def test_update_pnl_buy(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY"
        )
        price_1 = 160.0 # In profit
        price_2 = 140.0 # In loss

        position.update(price_1)
        self.assertEqual(position.pnl, 999.5)

        position.update(price_2)
        self.assertEqual(position.pnl, -1000.5)

    def test_update_pnl_sell(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "SELL"
        )
        price_1 = 140.0 # In profit
        price_2 = 160.0 # In loss

        position.update(price_1)
        self.assertEqual(position.pnl, 999.5)

        position.update(price_2)
        self.assertEqual(position.pnl, -1000.5)

    def test_pnl_on_buy_close(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY"
        )
        price_1 = 160.0 # In profit
        price_2 = 140.0 # In loss
        position.commission += position.commission

        position.update(price_1)
        self.assertEqual(position.pnl, 999.0)

        position.update(price_2)
        self.assertEqual(position.pnl, -1001.0)

    def test_pnl_on_sell_close(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "SELL"
        )
        price_1 = 140.0 # In profit
        price_2 = 160.0 # In loss
        position.commission += position.commission

        position.update(price_1)
        self.assertEqual(position.pnl, 999.0)

        position.update(price_2)
        self.assertEqual(position.pnl, -1001.0)

    def test_get_cost(self):
        position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY"
        )
        self.assertEqual(position.get_cost(), 15000.0)

if __name__ == "__main__":
    unittest.main()
