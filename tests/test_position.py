import unittest
from margin_trader.broker.sim_broker import Position

class TestPosition(unittest.TestCase):

    def setUp(self):
        self.position = Position(
            "2024-05-06", "AAPL", 100, 150.0, 0.5, "BUY", 0
        )

    def test_init(self):
        self.assertEqual(self.position.symbol, "AAPL")
        self.assertEqual(self.position.units, 100)
        self.assertEqual(self.position.fill_price, 150.0)
        self.assertEqual(self.position.last_price, 150.0)
        self.assertEqual(self.position.commission, 0.5)
        self.assertEqual(self.position.side, "BUY")
        self.assertEqual(self.position.pnl, 0)
        self.assertEqual(self.position.id, 0)

    def test_update_last_price(self):
        new_price = 160.0
        self.position.update_last_price(new_price)
        self.assertEqual(self.position.last_price, new_price)

    def test_update_pnl_buy(self):
        price_1 = 160.0 # In profit
        price_2 = 140.0 # In loss

        self.position.update(price_1)
        self.assertEqual(self.position.pnl, 999.5)

        self.position.update(price_2)
        self.assertEqual(self.position.pnl, -1000.5)

    def test_update_pnl_sell(self):
        self.position.side = "SELL"
        price_1 = 140.0 # In profit
        price_2 = 160.0 # In loss

        self.position.update(price_1)
        self.assertEqual(self.position.pnl, 999.5)

        self.position.update(price_2)
        self.assertEqual(self.position.pnl, -1000.5)

    def test_pnl_on_buy_close(self):
        price_1 = 160.0 # In profit
        price_2 = 140.0 # In loss
        self.position.commission += self.position.commission

        self.position.update(price_1)
        self.assertEqual(self.position.pnl, 999.0)

        self.position.update(price_2)
        self.assertEqual(self.position.pnl, -1001.0)

    def test_pnl_on_sell_close(self):
        self.position.side = "SELL"
        price_1 = 140.0 # In profit
        price_2 = 160.0 # In loss
        self.position.commission += self.position.commission

        self.position.update(price_1)
        self.assertEqual(self.position.pnl, 999.0)

        self.position.update(price_2)
        self.assertEqual(self.position.pnl, -1001.0)

    def test_get_cost(self):
        self.assertEqual(self.position.get_cost(), 15000.0)

    def test_update_close_time(self):
        self.position.update_close_time("2024-05-07")
        self.assertEqual(self.position.close_time, "2024-05-07")

if __name__ == "__main__":
    unittest.main()
