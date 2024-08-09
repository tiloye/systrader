import unittest

from margin_trader.errors import (
    LimitOrderError,
    MarketOrderError,
    StopLossPriceError,
    StopOrderError,
    TakeProfitPriceError,
)


class TestOrderErrors(unittest.TestCase):
    def test_limit_order_error(self):
        with self.assertRaises(LimitOrderError) as e:
            raise LimitOrderError("Test limit order error")
        self.assertEqual(str(e.exception), "Test limit order error")

    def test_market_order_error(self):
        with self.assertRaises(MarketOrderError) as e:
            raise MarketOrderError("Test market order error")
        self.assertEqual(str(e.exception), "Test market order error")

    def test_stop_order_error(self):
        with self.assertRaises(StopOrderError) as e:
            raise StopOrderError("Test stop order error")
        self.assertEqual(str(e.exception), "Test stop order error")

    def test_stop_loss_error(self):
        with self.assertRaises(StopLossPriceError) as e:
            raise StopLossPriceError("Stop loss price must be below entry price")
        self.assertEqual(str(e.exception), "Stop loss price must be below entry price")

    def test_take_profit_error(self):
        with self.assertRaises(TakeProfitPriceError) as e:
            raise TakeProfitPriceError("Take profit price must be above entry price")
        self.assertEqual(
            str(e.exception), "Take profit price must be above entry price"
        )


if __name__ == "__main__":
    unittest.main()
