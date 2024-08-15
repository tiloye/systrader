from enum import Enum


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class OrderType(Enum):
    MARKET = "mkt"
    LIMIT = "lmt"
    STOP = "stp"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class OrderStatus(Enum):
    EXECUTED = "executed"
    PENDING = "pending"
    REJECTED = "rejected"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
