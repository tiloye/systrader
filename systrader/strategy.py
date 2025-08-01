from abc import ABC, abstractmethod

from systrader.event import EventListener


class Strategy(ABC, EventListener):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    The goal of a (derived) Strategy object is to generate Order
    objects for particular symbols based on the inputs of Bars
    (OLHCVI) generated by a DataHandler object.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source,
    since it obtains the bar tuples from a queue object.
    """

    def __init__(self, symbols):
        self.symbols = symbols

    @abstractmethod
    def on_market(self):
        """
        This method is called whenever a market event occurs. The order generation
        process should be implemented here.
        """
        raise NotImplementedError("Should implement on_market()")

    def on_fill(self, event):
        """
        This method is called whenever a fill event occurs.
        """
        pass

    def on_order(self, event):
        """
        This method is called whenever an order event occurs.
        """
        pass

    def add_data_handler(self, data_handler):
        self.data_handler = data_handler

    def add_broker(self, broker):
        self.broker = broker

    def update(self, event=None):
        if event is None:
            self.on_market()
        elif type(event).__name__ == "Order":
            self.on_order(event)
        else:
            self.on_fill(event)
