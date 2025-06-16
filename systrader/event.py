from collections import defaultdict

MARKETEVENT = "market"
ORDEREVENT = "order"
FILLEVENT = "fill"


class EventManager:
    """
    Manages event subscriptions and notifications.
    """

    def __init__(self):
        self.listeners = defaultdict(
            list
        )  # Dictionary to store event type and its listeners

    def subscribe(self, event_type, listener):
        """
        Subscribes a listener to a specific event type.
        """
        self.listeners[event_type].append(listener)

    def unsubscribe(self, event_type, listener):
        """
        Unsubscribes a listener from a specific event type.
        """
        if event_type in self.listeners:
            self.listeners[event_type].remove(listener)

    def notify(self, event_type, event=None):
        """
        Notifies all subscribed listeners for a specific event type.
        """
        for listener in self.listeners[event_type]:
            listener.update(event)


class EventListener:
    """
    An interface for event listeners.
    """

    def update(self, event=None):
        """
        Update event listeners with the event class.

        parameters
        ----------
        event: None or Order or Fill
            The event the listener should act on. If it is None, then it should be
            assumed to be a market event.
        """
        raise NotImplementedError("Subclasses must implement update method")
