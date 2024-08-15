import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import Mock

from margin_trader.event import (
    FILLEVENT,
    MARKETEVENT,
    ORDEREVENT,
    EventListener,
    EventManager,
)


class MockListener(EventListener):
    def update(self, event=None):
        print(event, end="") if event else print("market_event", end="")


class TestEventManager(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_listener = MockListener()

    def test_subsribe(self):
        event_manager = EventManager()
        event_manager.subscribe(MARKETEVENT, self.mock_listener)
        event_manager.subscribe(ORDEREVENT, self.mock_listener)
        event_manager.subscribe(FILLEVENT, self.mock_listener)

        self.assertEqual(
            event_manager.listeners,
            {
                MARKETEVENT: [self.mock_listener],
                ORDEREVENT: [self.mock_listener],
                FILLEVENT: [self.mock_listener],
            },
        )

    def test_notify(self):
        event_manager = EventManager()
        event_manager.subscribe(MARKETEVENT, self.mock_listener)
        event_manager.subscribe(ORDEREVENT, self.mock_listener)
        event_manager.subscribe(FILLEVENT, self.mock_listener)

        for event_type, event in zip(
            [MARKETEVENT, ORDEREVENT, FILLEVENT],
            ["market_event", "order_event", "fill_event"],
        ):
            with self.subTest(event):
                with StringIO() as out, redirect_stdout(out):
                    event_manager.notify(event_type, event)
                    processed_event_outcome = out.getvalue()
                    self.assertEqual(event, processed_event_outcome)

    def test_unsubscribe(self):
        event_manager = EventManager()
        event_manager.subscribe(MARKETEVENT, self.mock_listener)
        event_manager.unsubscribe(MARKETEVENT, self.mock_listener)

        self.assertEqual(event_manager.listeners, {MARKETEVENT: []})

    def test_notify_nonexistent_event(self):
        event_manager = EventManager()
        mock_listener = Mock()

        event_type = "nonexistent_event"
        event_data = "test_data"
        event_manager.notify(event_type, event_data)

        mock_listener.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
