"""
Regression tests for issue #3347: Stacked @card — duplicate id from a non-editable card
raises IndexError, or silently discards card content.
"""

import pytest
from unittest.mock import Mock, MagicMock

from metaflow.plugins.cards.component_serializer import CardComponentCollector
from metaflow.plugins.cards.card_modules.basic import MarkdownComponent


class MockLogger:
    """Mock logger to capture warnings."""

    def __init__(self):
        self.messages = []

    def __call__(self, msg, timestamp=False, bad=False):
        self.messages.append(msg)


@pytest.fixture
def logger():
    return MockLogger()


@pytest.fixture
def card_creator():
    """Mock card creator."""
    return Mock()


@pytest.fixture
def collector(logger, card_creator):
    """Create a CardComponentCollector with mocked dependencies."""
    return CardComponentCollector(logger=logger, card_creator=card_creator)


def test_duplicate_id_with_non_editable_card_no_longer_raises_index_error(
    collector, logger
):
    """
    Test case 1 from issue #3347:
    When duplicate ids exist across editable and non-editable cards,
    _finalize() used to raise IndexError. After fix, it should handle gracefully.

    Flow:
        @card(type="default_json", id="mycard")  # non-editable
        @card(type="blank")                      # editable, no id
        @card(type="blank", id="mycard")         # editable, duplicate id

    Expected: Since only ONE editable card has id="mycard", it should win.
    """
    # Add three cards: one non-editable with id, one editable without id, one editable with duplicate id
    collector._add_card(
        card_type="default_json",
        card_id="mycard",
        decorator_attributes={"type": "default_json"},
        card_options={},
        editable=False,  # default_json has ALLOW_USER_COMPONENTS=False
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    collector._add_card(
        card_type="blank",
        card_id=None,
        decorator_attributes={"type": "blank"},
        card_options={},
        editable=True,  # blank has ALLOW_USER_COMPONENTS=True
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    editable_card_with_id = collector._add_card(
        card_type="blank",
        card_id="mycard",  # duplicate id
        decorator_attributes={"type": "blank"},
        card_options={},
        editable=True,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )

    # After the fix, this should not raise IndexError
    collector._finalize()

    # Since only one editable card has "mycard", it should win
    assert "mycard" in collector._card_id_map
    assert collector._card_id_map["mycard"] == editable_card_with_id["uuid"]

    # The editable card should be accessible
    card_meta = collector._cards_meta[collector._card_id_map["mycard"]]
    assert card_meta["editable"] is True
    assert card_meta["type"] == "blank"


def test_silent_content_loss_with_single_editable_card(collector, logger):
    """
    Test case 2 from issue #3347:
    When there's exactly one editable card and a non-editable card shares its id,
    content appended to that id may be silently lost.

    Flow:
        @card(type="default_json", id="mycard")  # non-editable
        @card(type="blank", id="mycard")         # editable, duplicate id
        @step
        def start(self):
            current.card["mycard"].append(MarkdownComponent("# IMPORTANT"))
    """
    # Add two cards with duplicate id: one non-editable, one editable
    collector._add_card(
        card_type="default_json",
        card_id="mycard",
        decorator_attributes={"type": "default_json"},
        card_options={},
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    collector._add_card(
        card_type="blank",
        card_id="mycard",  # duplicate id
        decorator_attributes={"type": "blank"},
        card_options={},
        editable=True,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )

    # Finalize - with only 1 editable card, early return happens before duplicate check
    collector._finalize()

    # The _card_id_map should resolve "mycard" to one card, but which one?
    # If it resolves to the non-editable default_json, content will be lost
    # because non-editable cards don't accept user components

    # Verify that _finalize() returned early (only 1 editable card)
    assert collector._default_editable_card is not None

    # Check which card "mycard" resolves to
    card_uuid = collector._card_id_map.get("mycard")
    assert card_uuid is not None

    # Before the fix, this might resolve to the non-editable card,
    # silently discarding content. After the fix, it should either:
    # 1. Resolve to the editable card, or
    # 2. Be removed from _card_id_map with a warning


def test_duplicate_id_across_multiple_non_editable_cards(collector):
    """
    Additional test: Multiple non-editable cards with same id.
    """
    collector._add_card(
        card_type="default_json",
        card_id="mycard",
        decorator_attributes={"type": "default_json"},
        card_options={},
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    collector._add_card(
        card_type="taskspec_card",  # another non-editable card type
        card_id="mycard",
        decorator_attributes={"type": "taskspec_card"},
        card_options={},
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )

    # With no editable cards, _finalize should return early without error
    # But it should still validate ids if needed
    collector._finalize()

    # No default editable card should be set
    assert collector._default_editable_card is None


def test_multiple_editable_cards_with_same_id_drops_id(collector, logger):
    """
    When multiple editable cards have the same ID, the ID should be dropped with a warning.
    """
    collector._add_card(
        card_type="blank",
        card_id="mycard",
        decorator_attributes={"type": "blank"},
        card_options={},
        editable=True,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    collector._add_card(
        card_type="default",  # also editable
        card_id="mycard",
        decorator_attributes={"type": "default"},
        card_options={},
        editable=True,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )

    collector._finalize()

    # Multiple editable cards with same ID: should be dropped
    assert "mycard" not in collector._card_id_map

    # A warning should have been logged
    assert any("duplicate id" in msg.lower() for msg in logger.messages)


def test_unique_ids_work_correctly(collector):
    """
    Sanity check: unique ids should work fine.
    """
    collector._add_card(
        card_type="default_json",
        card_id="card1",
        decorator_attributes={"type": "default_json"},
        card_options={},
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )
    collector._add_card(
        card_type="blank",
        card_id="card2",
        decorator_attributes={"type": "blank"},
        card_options={},
        editable=True,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    )

    collector._finalize()

    # Both ids should be in the map
    assert "card1" in collector._card_id_map
    assert "card2" in collector._card_id_map
    # Default editable card should be set (only 1 editable card)
    assert collector._default_editable_card is not None
