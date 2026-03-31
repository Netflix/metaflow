import pytest

from metaflow.plugins.cards import card_server


class DummyTask:
    def __init__(self, pathspec, finished=False):
        self.pathspec = pathspec
        self.finished = finished


class DummyStep:
    def __init__(self, tasks):
        self._tasks = tasks

    def tasks(self):
        return iter(self._tasks)


class DummyRun:
    def __init__(self, steps):
        self._steps = steps

    def steps(self):
        return iter(self._steps)


@pytest.fixture
def card_mapping():
    return {
        "Flow/1/start/1": ["card-1", "card-2"],
        "Flow/1/end/2": ["card-3", "card-4"],
        "Flow/1/skip/3": ["card-5"],
    }


@pytest.fixture
def patch_cards_for_task(monkeypatch, card_mapping):
    def _cards_for_task(
        flow_datastore, task_pathspec, card_type=None, card_hash=None, card_id=None
    ):
        cards = card_mapping.get(task_pathspec)
        if cards is None:
            return None
        return iter(cards)

    monkeypatch.setattr(card_server, "cards_for_task", _cards_for_task)


def _build_run():
    return DummyRun(
        [
            DummyStep([DummyTask("Flow/1/start/1")]),
            DummyStep(
                [
                    DummyTask("Flow/1/end/2"),
                    DummyTask("Flow/1/skip/3", finished=True),
                ]
            ),
        ]
    )


@pytest.mark.parametrize(
    "max_cards,expected_cards",
    [
        (0, []),
        (1, ["card-1"]),
        (2, ["card-1", "card-2"]),
        (4, ["card-1", "card-2", "card-3", "card-4"]),
        (10, ["card-1", "card-2", "card-3", "card-4", "card-5"]),
    ],
)
def test_cards_for_run_yields_exactly_max_cards(
    patch_cards_for_task, max_cards, expected_cards
):
    cards = list(
        card_server.cards_for_run(
            None, _build_run(), only_running=False, max_cards=max_cards
        )
    )

    assert [card for _, card in cards] == expected_cards


def test_cards_for_run_preserves_pathspec_card_pairing_and_order(
    patch_cards_for_task,
):
    cards = list(
        card_server.cards_for_run(
            None, _build_run(), only_running=False, max_cards=10
        )
    )

    assert cards == [
        ("Flow/1/start/1", "card-1"),
        ("Flow/1/start/1", "card-2"),
        ("Flow/1/end/2", "card-3"),
        ("Flow/1/end/2", "card-4"),
        ("Flow/1/skip/3", "card-5"),
    ]


def test_cards_for_run_stops_cleanly_after_limit(patch_cards_for_task):
    card_iter = card_server.cards_for_run(
        None, _build_run(), only_running=False, max_cards=1
    )

    assert next(card_iter) == ("Flow/1/start/1", "card-1")
    with pytest.raises(StopIteration):
        next(card_iter)


def test_cards_for_run_skips_finished_tasks_when_only_running(patch_cards_for_task):
    cards = list(
        card_server.cards_for_run(
            None, _build_run(), only_running=True, max_cards=10
        )
    )

    assert [card for _, card in cards] == ["card-1", "card-2", "card-3", "card-4"]


def test_cards_for_run_skips_tasks_without_cards(monkeypatch):
    def _cards_for_task(flow_datastore, task_pathspec, **kwargs):
        if task_pathspec == "Flow/1/start/1":
            return None
        return iter(["card-3", "card-4"]) if task_pathspec == "Flow/1/end/2" else None

    monkeypatch.setattr(card_server, "cards_for_task", _cards_for_task)

    cards = list(
        card_server.cards_for_run(
            None, _build_run(), only_running=False, max_cards=10
        )
    )

    assert cards == [
        ("Flow/1/end/2", "card-3"),
        ("Flow/1/end/2", "card-4"),
    ]
