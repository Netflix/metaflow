import pytest
from metaflow.plugins.cards.card_server import cards_for_run


class DummyCard:
    pass


class DummyTask:
    def __init__(self, pathspec):
        self.pathspec = pathspec
        self.finished = False


class DummyStep:
    def tasks(self):
        return [
            DummyTask("flow/run/step/task1"),
            DummyTask("flow/run/step/task2"),
        ]


class DummyRun:
    def steps(self):
        return [DummyStep()]


def dummy_cards_for_task(*args, **kwargs):
    yield DummyCard()


def test_cards_for_run_respects_max_cards(monkeypatch):
    monkeypatch.setattr(
        "metaflow.plugins.cards.card_server.cards_for_task",
        dummy_cards_for_task,
    )

    run = DummyRun()

    result = list(
        cards_for_run(
            flow_datastore=None,
            run_object=run,
            only_running=False,
            max_cards=1,
        )
    )

    assert len(result) == 1, f"Expected 1 card, got {len(result)}"