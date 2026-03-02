import pytest

from metaflow.plugins.cards.card_server import cards_for_run


class DummyTask:
    def __init__(self, pathspec):
        self.pathspec = pathspec
        self.finished = False


class DummyStep:
    def __init__(self, tasks):
        self._tasks = tasks

    def tasks(self):
        return self._tasks


class DummyRun:
    def __init__(self):
        self._steps = [
            DummyStep([DummyTask("flow/1/step1/task1")]),
            DummyStep([DummyTask("flow/1/step2/task2")]),
        ]

    def steps(self):
        return self._steps


def dummy_cards_for_task(*args, **kwargs):
    class DummyCard:
        hash = "abc"
        type = "test"
        path = "/tmp"
        id = "1"

    yield DummyCard()


def test_cards_for_run_respects_max_cards(monkeypatch):
    # Patch cards_for_task to return dummy cards
    monkeypatch.setattr(
        "metaflow.plugins.cards.card_server.cards_for_task",
        dummy_cards_for_task,
    )

    run = DummyRun()

    # This should NOT raise RuntimeError
    try:
        list(
            cards_for_run(
                flow_datastore=None,
                run_object=run,
                only_running=False,
                max_cards=1,
            )
        )
    except RuntimeError:
        pytest.fail("cards_for_run raised RuntimeError due to StopIteration")