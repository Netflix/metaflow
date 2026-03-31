import os
import sys
import types
from types import SimpleNamespace

if not hasattr(os, "O_NONBLOCK"):
    os.O_NONBLOCK = 0

if "fcntl" not in sys.modules:
    fake_fcntl = types.ModuleType("fcntl")
    fake_fcntl.F_SETFL = 0
    fake_fcntl.fcntl = lambda *args, **kwargs: None
    sys.modules["fcntl"] = fake_fcntl

from metaflow.plugins.cards import card_server


class MockTask(object):
    def __init__(self, pathspec, finished=False):
        self.pathspec = pathspec
        self.finished = finished


class MockStep(object):
    def __init__(self, tasks):
        self._tasks = tasks

    def tasks(self):
        return self._tasks


class MockRun(object):
    def __init__(self, steps):
        self._steps = steps

    def steps(self):
        return self._steps


def test_cards_for_run_stops_cleanly_at_max_cards(monkeypatch):
    def fake_cards_for_task(*args, **kwargs):
        for idx in range(25):
            yield SimpleNamespace(hash="hash-%d" % idx)

    monkeypatch.setattr(card_server, "cards_for_task", fake_cards_for_task)

    run = MockRun([MockStep([MockTask("MyFlow/1/start/1")])])

    cards = list(
        card_server.cards_for_run(
            None,
            run,
            only_running=False,
            max_cards=20,
        )
    )

    assert len(cards) == 20
    assert cards[0][0] == "MyFlow/1/start/1"
    assert cards[-1][1].hash == "hash-19"


def test_cards_for_run_returns_all_cards_below_limit(monkeypatch):
    def fake_cards_for_task(*args, **kwargs):
        for idx in range(3):
            yield SimpleNamespace(hash="hash-%d" % idx)

    monkeypatch.setattr(card_server, "cards_for_task", fake_cards_for_task)

    run = MockRun([MockStep([MockTask("MyFlow/1/start/1")])])

    cards = list(
        card_server.cards_for_run(
            None,
            run,
            only_running=False,
            max_cards=20,
        )
    )

    assert len(cards) == 3
    assert [card.hash for _, card in cards] == ["hash-0", "hash-1", "hash-2"]
