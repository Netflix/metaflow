"""Tests for the card client (`get_cards`)."""

import pytest

from metaflow import Runner
from metaflow.client import core as client_core
from metaflow.client.core import get_namespace, namespace
from metaflow.plugins.cards.card_client import get_cards
from metaflow.plugins.datastores.local_storage import LocalStorage

CARD_OWNER = "card-owner"
RESUMER = "resumer"

CARD_FLOW = """
from metaflow import FlowSpec, card, step


class CardClientFlow(FlowSpec):
    @card
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CardClientFlow()
"""


@pytest.fixture(scope="module")
def card_flow_dir(tmp_path_factory):
    flow_dir = tmp_path_factory.mktemp("card_client")
    flow_path = flow_dir / "card_client_flow.py"
    flow_path.write_text(CARD_FLOW)
    with Runner(
        str(flow_path),
        cwd=str(flow_dir),
        env={"METAFLOW_USER": CARD_OWNER},
    ).run() as running:
        assert running.status == "successful"
        origin_run_id = running.run.id
        start_task_pathspec = running.run["start"].task.pathspec
    # A different user resumes the run, so the cloned `start` task lives in the
    # resumer's namespace while its card lives with the origin task.
    with Runner(
        str(flow_path),
        cwd=str(flow_dir),
        env={"METAFLOW_USER": RESUMER},
    ).resume(step_to_rerun="end", origin_run_id=origin_run_id) as running:
        assert running.status == "successful"
        resumed_task_pathspec = running.run["start"].task.pathspec
    return flow_dir, start_task_pathspec, resumed_task_pathspec


@pytest.fixture
def local_client(monkeypatch, card_flow_dir):
    # Point the client at the local datastore of the run created above, and
    # restore the global client state (namespace / metadata provider) afterwards.
    flow_dir, start_task_pathspec, resumed_task_pathspec = card_flow_dir
    monkeypatch.chdir(flow_dir)
    monkeypatch.setattr(LocalStorage, "datastore_root", None)
    monkeypatch.setattr(client_core, "current_metadata", False)
    monkeypatch.setattr(client_core, "current_namespace", False)
    return start_task_pathspec, resumed_task_pathspec


@pytest.mark.parametrize(
    "use_resumed_task",
    [False, True],
    ids=["origin-task", "resumed-task"],
)
def test_get_cards_with_pathspec_does_not_change_global_namespace(
    local_client, use_resumed_task
):
    start_task_pathspec, resumed_task_pathspec = local_client
    pathspec = resumed_task_pathspec if use_resumed_task else start_task_pathspec
    # Neither run is in this namespace; get_cards must still find the card.
    namespace("user:someone-else")

    cards = get_cards(pathspec)

    assert len(cards) == 1
    # get_cards must not leak a namespace(None) into the caller's session.
    assert get_namespace() == "user:someone-else"


def test_get_cards_with_task_follows_resumed_origin_across_namespaces(local_client):
    _, resumed_task_pathspec = local_client
    # The resumed task is in the resumer's namespace, but its card lives with
    # the origin task, which belongs to another user.
    namespace("user:%s" % RESUMER)
    task = client_core.Task(resumed_task_pathspec)

    cards = get_cards(task)

    assert len(cards) == 1
    assert get_namespace() == "user:%s" % RESUMER
