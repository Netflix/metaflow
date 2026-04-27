import json
import os
from metaflow.util import is_stringish

import pytest

from . import (
    MetaflowCheck,
    AssertArtifactFailed,
    AssertCardFailed,
    AssertLogFailed,
    truncate,
)


class MetadataCheck(MetaflowCheck):
    def __init__(self, flow, run_id, cli_options=()):
        super(MetadataCheck, self).__init__(flow, run_id, cli_options)
        from metaflow.client import Flow, get_namespace

        self.flow = flow
        self.run = Flow(flow.name)[self.run_id]
        expected_steps = set(step.name for step in flow)
        actual_steps = set(step.id for step in self.run)
        assert actual_steps.issubset(
            expected_steps
        ), f"Executed steps {actual_steps} not a subset of flow steps {expected_steps}"
        self._test_namespace()

    def _test_namespace(self):
        from metaflow.client import Flow, get_namespace, namespace, default_namespace
        from metaflow.exception import MetaflowNamespaceMismatch

        # test 1) METAFLOW_USER should be the default
        assert get_namespace() == "user:%s" % os.environ.get("METAFLOW_USER")
        # test 2) Run should be in the listing
        assert self.run_id in [run.id for run in Flow(self.flow.name)]
        # test 3) changing namespace should change namespace
        namespace("user:nobody")
        assert get_namespace() == "user:nobody"
        # test 4) fetching results in the incorrect namespace should fail
        with pytest.raises(MetaflowNamespaceMismatch):
            Flow(self.flow.name)[self.run_id]
        # test 5) global namespace should work
        namespace(None)
        assert get_namespace() is None
        Flow(self.flow.name)[self.run_id]
        default_namespace()

    def get_run(self):
        return self.run

    def artifact(self, step, name):
        """Return {task_id: value} for *name* in every task of *step*.

        Use in check_results for explicit assertions::

            for v in checker.artifact(step, "data").values():
                assert v == "abc"
        """
        return {
            task_id: artifacts[name]
            for task_id, artifacts in self.artifact_dict(step, name).items()
            if name in artifacts
        }

    def assert_artifact(self, step, name, value, fields=None):
        for task, artifacts in self.artifact_dict(step, name).items():
            assert name in artifacts, (
                "Task '%s' expected %s=%s but the key was not found"
                % (task, name, truncate(value))
            )
            artifact = artifacts[name]
            if fields:
                for field, v in fields.items():
                    if is_stringish(artifact):
                        data = json.loads(artifact)
                    else:
                        data = artifact
                    assert isinstance(data, dict), (
                        "Task '%s' expected %s to be a dictionary (got %s)"
                        % (task, name, type(data))
                    )
                    assert data.get(field) == v, (
                        "Task '%s' expected %s[%s]=%r but got %s[%s]=%s"
                        % (task, name, field, truncate(v), name, field, truncate(data.get(field)))
                    )
            else:
                assert artifact == value, (
                    "Task '%s' expected %s=%r but got %s=%s"
                    % (task, name, truncate(value), name, truncate(artifact))
                )

    def artifact_dict(self, step, name):
        return {task.id: {name: task[name].data} for task in self.run[step]}

    def artifact_dict_if_exists(self, step, name):
        return {
            task.id: {name: task[name].data} for task in self.run[step] if name in task
        }

    def assert_log(self, step, logtype, value, exact_match=True):
        log_value = self.get_log(step, logtype)
        if exact_match:
            assert log_value == value, (
                "Step '%s' expected task.%s=%r but got %r"
                % (step, logtype, value, log_value)
            )
        else:
            assert value in log_value, (
                "Step '%s' expected task.%s to contain %r but got %r"
                % (step, logtype, value, log_value)
            )

    def list_cards(self, step, task, card_type=None):
        from metaflow.plugins.cards.exception import CardNotPresentException

        try:
            card_iter = self.get_card(step, task, card_type)
        except CardNotPresentException:
            card_iter = None

        if card_iter is None:
            return
        pathspec = self.run[step][task].pathspec
        list_data = dict(pathspec=pathspec, cards=[])
        if len(card_iter) > 0:
            list_data["cards"] = [
                dict(
                    hash=card.hash,
                    id=card.id,
                    type=card.type,
                    filename=card.path.split("/")[-1],
                )
                for card in card_iter
            ]
        return list_data

    def assert_card(
        self,
        step,
        task,
        card_type,
        value,
        card_hash=None,
        card_id=None,
        exact_match=True,
    ):
        from metaflow.plugins.cards.exception import CardNotPresentException

        try:
            card_iter = self.get_card(step, task, card_type, card_id=card_id)
        except CardNotPresentException:
            card_iter = None
        card_data = None
        # Since there are many cards possible for a taskspec, we check for hash to assert a single card.
        # If the id argument is present then there will be a single cards anyway.
        if card_iter is not None:
            if len(card_iter) > 0:
                if card_hash is None:
                    card_data = card_iter[0].get()
                else:
                    card_filter = [c for c in card_iter if card_hash in c.hash]
                    card_data = None if len(card_filter) == 0 else card_filter[0].get()
        if exact_match:
            assert card_data == value, (
                "Task '%s/%s' expected %s card content %r but got %r"
                % (self.run_id, step, card_type, value, card_data)
            )
        else:
            assert value in card_data, (
                "Task '%s/%s' expected %s card to contain %r"
                % (self.run_id, step, card_type, value)
            )

    def get_card_data(self, step, task, card_type, card_id=None):
        """
        returns : (card_present, card_data)
        """
        from metaflow.plugins.cards.exception import CardNotPresentException

        try:
            card_iter = self.get_card(step, task, card_type, card_id=card_id)
        except CardNotPresentException:
            return False, None
        if card_id is None:
            # Return the first piece of card_data we can find.
            return True, card_iter[0].get_data()
        for card in card_iter:
            if card.id == card_id:
                return True, card.get_data()
        return False, None

    def get_log(self, step, logtype):
        return "".join(getattr(task, logtype) for task in self.run[step])

    def get_card(self, step, task, card_type, card_id=None):
        from metaflow.cards import get_cards

        iterator = get_cards(self.run[step][task], type=card_type, id=card_id)
        return iterator

    def get_user_tags(self):
        return self.run.user_tags

    def get_system_tags(self):
        return self.run.system_tags

    def add_tag(self, tag):
        return self.run.add_tag(tag)

    def add_tags(self, tags):
        return self.run.add_tags(tags)

    def remove_tag(self, tag):
        return self.run.remove_tag(tag)

    def remove_tags(self, tags):
        return self.run.remove_tags(tags)

    def replace_tag(self, tag_to_remove, tag_to_add):
        return self.run.replace_tag(tag_to_remove, tag_to_add)

    def replace_tags(self, tags_to_remove, tags_to_add):
        return self.run.replace_tags(tags_to_remove, tags_to_add)
