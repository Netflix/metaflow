import json
import os
from metaflow.util import is_stringish

from . import (
    MetaflowCheck,
    AssertArtifactFailed,
    AssertCardFailed,
    AssertLogFailed,
    assert_equals,
    assert_exception,
    truncate,
)


class MetadataCheck(MetaflowCheck):
    def __init__(self, flow):
        from metaflow.client import Flow, get_namespace

        self.flow = flow
        self.run = Flow(flow.name)[self.run_id]
        assert_equals(
            sorted(step.name for step in flow), sorted(step.id for step in self.run)
        )
        self._test_namespace()

    def _test_namespace(self):
        from metaflow.client import Flow, get_namespace, namespace, default_namespace
        from metaflow.exception import MetaflowNamespaceMismatch
        import os

        # test 1) METAFLOW_USER should be the default
        assert_equals("user:%s" % os.environ.get("METAFLOW_USER"), get_namespace())
        # test 2) Run should be in the listing
        assert_equals(True, self.run_id in [run.id for run in Flow(self.flow.name)])
        # test 3) changing namespace should change namespace
        namespace("user:nobody")
        assert_equals(get_namespace(), "user:nobody")
        # test 4) fetching results in the incorrect namespace should fail
        assert_exception(
            lambda: Flow(self.flow.name)[self.run_id], MetaflowNamespaceMismatch
        )
        # test 5) global namespace should work
        namespace(None)
        assert_equals(get_namespace(), None)
        Flow(self.flow.name)[self.run_id]
        default_namespace()

    def get_run(self):
        return self.run

    def assert_artifact(self, step, name, value, fields=None):
        for task, artifacts in self.artifact_dict(step, name).items():
            if name in artifacts:
                artifact = artifacts[name]
                if fields:
                    for field, v in fields.items():
                        if is_stringish(artifact):
                            data = json.loads(artifact)
                        else:
                            data = artifact
                        if not isinstance(data, dict):
                            raise AssertArtifactFailed(
                                "Task '%s' expected %s to be a dictionary (got %s)"
                                % (task, name, type(data))
                            )
                        if data.get(field, None) != v:
                            raise AssertArtifactFailed(
                                "Task '%s' expected %s[%s]=%r but got %s[%s]=%s"
                                % (
                                    task,
                                    name,
                                    field,
                                    truncate(value),
                                    name,
                                    field,
                                    truncate(data[field]),
                                )
                            )
                elif artifact != value:
                    raise AssertArtifactFailed(
                        "Task '%s' expected %s=%r but got %s=%s"
                        % (task, name, truncate(value), name, truncate(artifact))
                    )
            else:
                raise AssertArtifactFailed(
                    "Task '%s' expected %s=%s but "
                    "the key was not found" % (task, name, truncate(value))
                )
        return True

    def artifact_dict(self, step, name):
        return {task.id: {name: task[name].data} for task in self.run[step]}

    def artifact_dict_if_exists(self, step, name):
        return {
            task.id: {name: task[name].data} for task in self.run[step] if name in task
        }

    def assert_log(self, step, logtype, value, exact_match=True):
        log_value = self.get_log(step, logtype)
        if log_value == value:
            return True
        elif not exact_match and value in log_value:
            return True
        else:
            raise AssertLogFailed(
                "Step '%s' expected task.%s='%s' but got task.%s='%s'"
                % (step, logtype, repr(value), logtype, repr(log_value))
            )

    def assert_card(self, step, task, card_type, value, exact_match=True):
        from metaflow.plugins.cards.exception import CardNotPresentException

        try:
            card_iter = self.get_card(step, task, card_type)
        except CardNotPresentException:
            card_iter = None
        card_data = None
        # FUTURE FIXME:
        # We are checking the first card here.
        # Not all possible present cards
        # This should change in the future when we support many decorator.
        if card_iter is not None:
            if len(card_iter) > 0:
                card_data = card_iter[0].get()
        if (exact_match and card_data != value) or (
            not exact_match and value not in card_data
        ):
            raise AssertCardFailed(
                "Task '%s/%s' expected %s card with content '%s' but got '%s'"
                % (self.run_id, step, card_type, repr(value), repr(card_data))
            )
        return True

    def get_log(self, step, logtype):
        return "".join(getattr(task, logtype) for task in self.run[step])

    def get_card(self, step, task, card_type):
        from metaflow.cards import get_cards

        iterator = get_cards(self.run[step][task], type=card_type)
        return iterator
