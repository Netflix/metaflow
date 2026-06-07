"""Unit test for the late-attached decorator mutator lifecycle guard."""

import pytest

from metaflow import FlowSpec, StepMutator, step
from metaflow.user_decorators.user_step_decorator import UserStepDecoratorMeta
from metaflow import decorators

# These tests exercise the @kubernetes late-attach path. If the Kubernetes module
# is importable but the plugin is disabled in this Metaflow install, skip rather
# than failing later with UnknownStepDecoratorException.
KubernetesDecorator = pytest.importorskip(
    "metaflow.plugins.kubernetes.kubernetes_decorator"
).KubernetesDecorator
if UserStepDecoratorMeta.get_decorator_by_name(KubernetesDecorator.name) is None:
    pytest.skip(
        "@kubernetes step decorator plugin is not enabled", allow_module_level=True
    )


@pytest.fixture
def counting_mutator_factory():
    """Build a StepMutator that records each mutate() into a list captured by
    closure, so the stateful data lives in the test rather than on the class.
    A closure variable survives the per-step copy of decorator init args (a value
    passed via init kwargs would not)."""

    def factory():
        calls = []

        class counting_mutator(StepMutator):
            def init(self, *args, **kwargs):
                pass

            def mutate(self, mutable_step):
                calls.append(mutable_step._my_step.name)

        return counting_mutator, calls

    return factory


class _Datastore:
    TYPE = "gs"


def _logger(*args, **kwargs):
    pass


def _init_mutators(*steps):
    for step in steps:
        for deco in step.config_decorators:
            if isinstance(deco, StepMutator):
                deco.external_init()


def _call_process_late_attached(flow_cls):
    flow = flow_cls(use_cli=False)
    decorators._process_late_attached_decorator(
        [KubernetesDecorator.name],
        flow,
        flow_cls._graph,
        environment=None,
        flow_datastore=_Datastore(),
        logger=_logger,
    )


def _kube(step_obj):
    return [d for d in step_obj.decorators if d.name == "kubernetes"]


@pytest.mark.parametrize(
    "preinitialize, expected_calls",
    ((True, []), (False, ["start"])),
    ids=("already_initialized", "fresh_attach"),
)
def test_late_attachment_reruns_mutator_only_for_fresh_decorator(
    counting_mutator_factory, preinitialize, expected_calls
):
    """Unit test for mutator re-run enrollment of fresh late attachments only."""

    counting_mutator, calls = counting_mutator_factory()

    class TestFlow(FlowSpec):
        @counting_mutator()
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step = TestFlow.start
    _init_mutators(start_step)

    # Late-attach @kubernetes; optionally pre-initialize it to emulate a
    # statically-defined decorator already handled by _init_step_decorators.
    decorators._attach_decorators_to_step(start_step, [KubernetesDecorator.name])
    if preinitialize:
        for deco in _kube(start_step):
            deco.external_init()
            assert deco._ran_init is True

    _call_process_late_attached(TestFlow)
    assert calls == expected_calls
