"""
Test that StepMutators can see and modify late-attached platform decorators.

When deploying to Argo Workflows, @kubernetes is attached to steps after
_init_step_decorators (which runs StepMutator.mutate()) has already executed.
_process_late_attached_decorator must re-run mutators so they get a chance
to see and modify the late-attached decorators.

Regression test for the ordering change introduced in 2.19.14 (PR #2719).
"""

from unittest.mock import MagicMock

import pytest

from metaflow import FlowSpec, StepMutator, step
from metaflow.user_decorators.mutable_step import MutableStep
from metaflow import decorators

KubernetesDecorator = pytest.importorskip(
    "metaflow.plugins.kubernetes.kubernetes_decorator"
).KubernetesDecorator


class kubernetes_override(StepMutator):
    """A StepMutator that overrides @kubernetes attributes."""

    def init(self, *args, **kwargs):
        self.deco_kwargs = kwargs

    def mutate(self, mutable_step):
        for deco_name, _, _, attributes in mutable_step.decorator_specs:
            if deco_name == "kubernetes":
                attributes.update(self.deco_kwargs)
                mutable_step.add_decorator(
                    deco_type=deco_name,
                    deco_kwargs=attributes,
                    duplicates=mutable_step.OVERRIDE,
                )


class _MockFlow:
    """
    Minimal wrapper that makes a FlowSpec class work with
    _process_late_attached_decorator without invoking the CLI.

    _process_late_attached_decorator needs:
    - iteration (for s in flow) to yield steps
    - flow.__class__._steps for mutator re-run
    - flow.__class__._init_graph() for graph rebuild
    - flow._graph for graph access
    """

    def __init__(self, flow_cls, steps):
        # Use the real FlowSpec class so _init_graph, _steps etc. work.
        self.__class__ = flow_cls
        self._steps_list = steps

    def __iter__(self):
        return iter(self._steps_list)


def test_process_late_attached_reruns_mutators():
    """
    Verify that _process_late_attached_decorator re-runs step mutators
    after late-attaching platform decorators.

    This reproduces the Argo Workflows deployment path:
    1. _init_step_decorators runs mutators (@kubernetes NOT yet attached)
    2. _attach_decorators adds @kubernetes to all steps
    3. _process_late_attached_decorator must re-run mutators

    Without the fix, the mutator never sees @kubernetes and the
    override silently doesn't apply.
    """

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=2, memory=8192)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step = TestFlow.start
    end_step = TestFlow.end

    # Initialize mutators (calls init()).
    for deco in start_step.config_decorators:
        if isinstance(deco, StepMutator):
            deco.external_init()

    # Step 1: Simulate _init_step_decorators running mutators.
    # No @kubernetes yet, so mutator finds nothing.
    for deco in start_step.config_decorators:
        if isinstance(deco, StepMutator):
            inserted_by_value = [deco.decorator_name] + (
                deco.inserted_by or []
            )
            deco.mutate(
                MutableStep(
                    TestFlow,
                    start_step,
                    pre_mutate=False,
                    statically_defined=deco.statically_defined,
                    inserted_by=inserted_by_value,
                )
            )

    # Confirm: no @kubernetes yet.
    assert not any(d.name == "kubernetes" for d in start_step.decorators)

    # Step 2: Late-attach @kubernetes to all steps.
    decorators._attach_decorators_to_step(
        start_step, [KubernetesDecorator.name]
    )
    decorators._attach_decorators_to_step(
        end_step, [KubernetesDecorator.name]
    )

    # @kubernetes is there but has defaults.
    k8s = [d for d in start_step.decorators if d.name == "kubernetes"][0]
    assert str(k8s.attributes["cpu"]) == "1"

    # Step 3: Call _process_late_attached_decorator.
    # This should re-run the mutator so it can override the @kubernetes.
    mock_flow = _MockFlow(TestFlow, [start_step, end_step])
    mock_datastore = MagicMock()
    mock_datastore.TYPE = "gs"
    decorators._process_late_attached_decorator(
        [KubernetesDecorator.name],
        mock_flow,
        TestFlow._graph,
        environment=None,
        flow_datastore=mock_datastore,
        logger=MagicMock(),
    )

    # Verify: start step @kubernetes should have overridden values.
    k8s_decos = [d for d in start_step.decorators if d.name == "kubernetes"]
    assert len(k8s_decos) == 1, (
        "Expected exactly one @kubernetes on start step"
    )
    assert str(k8s_decos[0].attributes["cpu"]) == "2", (
        f"Expected cpu=2, got {k8s_decos[0].attributes['cpu']}"
    )
    assert str(k8s_decos[0].attributes["memory"]) == "8192", (
        f"Expected memory=8192, got {k8s_decos[0].attributes['memory']}"
    )


def test_process_late_attached_preserves_defaults_without_mutator():
    """
    Steps without a StepMutator should retain default @kubernetes values
    after _process_late_attached_decorator.
    """

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=4, memory=16384)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step = TestFlow.start
    end_step = TestFlow.end

    # Initialize mutators on start step.
    for deco in start_step.config_decorators:
        if isinstance(deco, StepMutator):
            deco.external_init()

    # Late-attach @kubernetes to both steps.
    decorators._attach_decorators_to_step(
        start_step, [KubernetesDecorator.name]
    )
    decorators._attach_decorators_to_step(
        end_step, [KubernetesDecorator.name]
    )

    # Run _process_late_attached_decorator (exercises the new code path).
    mock_flow = _MockFlow(TestFlow, [start_step, end_step])
    mock_datastore = MagicMock()
    mock_datastore.TYPE = "gs"
    decorators._process_late_attached_decorator(
        [KubernetesDecorator.name],
        mock_flow,
        TestFlow._graph,
        environment=None,
        flow_datastore=mock_datastore,
        logger=MagicMock(),
    )

    # Verify defaults are preserved on end step (no mutator).
    k8s_decos = [d for d in end_step.decorators if d.name == "kubernetes"]
    assert len(k8s_decos) == 1
    assert str(k8s_decos[0].attributes["cpu"]) == "1"
    assert str(k8s_decos[0].attributes["memory"]) == "4096"


def test_mutator_is_noop_without_late_attachment():
    """
    If @kubernetes is never attached, the mutator's iteration over
    decorator_specs should find nothing and be a no-op.
    """

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=2, memory=8192)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step = TestFlow.start

    for deco in start_step.config_decorators:
        if isinstance(deco, StepMutator):
            deco.external_init()
            inserted_by_value = [deco.decorator_name] + (
                deco.inserted_by or []
            )
            deco.mutate(
                MutableStep(
                    TestFlow,
                    start_step,
                    pre_mutate=False,
                    statically_defined=deco.statically_defined,
                    inserted_by=inserted_by_value,
                )
            )

    # No @kubernetes should exist.
    assert not any(d.name == "kubernetes" for d in start_step.decorators)
