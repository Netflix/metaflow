"""
Regression tests for issue #3025: StepMutator.mutate() must see late-attached
platform decorators (e.g. @kubernetes).

Background
----------
When deploying to Argo Workflows (and other platforms that late-attach a
compute decorator), ``@kubernetes`` is attached to steps *after*
``_init_step_decorators`` -- which runs ``StepMutator.mutate()`` -- has already
executed.  This ordering changed in 2.19.14 (PR #2719), which moved
``_init_step_decorators`` into ``cli.py start()`` so it now runs before
``_attach_decorators``.  As a result a ``StepMutator`` that inspects
``mutable_step.decorator_specs`` looking for ``"kubernetes"`` finds nothing and
its ``add_decorator(..., duplicates=OVERRIDE)`` silently becomes a no-op.

``_process_late_attached_decorator`` must therefore re-run step mutators for the
steps that just received a late-attached decorator so the override can take
effect.

These tests drive the internal ``_init_step_decorators`` ->
``_attach_decorators`` -> ``_process_late_attached_decorator`` sequence directly
(no CLI / no cloud backend required) and assert the override is applied.
"""

from unittest.mock import MagicMock

import pytest

from metaflow import FlowSpec, StepMutator, step
from metaflow.user_decorators.mutable_step import MutableStep
from metaflow import decorators

# These tests exercise the @kubernetes late-attach path.  If the Kubernetes
# plugin is unavailable, skip rather than fail.
KubernetesDecorator = pytest.importorskip(
    "metaflow.plugins.kubernetes.kubernetes_decorator"
).KubernetesDecorator


class kubernetes_override(StepMutator):
    """A StepMutator that overrides @kubernetes attributes when it can see it."""

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


class counting_mutator(StepMutator):
    """A StepMutator that records how many times ``mutate`` is invoked, so we
    can assert the late-attach re-run fires exactly once and is not spuriously
    re-run for already-initialized (statically-defined) decorators."""

    calls = []

    def init(self, *args, **kwargs):
        pass

    def mutate(self, mutable_step):
        type(self).calls.append(mutable_step._my_step.name)


class _MockFlow:
    """Make a FlowSpec class usable by ``_process_late_attached_decorator``
    without invoking the CLI.

    Reassigning ``__class__`` to ``flow_cls`` delegates ``_steps``,
    ``_init_graph``, ``_graph`` and ``__iter__`` (which iterates ``cls._steps``)
    to the real FlowSpec class.
    """

    def __init__(self, flow_cls):
        self.__class__ = flow_cls


def _init_mutators(*steps):
    for s in steps:
        for deco in s.config_decorators:
            if isinstance(deco, StepMutator):
                deco.external_init()


def _run_mutators_premutate(flow_cls, *steps):
    """Simulate the first mutator pass done by ``_init_step_decorators``
    (before any late-attached decorator exists)."""
    for s in steps:
        for deco in s.config_decorators:
            if isinstance(deco, StepMutator):
                inserted_by_value = [deco.decorator_name] + (deco.inserted_by or [])
                deco.mutate(
                    MutableStep(
                        flow_cls,
                        s,
                        pre_mutate=False,
                        statically_defined=deco.statically_defined,
                        inserted_by=inserted_by_value,
                    )
                )


def _call_process_late_attached(flow_cls):
    mock_flow = _MockFlow(flow_cls)
    mock_datastore = MagicMock()
    mock_datastore.TYPE = "gs"
    decorators._process_late_attached_decorator(
        [KubernetesDecorator.name],
        mock_flow,
        flow_cls._graph,
        environment=None,
        flow_datastore=mock_datastore,
        logger=MagicMock(),
    )


def _kube(step_obj):
    return [d for d in step_obj.decorators if d.name == "kubernetes"]


def test_late_attached_kubernetes_is_visible_to_step_mutator():
    """The headline regression: a mutator override of late-attached
    @kubernetes must apply after ``_process_late_attached_decorator``."""

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=2, memory=8192)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step, end_step = TestFlow.start, TestFlow.end

    _init_mutators(start_step)
    # First pass: no @kubernetes yet, so the override finds nothing.
    _run_mutators_premutate(TestFlow, start_step)
    assert not _kube(start_step), "no @kubernetes should exist before attach"

    # Late-attach @kubernetes to every step (what Argo deploy does).
    decorators._attach_decorators_to_step(start_step, [KubernetesDecorator.name])
    decorators._attach_decorators_to_step(end_step, [KubernetesDecorator.name])
    assert str(_kube(start_step)[0].attributes["cpu"]) == "1"  # defaults

    # The fix under test: re-run mutators on late-attached steps.
    _call_process_late_attached(TestFlow)

    start_k8s = _kube(start_step)
    assert len(start_k8s) == 1, "expected exactly one @kubernetes on start"
    assert str(start_k8s[0].attributes["cpu"]) == "2", (
        "mutator override of cpu must apply to late-attached @kubernetes; "
        "got %s" % start_k8s[0].attributes["cpu"]
    )
    assert str(start_k8s[0].attributes["memory"]) == "8192"


def test_step_without_mutator_keeps_kubernetes_defaults():
    """A step with no StepMutator must keep default @kubernetes values
    (the re-run must not leak the other step's override)."""

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=4, memory=16384)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step, end_step = TestFlow.start, TestFlow.end

    _init_mutators(start_step)
    decorators._attach_decorators_to_step(start_step, [KubernetesDecorator.name])
    decorators._attach_decorators_to_step(end_step, [KubernetesDecorator.name])

    _call_process_late_attached(TestFlow)

    # start (has mutator) -> overridden
    start_k8s = _kube(start_step)
    assert len(start_k8s) == 1
    assert str(start_k8s[0].attributes["cpu"]) == "4"
    assert str(start_k8s[0].attributes["memory"]) == "16384"

    # end (no mutator) -> defaults preserved, exactly one decorator
    end_k8s = _kube(end_step)
    assert len(end_k8s) == 1
    assert str(end_k8s[0].attributes["cpu"]) == "1"
    assert str(end_k8s[0].attributes["memory"]) == "4096"


def test_no_late_attachment_is_a_noop():
    """If no late-attached decorator is present, the re-run loop must be
    skipped and no @kubernetes should appear."""

    class TestFlow(FlowSpec):
        @kubernetes_override(cpu=2, memory=8192)
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    start_step = TestFlow.start
    _init_mutators(start_step)

    # No _attach_decorators_to_step call here.
    _call_process_late_attached(TestFlow)

    assert not _kube(start_step), "no @kubernetes should be created out of nothing"


def test_already_initialized_decorator_does_not_retrigger_mutator():
    """A decorator that was already external_init'd (``_ran_init`` True, e.g. a
    statically-defined ``@kubernetes`` that ``_init_step_decorators`` already
    initialized before the deploy-time late-attach) must NOT enroll its step for
    a mutator re-run -- only a genuinely fresh attachment (``_ran_init`` False)
    should. This guards the narrowing that avoids double-running non-idempotent
    mutators."""

    # Case 1: an already-initialized decorator must NOT retrigger the mutator.
    class AlreadyInitFlow(FlowSpec):
        @counting_mutator()
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    counting_mutator.calls = []
    start_step = AlreadyInitFlow.start
    _init_mutators(start_step)

    # Attach @kubernetes, then initialize it (external_init sets _ran_init=True
    # and fills defaults) to emulate a statically-defined decorator that
    # _init_step_decorators handled before the platform late-attach pass runs.
    decorators._attach_decorators_to_step(start_step, [KubernetesDecorator.name])
    for deco in _kube(start_step):
        deco.external_init()
        assert deco._ran_init is True

    _call_process_late_attached(AlreadyInitFlow)
    assert counting_mutator.calls == [], (
        "an already-initialized decorator must not retrigger the mutator; "
        "got calls=%r" % counting_mutator.calls
    )

    # Case 2 (sanity): a genuinely fresh attach (_ran_init False) DOES enroll
    # the step and triggers exactly one mutator re-run.
    class FreshAttachFlow(FlowSpec):
        @counting_mutator()
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    counting_mutator.calls = []
    fresh_step = FreshAttachFlow.start
    _init_mutators(fresh_step)
    decorators._attach_decorators_to_step(fresh_step, [KubernetesDecorator.name])
    _call_process_late_attached(FreshAttachFlow)
    assert counting_mutator.calls == ["start"], (
        "a genuinely late-attached decorator must trigger exactly one mutator "
        "re-run; got calls=%r" % counting_mutator.calls
    )
