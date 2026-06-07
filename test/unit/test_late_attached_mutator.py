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

import pytest

from metaflow import FlowSpec, StepMutator, step
from metaflow.user_decorators.mutable_step import MutableStep
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


@pytest.mark.parametrize(
    "preinitialize, expected_calls",
    ((True, []), (False, ["start"])),
    ids=("already_initialized", "fresh_attach"),
)
def test_late_attachment_reruns_mutator_only_for_fresh_decorator(
    counting_mutator_factory, preinitialize, expected_calls
):
    """A decorator already external_init'd (``_ran_init`` True, e.g. a
    statically-defined ``@kubernetes`` that ``_init_step_decorators`` already
    initialized) must NOT enroll its step for a mutator re-run; only a genuinely
    fresh attachment (``_ran_init`` False) should. Guards the narrowing that
    avoids double-running non-idempotent mutators."""

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
