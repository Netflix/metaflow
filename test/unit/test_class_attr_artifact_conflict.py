"""
Class-attribute / artifact conflict detection (issue #422)
===========================================================
Class attributes defined on a FlowSpec are converted to read-only properties
by MetaflowTask._init_parameters before each step executes.  The existing
conflict detection (the property setter) now:

  1. Raises MetaflowException (not the generic AttributeError) so the error
     is clearly surfaced as a Metaflow-level problem.
  2. Names the exact attribute in the error message.
  3. Reports the current step name.

Additionally, _init_parameters performs an *early* conflict check (issue #422)
before the step body runs: if the input datastore contains an artifact whose
name matches a class-level attribute AND that artifact was NOT a class constant
in the previous step (i.e. it is a genuine user artifact), a MetaflowException
is raised at task-initialisation time — before _exec_step_function is called.

The check uses only information already available at task init time
(_graph_info["constants"] from the input datastore); no AST analysis or step
source inspection is required.

These tests verify:
  - Cross-step early detection: artifact from previous step clashes with a
    class-level attribute → MetaflowException raised before step body starts.
  - Error message names the attribute.
  - Class constants passed down from the previous step are NOT flagged.
  - Parameters are not flagged.
  - New artifact names (no clash) are not flagged.
  - Underscore-prefixed internal names are not flagged.
  - After _init_parameters the property setter raises MetaflowException
    (not AttributeError) and names the attribute.
  - Methods on the class are not made read-only.

NOTE: These tests require a Linux environment.  Metaflow's sidecar/plugin
layer imports ``fcntl`` and ``os.O_NONBLOCK`` which are unavailable on
Windows, causing collection failures identical to those seen across every
other test in ``test/unit/``.  Run these tests in the project's standard
Linux CI environment.
"""

import pytest

from metaflow import FlowSpec, Parameter, step
from metaflow.exception import MetaflowException
from metaflow.task import MetaflowTask


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


class _FakeDS:
    """Minimal datastore stand-in for _init_parameters tests.

    Parameters
    ----------
    artifact_names : iterable of str
        Names that should appear to be present in the datastore (i.e. returned
        as True by ``name in ds``).
    prev_constants : iterable of str
        Names that _graph_info["constants"] will report as class-level
        constants from the previous step.
    """

    def __init__(self, artifact_names=(), prev_constants=()):
        self._names = frozenset(artifact_names) | {"_graph_info"}
        self._constants = list(prev_constants)

    def __contains__(self, name):
        return name in self._names

    def __getitem__(self, name):
        if name == "_graph_info":
            return {"constants": [{"name": n} for n in self._constants]}
        raise KeyError(name)


class _FakeTask:
    """Minimal stand-in for MetaflowTask — only ``self.flow`` is required."""

    def __init__(self, flow):
        self.flow = flow


def _call_init_parameters(flow_cls, artifact_names=(), prev_constants=()):
    """
    Create a bare flow instance and call MetaflowTask._init_parameters with a
    fake datastore.  ``artifact_names`` are the names the fake datastore
    reports as present; ``prev_constants`` are names that _graph_info will
    report as class-level constants from the previous step.
    """
    flow = object.__new__(flow_cls)
    # _current_step is set by run_step before _init_parameters; simulate that.
    flow._current_step = "end"
    task = _FakeTask(flow)
    ds = _FakeDS(artifact_names=artifact_names, prev_constants=prev_constants)
    MetaflowTask._init_parameters(task, ds, passdown=False)


# ---------------------------------------------------------------------------
# Cross-step early conflict detection
# ---------------------------------------------------------------------------


def test_cross_step_conflict_raises_before_step_body():
    """When a previous step produced an artifact whose name matches a
    class-level attribute — and that name was NOT a class constant in the
    previous step — MetaflowException must be raised during task init (before
    the step body executes)."""

    class ConflictFlow(FlowSpec):
        myval = 5

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    with pytest.raises(MetaflowException):
        # 'myval' is in the input datastore as a step-body artifact
        # (prev_constants does NOT include 'myval').
        _call_init_parameters(
            ConflictFlow,
            artifact_names=["myval"],
            prev_constants=[],
        )


def test_error_message_names_the_attribute():
    """The early-detection error message must contain the conflicting
    attribute name."""

    class NamedConflictFlow(FlowSpec):
        conflicting_attr = "original"

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    with pytest.raises(MetaflowException) as exc_info:
        _call_init_parameters(
            NamedConflictFlow,
            artifact_names=["conflicting_attr"],
            prev_constants=[],
        )
    assert "conflicting_attr" in str(exc_info.value)


def test_class_constant_passdown_not_flagged():
    """When an artifact name IS listed in _graph_info["constants"] of the
    previous step, it is a passed-down class constant — not a user artifact.
    It must NOT trigger the early detection."""

    class SafeFlow(FlowSpec):
        constant = 100

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    # 'constant' is in the datastore AND was a class constant in the previous
    # step → normal passdown, not a conflict.
    _call_init_parameters(
        SafeFlow,
        artifact_names=["constant"],
        prev_constants=["constant"],
    )  # must not raise


def test_parameter_not_flagged():
    """Parameter descriptors must not be treated as class-level attributes."""

    class ParamFlow(FlowSpec):
        my_param = Parameter("my_param", default=42)

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    # Even if 'my_param' appears in the datastore, it is a Parameter and
    # must not trigger the class-attr conflict check.
    _call_init_parameters(
        ParamFlow,
        artifact_names=["my_param"],
        prev_constants=[],
    )  # must not raise


def test_new_artifact_not_a_class_attr_not_flagged():
    """An artifact name in the datastore that does NOT correspond to any
    class-level attribute must not raise."""

    class ValidFlow(FlowSpec):
        existing = 5  # class attr

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    # 'new_artifact' is in the datastore but is not a class attr → no conflict.
    _call_init_parameters(
        ValidFlow,
        artifact_names=["new_artifact"],
        prev_constants=[],
    )  # must not raise


def test_underscore_prefix_not_flagged():
    """Names with a leading underscore are internal and must not be detected
    as class-level attribute conflicts."""

    class InternalFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    _call_init_parameters(
        InternalFlow,
        artifact_names=["_internal"],
        prev_constants=[],
    )  # must not raise


def test_no_conflict_when_datastore_is_empty():
    """A class with class-level attributes but no conflicting artifacts in the
    datastore must construct and initialise without error."""

    class CleanFlow(FlowSpec):
        constant = 100

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    _call_init_parameters(CleanFlow)  # empty datastore → must not raise


# ---------------------------------------------------------------------------
# Same-step property setter improvements
# ---------------------------------------------------------------------------


def test_setter_raises_metaflow_exception_not_attribute_error():
    """After _init_parameters the property setter must raise MetaflowException
    (not AttributeError) when user code assigns to a class-level attribute."""

    class SetterFlow(FlowSpec):
        x = 5

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    flow = object.__new__(SetterFlow)
    flow._current_step = "start"
    task = _FakeTask(flow)
    MetaflowTask._init_parameters(task, _FakeDS(), passdown=False)

    # After _init_parameters, 'x' is a read-only property on SetterFlow.
    # Assigning to it must raise MetaflowException (not AttributeError).
    with pytest.raises(MetaflowException):
        flow.x = 99


def test_setter_names_attribute_in_message():
    """The property setter error message must contain the attribute name."""

    class SetterMsgFlow(FlowSpec):
        y = 10

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    flow = object.__new__(SetterMsgFlow)
    flow._current_step = "start"
    task = _FakeTask(flow)
    MetaflowTask._init_parameters(task, _FakeDS(), passdown=False)

    with pytest.raises(MetaflowException) as exc_info:
        flow.y = 42
    assert "y" in str(exc_info.value)


def test_setter_names_step_in_message():
    """The property setter error message must contain the step name."""

    class SetterStepFlow(FlowSpec):
        z = 7

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    flow = object.__new__(SetterStepFlow)
    flow._current_step = "start"
    task = _FakeTask(flow)
    MetaflowTask._init_parameters(task, _FakeDS(), passdown=False)

    with pytest.raises(MetaflowException) as exc_info:
        flow.z = 0
    assert "start" in str(exc_info.value)


def test_method_not_made_readonly():
    """Class methods must not become read-only properties."""

    class MethodFlow(FlowSpec):
        def helper(self):
            return 42

        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            self.next()

    flow = object.__new__(MethodFlow)
    flow._current_step = "start"
    task = _FakeTask(flow)
    MetaflowTask._init_parameters(task, _FakeDS(), passdown=False)

    # 'helper' is a method and must remain callable on the instance.
    assert callable(MethodFlow.helper)
