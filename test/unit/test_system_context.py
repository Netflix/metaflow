import pytest

from metaflow.system_context import (
    SystemContext,
    ExecutionPhase,
    _phase_from_cli_args,
    _TASK_COMMANDS,
    system_context,
)
from metaflow.decorators import Decorator, StepDecorator, FlowDecorator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singleton():
    """Ensure the system context singleton is reset between tests."""
    yield
    system_context._reset()


@pytest.fixture
def mock_trampoline_plugins(mocker):
    """Mock the trampoline plugin names to return a deterministic set."""
    mocker.patch(
        "metaflow.plugins.get_trampoline_cli_names",
        return_value=frozenset({"batch", "kubernetes"}),
    )


# ---------------------------------------------------------------------------
# ExecutionPhase & CLI Arg Resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "phase, expected_string",
    [
        (ExecutionPhase.LAUNCH, "launch"),
        (ExecutionPhase.TRAMPOLINE, "trampoline"),
        (ExecutionPhase.TASK, "task"),
    ],
    ids=["launch_phase", "trampoline_phase", "task_phase"],
)
def test_execution_phase_enum_values_match_expected_strings(phase, expected_string):
    """Test that the ExecutionPhase enum values evaluate to the correct string literals."""
    assert phase.value == expected_string


@pytest.mark.parametrize(
    "cli_args, expected_phase",
    [
        (None, ExecutionPhase.LAUNCH),
        ([], ExecutionPhase.LAUNCH),
        (["run"], ExecutionPhase.LAUNCH),
        (["resume"], ExecutionPhase.LAUNCH),
        (["argo-workflows", "create"], ExecutionPhase.LAUNCH),
        (["step-functions", "create"], ExecutionPhase.LAUNCH),
        (["show"], ExecutionPhase.LAUNCH),
        (["status"], ExecutionPhase.LAUNCH),
        (["step", "mystep"], ExecutionPhase.TASK),
        (["init"], ExecutionPhase.TASK),
        (["spin-step"], ExecutionPhase.TASK),
        (["batch", "step", "train"], ExecutionPhase.TRAMPOLINE),
        (["kubernetes", "step", "train"], ExecutionPhase.TRAMPOLINE),
    ],
    ids=[
        "none",
        "empty",
        "run",
        "resume",
        "argo_create",
        "step_functions_create",
        "show",
        "status",
        "step",
        "init",
        "spin_step",
        "batch_plugin",
        "k8s_plugin",
    ],
)
def test_phase_from_cli_args_resolves_correct_execution_phase(
    mock_trampoline_plugins, cli_args, expected_phase
):
    """Test that the CLI argument parser correctly maps commands to ExecutionPhases."""
    assert _phase_from_cli_args(cli_args) == expected_phase


# ---------------------------------------------------------------------------
# SystemContext: Phase Queries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "target_phase, expect_launch, expect_trampoline, expect_task",
    [
        (ExecutionPhase.LAUNCH, True, False, False),
        (ExecutionPhase.TRAMPOLINE, False, True, False),
        (ExecutionPhase.TASK, False, False, True),
    ],
    ids=["launch_active", "trampoline_active", "task_active"],
)
def test_system_context_boolean_flags_reflect_current_phase(
    target_phase, expect_launch, expect_trampoline, expect_task
):
    """Test that the boolean helper properties correctly reflect the underlying phase."""
    system_context._update(phase=target_phase)

    assert system_context.phase == target_phase
    assert system_context.is_launch is expect_launch
    assert system_context.is_trampoline is expect_trampoline
    assert system_context.is_task is expect_task


# ---------------------------------------------------------------------------
# SystemContext: Progressive Update
# ---------------------------------------------------------------------------


def test_system_context_initializes_with_none_values():
    assert system_context.flow is None
    assert system_context.graph is None
    assert system_context.environment is None
    assert system_context.run_id is None
    assert system_context.task_id is None


def test_system_context_supports_progressive_updates():
    # Flow-level info arrives first
    system_context._update(flow="my_flow", graph="my_graph")
    assert system_context.flow == "my_flow"
    assert system_context.graph == "my_graph"

    # Runtime-level info arrives later
    system_context._update(run_id="run-123", package="my_package")
    assert system_context.run_id == "run-123"
    assert system_context.package == "my_package"

    # Task-level info arrives last
    system_context._update(task_id="task-456", retry_count=2)
    assert system_context.task_id == "task-456"
    assert system_context.retry_count == 2


def test_system_context_updates_overwrite_existing_values():
    system_context._update(run_id="run-1")
    assert system_context.run_id == "run-1"

    system_context._update(run_id="run-2")
    assert system_context.run_id == "run-2"


def test_system_context_update_raises_attribute_error_on_invalid_keys():
    with pytest.raises(AttributeError, match="no_such_field"):
        system_context._update(no_such_field="value")


def test_system_context_reset_clears_all_attributes():
    system_context._update(phase=ExecutionPhase.TASK, flow="f", run_id="r")
    system_context._reset()

    assert system_context.phase is None
    assert system_context.flow is None
    assert system_context.run_id is None


def test_system_context_input_paths_lifecycle():
    assert system_context.input_paths is None

    system_context._update(input_paths=["run/step/1", "run/step/2"])
    assert system_context.input_paths == ["run/step/1", "run/step/2"]


# ---------------------------------------------------------------------------
# Decorator Base Classes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "decorator_cls",
    [Decorator, StepDecorator, FlowDecorator],
    ids=["base_decorator", "step_decorator", "flow_decorator"],
)
def test_decorator_classes_expose_system_context_singleton(decorator_cls):
    """Test that all decorator base classes correctly expose the context singleton."""
    d = decorator_cls()
    assert d.system_ctx is system_context


def test_step_decorator_ctx_variants_default_to_none():
    d = StepDecorator()
    assert d.step_init_ctx is None
    assert d.runtime_init_ctx is None
    assert d.runtime_task_created_ctx is None
    assert d.runtime_step_cli_ctx is None
    assert d.runtime_finished_ctx is None
    assert d.task_pre_step_ctx is None
    assert d.task_decorate_ctx is None
    assert d.task_step_completed_ctx is None
    assert d.task_finished_ctx is None


def test_flow_decorator_ctx_variant_defaults_to_none():
    d = FlowDecorator()
    assert d.flow_init_ctx is None


# ---------------------------------------------------------------------------
# _ctx Variant Overrides
# ---------------------------------------------------------------------------


def test_step_init_ctx_override_is_called_successfully():
    class MyDeco(StepDecorator):
        name = "test_deco"
        called = False

        def step_init_ctx(self, step_name):
            MyDeco.called = True

    d = MyDeco()
    assert d.step_init_ctx is not None

    d.step_init_ctx("train")
    assert MyDeco.called


def test_task_step_completed_ctx_handles_exceptions_correctly():
    class MyDeco(StepDecorator):
        name = "test_deco"
        last_exception = "NOT_CALLED"

        def task_step_completed_ctx(self, step_name, exception=None):
            MyDeco.last_exception = exception

    d = MyDeco()

    # Success path (no exception)
    d.task_step_completed_ctx("train")
    assert MyDeco.last_exception is None

    # Exception path
    err = ValueError("boom")
    d.task_step_completed_ctx("train", exception=err)
    assert MyDeco.last_exception is err


def test_task_step_completed_ctx_returns_true_when_handling_exception():
    class CatchDeco(StepDecorator):
        name = "catch"

        def task_step_completed_ctx(self, step_name, exception=None):
            if exception is not None:
                return True  # exception handled
            return None

    d = CatchDeco()
    assert d.task_step_completed_ctx("train", exception=ValueError("x")) is True
    assert not d.task_step_completed_ctx("train")


def test_task_decorate_ctx_successfully_wraps_functions():
    class WrapDeco(StepDecorator):
        name = "wrap"

        def task_decorate_ctx(self, step_name, step_func):
            def wrapper(*args, **kwargs):
                return step_func(*args, **kwargs)

            return wrapper

    d = WrapDeco()
    original = lambda: 42
    wrapped = d.task_decorate_ctx("train", original)

    assert wrapped is not original
    assert wrapped() == 42


def test_flow_init_ctx_receives_options_dictionary():
    class MyFlowDeco(FlowDecorator):
        name = "test_flow_deco"
        received_options = None

        def flow_init_ctx(self, options):
            MyFlowDeco.received_options = options

    d = MyFlowDeco()
    d.flow_init_ctx({"name": "test"})

    assert MyFlowDeco.received_options == {"name": "test"}


def test_legacy_hooks_trigger_when_ctx_variants_are_missing():
    """Decorators that don't define _ctx variants still use legacy hooks."""

    class LegacyDeco(StepDecorator):
        name = "legacy"
        called_with = None

        def step_init(
            self,
            flow,
            graph,
            step_name,
            decorators,
            environment,
            flow_datastore,
            logger,
        ):
            LegacyDeco.called_with = step_name

    d = LegacyDeco()

    assert d.step_init_ctx is None
    d.step_init("f", "g", "train", [], "env", "ds", "log")

    assert LegacyDeco.called_with == "train"


# ---------------------------------------------------------------------------
# SystemContext: Shared State (Inter-Decorator Communication)
# ---------------------------------------------------------------------------


def test_shared_state_publish_and_retrieve_values():
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.get_published("train", "timeout", "seconds") == 300


@pytest.mark.parametrize(
    "namespace, key, fallback",
    [
        ("train", "nonexistent", "key"),
        ("train", "timeout", "nonexistent"),
    ],
    ids=["missing_namespace", "missing_key"],
)
def test_shared_state_returns_none_for_missing_keys(namespace, key, fallback):
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.get_published(namespace, key, fallback) is None


def test_shared_state_get_published_respects_default_fallback_values():
    assert system_context.get_published("train", "timeout", "seconds", default=60) == 60


def test_shared_state_has_published_returns_booleans():
    assert not system_context.has_published("train", "timeout")
    system_context.publish("train", "timeout", "seconds", 300)

    assert system_context.has_published("train", "timeout")
    assert system_context.has_published("train", "timeout", "seconds")
    assert not system_context.has_published("train", "timeout", "minutes")


def test_shared_state_get_all_published_returns_full_dictionary():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "resources", "memory", "8192")
    system_context.publish("train", "resources", "gpu", "1")

    all_resources = system_context.get_all_published("train", "resources")
    assert all_resources == {"cpu": "4", "memory": "8192", "gpu": "1"}


def test_shared_state_get_all_published_returns_empty_dict_on_missing_namespace():
    assert system_context.get_all_published("train", "nonexistent") == {}


def test_shared_state_publishing_overwrites_existing_keys():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "resources", "cpu", "8")
    assert system_context.get_published("train", "resources", "cpu") == "8"


def test_shared_state_isolates_data_across_namespaces_and_steps():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "timeout", "seconds", 300)
    system_context.publish("predict", "resources", "cpu", "16")

    assert system_context.get_published("train", "resources", "cpu") == "4"
    assert system_context.get_published("train", "timeout", "seconds") == 300
    assert system_context.get_published("predict", "resources", "cpu") == "16"


# ---------------------------------------------------------------------------
# SystemContext: Step Decorator Registration
# ---------------------------------------------------------------------------


def test_decorator_registration_stores_and_retrieves_lists():
    decos = ["deco1", "deco2"]
    system_context.register_step_decorators("train", decos)
    assert system_context.get_step_decorators("train") == decos


def test_decorator_registration_returns_empty_list_for_missing_steps():
    assert system_context.get_step_decorators("nonexistent") == []


def test_reset_clears_shared_state_and_decorator_registrations():
    system_context.publish("train", "timeout", "seconds", 300)
    system_context.register_step_decorators("train", ["d"])

    system_context._reset()

    assert system_context.get_step_decorators("train") == []
    assert system_context.get_published("train", "timeout", "seconds") is None
