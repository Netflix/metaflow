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
# Fixtures to ensure singleton is reset between tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singleton():
    yield
    system_context._reset()


# ---------------------------------------------------------------------------
# ExecutionPhase
# ---------------------------------------------------------------------------


def test_execution_phase_enum_values():
    assert ExecutionPhase.LAUNCH.value == "launch"
    assert ExecutionPhase.TRAMPOLINE.value == "trampoline"
    assert ExecutionPhase.TASK.value == "task"


# ---------------------------------------------------------------------------
# _phase_from_cli_args
# ---------------------------------------------------------------------------


def test_phase_from_cli_args_none():
    assert _phase_from_cli_args(None) == ExecutionPhase.LAUNCH


def test_phase_from_cli_args_empty():
    assert _phase_from_cli_args([]) == ExecutionPhase.LAUNCH


def test_phase_from_cli_args_run_is_launch():
    assert _phase_from_cli_args(["run"]) == ExecutionPhase.LAUNCH


def test_phase_from_cli_args_resume_is_launch():
    assert _phase_from_cli_args(["resume"]) == ExecutionPhase.LAUNCH


def test_phase_from_cli_args_step_is_task():
    assert _phase_from_cli_args(["step", "mystep"]) == ExecutionPhase.TASK


def test_phase_from_cli_args_init_is_task():
    assert _phase_from_cli_args(["init"]) == ExecutionPhase.TASK


def test_phase_from_cli_args_spin_step_is_task():
    assert _phase_from_cli_args(["spin-step"]) == ExecutionPhase.TASK


def test_phase_from_cli_args_batch_is_trampoline(mocker):
    mocker.patch(
        "metaflow.plugins.get_trampoline_cli_names",
        return_value=frozenset({"batch", "kubernetes"}),
    )
    assert _phase_from_cli_args(["batch", "step", "train"]) == ExecutionPhase.TRAMPOLINE


def test_phase_from_cli_args_kubernetes_is_trampoline(mocker):
    mocker.patch(
        "metaflow.plugins.get_trampoline_cli_names",
        return_value=frozenset({"batch", "kubernetes"}),
    )
    assert (
        _phase_from_cli_args(["kubernetes", "step", "train"])
        == ExecutionPhase.TRAMPOLINE
    )


def test_phase_from_cli_args_deployment_is_launch():
    assert _phase_from_cli_args(["argo-workflows", "create"]) == ExecutionPhase.LAUNCH
    assert _phase_from_cli_args(["step-functions", "create"]) == ExecutionPhase.LAUNCH


def test_phase_from_cli_args_unknown_is_launch():
    assert _phase_from_cli_args(["show"]) == ExecutionPhase.LAUNCH
    assert _phase_from_cli_args(["status"]) == ExecutionPhase.LAUNCH


# ---------------------------------------------------------------------------
# SystemContext: phase queries
# ---------------------------------------------------------------------------


def test_system_context_launch_phase_queries():
    system_context._update(phase=ExecutionPhase.LAUNCH)
    assert system_context.is_launch
    assert not system_context.is_trampoline
    assert not system_context.is_task
    assert system_context.phase == ExecutionPhase.LAUNCH


def test_system_context_trampoline_phase_queries():
    system_context._update(phase=ExecutionPhase.TRAMPOLINE)
    assert not system_context.is_launch
    assert system_context.is_trampoline
    assert not system_context.is_task


def test_system_context_task_phase_queries():
    system_context._update(phase=ExecutionPhase.TASK)
    assert not system_context.is_launch
    assert not system_context.is_trampoline
    assert system_context.is_task


# ---------------------------------------------------------------------------
# SystemContext: progressive update
# ---------------------------------------------------------------------------


def test_system_context_initial_values_are_none():
    assert system_context.flow is None
    assert system_context.graph is None
    assert system_context.environment is None
    assert system_context.run_id is None
    assert system_context.task_id is None


def test_system_context_progressive_update():
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


def test_system_context_update_overwrites():
    system_context._update(run_id="run-1")
    assert system_context.run_id == "run-1"
    system_context._update(run_id="run-2")
    assert system_context.run_id == "run-2"


def test_system_context_update_invalid_key_raises():
    with pytest.raises(AttributeError, match="no_such_field"):
        system_context._update(no_such_field="value")


def test_system_context_reset():
    system_context._update(phase=ExecutionPhase.TASK, flow="f", run_id="r")
    system_context._reset()
    assert system_context.phase is None
    assert system_context.flow is None
    assert system_context.run_id is None


# ---------------------------------------------------------------------------
# SystemContext: input_paths
# ---------------------------------------------------------------------------


def test_system_context_input_paths_initial_none():
    assert system_context.input_paths is None


def test_system_context_input_paths_update():
    system_context._update(input_paths=["run/step/1", "run/step/2"])
    assert system_context.input_paths == ["run/step/1", "run/step/2"]


# ---------------------------------------------------------------------------
# Decorator base class: system_ctx property
# ---------------------------------------------------------------------------


def test_decorator_system_ctx_property():
    d = Decorator()
    assert d.system_ctx is system_context


def test_step_decorator_system_ctx_property():
    d = StepDecorator()
    assert d.system_ctx is system_context


def test_flow_decorator_system_ctx_property():
    d = FlowDecorator()
    assert d.system_ctx is system_context


# ---------------------------------------------------------------------------
# _ctx variant defaults
# ---------------------------------------------------------------------------


def test_step_decorator_ctx_variants_are_none():
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


def test_flow_decorator_ctx_variant_is_none():
    d = FlowDecorator()
    assert d.flow_init_ctx is None


# ---------------------------------------------------------------------------
# _ctx variant overrides
# ---------------------------------------------------------------------------


def test_step_init_ctx_override():
    class MyDeco(StepDecorator):
        name = "test_deco"
        called = False

        def step_init_ctx(self, step_name):
            MyDeco.called = True

    d = MyDeco()
    assert d.step_init_ctx is not None
    d.step_init_ctx("train")
    assert MyDeco.called


def test_task_step_completed_ctx_override_success():
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


def test_task_step_completed_ctx_handles_exception():
    class CatchDeco(StepDecorator):
        name = "catch"

        def task_step_completed_ctx(self, step_name, exception=None):
            if exception is not None:
                return True  # exception handled
            return None

    d = CatchDeco()
    assert bool(d.task_step_completed_ctx("train", exception=ValueError("x"))) is True
    assert not d.task_step_completed_ctx("train")


def test_task_decorate_ctx_override():
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


def test_flow_init_ctx_override():
    class MyFlowDeco(FlowDecorator):
        name = "test_flow_deco"
        received_options = None

        def flow_init_ctx(self, options):
            MyFlowDeco.received_options = options

    d = MyFlowDeco()
    d.flow_init_ctx({"name": "test"})
    assert MyFlowDeco.received_options == {"name": "test"}


def test_legacy_hook_still_works():
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
# SystemContext: shared state (inter-decorator communication)
# ---------------------------------------------------------------------------


def test_shared_state_publish_and_get():
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.get_published("train", "timeout", "seconds") == 300


def test_shared_state_get_missing_namespace():
    assert system_context.get_published("train", "nonexistent", "key") is None


def test_shared_state_get_missing_key():
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.get_published("train", "timeout", "nonexistent") is None


def test_shared_state_get_default():
    assert system_context.get_published("train", "timeout", "seconds", 60) == 60


def test_shared_state_has_published_namespace():
    assert not system_context.has_published("train", "timeout")
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.has_published("train", "timeout")


def test_shared_state_has_published_key():
    system_context.publish("train", "timeout", "seconds", 300)
    assert system_context.has_published("train", "timeout", "seconds")
    assert not system_context.has_published("train", "timeout", "minutes")


def test_shared_state_get_all_published():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "resources", "memory", "8192")
    system_context.publish("train", "resources", "gpu", "1")

    all_resources = system_context.get_all_published("train", "resources")
    assert all_resources == {"cpu": "4", "memory": "8192", "gpu": "1"}


def test_shared_state_get_all_published_missing():
    assert system_context.get_all_published("train", "nonexistent") == {}


def test_shared_state_overwrite_published():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "resources", "cpu", "8")
    assert system_context.get_published("train", "resources", "cpu") == "8"


def test_shared_state_multiple_namespaces():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("train", "timeout", "seconds", 300)
    system_context.publish("train", "batch", "image", "my-image:latest")

    assert system_context.get_published("train", "resources", "cpu") == "4"
    assert system_context.get_published("train", "timeout", "seconds") == 300
    assert system_context.get_published("train", "batch", "image") == "my-image:latest"


# ---------------------------------------------------------------------------
# SystemContext: step isolation for shared state
# ---------------------------------------------------------------------------


def test_step_isolation_shared_state():
    system_context.publish("train", "resources", "cpu", "4")
    system_context.publish("predict", "resources", "cpu", "16")

    assert system_context.get_published("train", "resources", "cpu") == "4"
    assert system_context.get_published("predict", "resources", "cpu") == "16"


# ---------------------------------------------------------------------------
# SystemContext: step decorator registration
# ---------------------------------------------------------------------------


def test_registration_register_and_get_step_decorators():
    decos = ["deco1", "deco2"]
    system_context.register_step_decorators("train", decos)
    assert system_context.get_step_decorators("train") == decos


def test_registration_get_step_decorators_missing_step():
    assert system_context.get_step_decorators("nonexistent") == []


def test_registration_reset_clears_shared_and_decorators():
    system_context.publish("train", "timeout", "seconds", 300)
    system_context.register_step_decorators("train", ["d"])
    system_context._reset()
    assert system_context.get_step_decorators("train") == []
    assert system_context.get_published("train", "timeout", "seconds") is None
