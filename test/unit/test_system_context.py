import pytest

from metaflow.system_context import (
    SystemContext,
    ExecutionPhase,
    _phase_from_cli_args,
    _TASK_COMMANDS,
    _TRAMPOLINE_COMMANDS,
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


class TestExecutionPhase:
    def test_enum_values(self):
        assert ExecutionPhase.LAUNCH.value == "launch"
        assert ExecutionPhase.TRAMPOLINE.value == "trampoline"
        assert ExecutionPhase.TASK.value == "task"


# ---------------------------------------------------------------------------
# _phase_from_cli_args
# ---------------------------------------------------------------------------


class TestPhaseFromCliArgs:
    def test_none_args(self):
        assert _phase_from_cli_args(None) == ExecutionPhase.LAUNCH

    def test_empty_args(self):
        assert _phase_from_cli_args([]) == ExecutionPhase.LAUNCH

    def test_run_is_launch(self):
        assert _phase_from_cli_args(["run"]) == ExecutionPhase.LAUNCH

    def test_resume_is_launch(self):
        assert _phase_from_cli_args(["resume"]) == ExecutionPhase.LAUNCH

    def test_step_is_task(self):
        assert _phase_from_cli_args(["step", "mystep"]) == ExecutionPhase.TASK

    def test_init_is_task(self):
        assert _phase_from_cli_args(["init"]) == ExecutionPhase.TASK

    def test_spin_step_is_task(self):
        assert _phase_from_cli_args(["spin-step"]) == ExecutionPhase.TASK

    def test_batch_is_trampoline(self):
        assert (
            _phase_from_cli_args(["batch", "step", "train"])
            == ExecutionPhase.TRAMPOLINE
        )

    def test_kubernetes_is_trampoline(self):
        assert (
            _phase_from_cli_args(["kubernetes", "step", "train"])
            == ExecutionPhase.TRAMPOLINE
        )

    def test_deployment_is_launch(self):
        assert (
            _phase_from_cli_args(["argo-workflows", "create"]) == ExecutionPhase.LAUNCH
        )
        assert (
            _phase_from_cli_args(["step-functions", "create"]) == ExecutionPhase.LAUNCH
        )

    def test_unknown_is_launch(self):
        assert _phase_from_cli_args(["show"]) == ExecutionPhase.LAUNCH
        assert _phase_from_cli_args(["status"]) == ExecutionPhase.LAUNCH


# ---------------------------------------------------------------------------
# SystemContext — phase queries
# ---------------------------------------------------------------------------


class TestSystemContextPhase:
    def test_phase_queries(self):
        system_context._update(phase=ExecutionPhase.LAUNCH)
        assert system_context.is_launch
        assert not system_context.is_trampoline
        assert not system_context.is_task
        assert system_context.phase == ExecutionPhase.LAUNCH

    def test_trampoline_phase(self):
        system_context._update(phase=ExecutionPhase.TRAMPOLINE)
        assert not system_context.is_launch
        assert system_context.is_trampoline
        assert not system_context.is_task

    def test_task_phase(self):
        system_context._update(phase=ExecutionPhase.TASK)
        assert not system_context.is_launch
        assert not system_context.is_trampoline
        assert system_context.is_task


# ---------------------------------------------------------------------------
# SystemContext — progressive update
# ---------------------------------------------------------------------------


class TestSystemContextUpdate:
    def test_initial_values_are_none(self):
        assert system_context.flow is None
        assert system_context.graph is None
        assert system_context.environment is None
        assert system_context.run_id is None
        assert system_context.task_id is None
        assert system_context.step_name is None

    def test_progressive_update(self):
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

    def test_update_overwrites(self):
        system_context._update(run_id="run-1")
        assert system_context.run_id == "run-1"
        system_context._update(run_id="run-2")
        assert system_context.run_id == "run-2"

    def test_update_invalid_key_raises(self):
        with pytest.raises(AttributeError, match="no_such_field"):
            system_context._update(no_such_field="value")

    def test_reset(self):
        system_context._update(phase=ExecutionPhase.TASK, flow="f", run_id="r")
        system_context._reset()
        assert system_context.phase is None
        assert system_context.flow is None
        assert system_context.run_id is None


# ---------------------------------------------------------------------------
# SystemContext — input_paths
# ---------------------------------------------------------------------------


class TestSystemContextInputPaths:
    def test_input_paths_initial_none(self):
        assert system_context.input_paths is None

    def test_input_paths_update(self):
        system_context._update(input_paths=["run/step/1", "run/step/2"])
        assert system_context.input_paths == ["run/step/1", "run/step/2"]


# ---------------------------------------------------------------------------
# Decorator base class — system_ctx property
# ---------------------------------------------------------------------------


class TestDecoratorBaseClassProperties:
    def test_system_ctx_property(self):
        d = Decorator()
        assert d.system_ctx is system_context

    def test_step_decorator_has_property(self):
        d = StepDecorator()
        assert d.system_ctx is system_context

    def test_flow_decorator_has_property(self):
        d = FlowDecorator()
        assert d.system_ctx is system_context


# ---------------------------------------------------------------------------
# _ctx variant defaults
# ---------------------------------------------------------------------------


class TestCtxVariantDefaults:
    """Verify that _ctx variants are None by default on base classes."""

    def test_step_decorator_ctx_variants_are_none(self):
        d = StepDecorator()
        assert d.step_init_ctx is None
        assert d.runtime_init_ctx is None
        assert d.runtime_task_created_ctx is None
        assert d.runtime_step_cli_ctx is None
        assert d.runtime_finished_ctx is None
        assert d.task_pre_step_ctx is None
        assert d.task_decorate_ctx is None
        assert d.task_step_completed is None
        assert d.task_finished_ctx is None

    def test_flow_decorator_ctx_variant_is_none(self):
        d = FlowDecorator()
        assert d.flow_init_ctx is None


# ---------------------------------------------------------------------------
# _ctx variant overrides
# ---------------------------------------------------------------------------


class TestCtxVariantOverride:
    """Verify that subclasses can override _ctx variants to enable dispatch."""

    def test_step_init_ctx_override(self):
        class MyDeco(StepDecorator):
            name = "test_deco"
            called = False

            def step_init_ctx(self):
                MyDeco.called = True

        d = MyDeco()
        assert d.step_init_ctx is not None
        d.step_init_ctx()
        assert MyDeco.called

    def test_task_step_completed_override_success(self):
        class MyDeco(StepDecorator):
            name = "test_deco"
            last_exception = "NOT_CALLED"

            def task_step_completed(self, exception=None):
                MyDeco.last_exception = exception

        d = MyDeco()

        # Success path (no exception)
        d.task_step_completed()
        assert MyDeco.last_exception is None

        # Exception path
        err = ValueError("boom")
        d.task_step_completed(exception=err)
        assert MyDeco.last_exception is err

    def test_task_step_completed_handles_exception(self):
        class CatchDeco(StepDecorator):
            name = "catch"

            def task_step_completed(self, exception=None):
                if exception is not None:
                    return True  # exception handled
                return None

        d = CatchDeco()
        assert bool(d.task_step_completed(exception=ValueError("x"))) is True
        assert not d.task_step_completed()

    def test_task_decorate_ctx_override(self):
        class WrapDeco(StepDecorator):
            name = "wrap"

            def task_decorate_ctx(self, step_func):
                def wrapper(*args, **kwargs):
                    return step_func(*args, **kwargs)

                return wrapper

        d = WrapDeco()
        original = lambda: 42
        wrapped = d.task_decorate_ctx(original)
        assert wrapped is not original
        assert wrapped() == 42

    def test_flow_init_ctx_override(self):
        class MyFlowDeco(FlowDecorator):
            name = "test_flow_deco"
            received_options = None

            def flow_init_ctx(self, options):
                MyFlowDeco.received_options = options

        d = MyFlowDeco()
        d.flow_init_ctx({"name": "test"})
        assert MyFlowDeco.received_options == {"name": "test"}

    def test_legacy_hook_still_works(self):
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
# SystemContext — shared state (inter-decorator communication)
# ---------------------------------------------------------------------------


class TestSystemContextSharedState:
    def test_publish_and_get(self):
        system_context._update(step_name="train")
        system_context.publish("timeout", "seconds", 300)
        assert system_context.get_published("timeout", "seconds") == 300

    def test_get_missing_namespace(self):
        system_context._update(step_name="train")
        assert system_context.get_published("nonexistent", "key") is None

    def test_get_missing_key(self):
        system_context._update(step_name="train")
        system_context.publish("timeout", "seconds", 300)
        assert system_context.get_published("timeout", "nonexistent") is None

    def test_get_default(self):
        system_context._update(step_name="train")
        assert system_context.get_published("timeout", "seconds", 60) == 60

    def test_has_published_namespace(self):
        system_context._update(step_name="train")
        assert not system_context.has_published("timeout")
        system_context.publish("timeout", "seconds", 300)
        assert system_context.has_published("timeout")

    def test_has_published_key(self):
        system_context._update(step_name="train")
        system_context.publish("timeout", "seconds", 300)
        assert system_context.has_published("timeout", "seconds")
        assert not system_context.has_published("timeout", "minutes")

    def test_get_all_published(self):
        system_context._update(step_name="train")
        system_context.publish("resources", "cpu", "4")
        system_context.publish("resources", "memory", "8192")
        system_context.publish("resources", "gpu", "1")

        all_resources = system_context.get_all_published("resources")
        assert all_resources == {"cpu": "4", "memory": "8192", "gpu": "1"}

    def test_get_all_published_missing(self):
        system_context._update(step_name="train")
        assert system_context.get_all_published("nonexistent") == {}

    def test_overwrite_published(self):
        system_context._update(step_name="train")
        system_context.publish("resources", "cpu", "4")
        system_context.publish("resources", "cpu", "8")
        assert system_context.get_published("resources", "cpu") == "8"

    def test_multiple_namespaces(self):
        system_context._update(step_name="train")
        system_context.publish("resources", "cpu", "4")
        system_context.publish("timeout", "seconds", 300)
        system_context.publish("batch", "image", "my-image:latest")

        assert system_context.get_published("resources", "cpu") == "4"
        assert system_context.get_published("timeout", "seconds") == 300
        assert system_context.get_published("batch", "image") == "my-image:latest"


# ---------------------------------------------------------------------------
# SystemContext — step isolation for shared state
# ---------------------------------------------------------------------------


class TestSystemContextStepIsolation:
    def test_isolated_shared_state(self):
        system_context._update(step_name="train")
        system_context.publish("resources", "cpu", "4")

        system_context._update(step_name="predict")
        system_context.publish("resources", "cpu", "16")

        # Switch back and verify isolation
        system_context._update(step_name="train")
        assert system_context.get_published("resources", "cpu") == "4"

        system_context._update(step_name="predict")
        assert system_context.get_published("resources", "cpu") == "16"


# ---------------------------------------------------------------------------
# SystemContext — step decorator registration
# ---------------------------------------------------------------------------


class TestSystemContextRegistration:
    def test_register_and_get_step_decorators(self):
        decos = ["deco1", "deco2"]
        system_context.register_step_decorators("train", decos)
        assert system_context.get_step_decorators("train") == decos

    def test_get_step_decorators_uses_current_step(self):
        decos = ["deco1"]
        system_context.register_step_decorators("train", decos)
        system_context._update(step_name="train")
        assert system_context.get_step_decorators() == decos

    def test_get_step_decorators_missing_step(self):
        assert system_context.get_step_decorators("nonexistent") == []

    def test_reset_clears_shared_and_decorators(self):
        system_context._update(step_name="train")
        system_context.publish("timeout", "seconds", 300)
        system_context.register_step_decorators("train", ["d"])
        system_context._reset()
        assert system_context.get_step_decorators("train") == []
        system_context._update(step_name="train")
        assert system_context.get_published("timeout", "seconds") is None
