import os
import tempfile
import pytest

from metaflow import Runner
from spin_test_helpers import assert_artifacts, run_step, FLOWS_DIR, ARTIFACTS_DIR

# ---------------------------------------------------------------------------
# Simple Flow Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_file,fixture_name",
    [
        ("merge_artifacts_flow.py", "merge_artifacts_run"),
        ("simple_config_flow.py", "simple_config_run"),
        ("simple_parameter_flow.py", "simple_parameter_run"),
        ("complex_dag_flow.py", "complex_dag_run"),
    ],
    ids=["merge_artifacts", "simple_config", "simple_parameter", "complex_dag"],
)
def test_simple_flows_validate_artifacts(flow_file, fixture_name, request):
    """Test that basic flows run steps correctly and validate their artifacts."""
    run = request.getfixturevalue(fixture_name)

    # Act & Assert: Iterate through and run each step
    for step in run.steps():
        if fixture_name == "complex_dag_run":
            run_step(flow_file, run, step.id, environment="conda")
        else:
            run_step(flow_file, run, step.id)


# ---------------------------------------------------------------------------
# Artifacts Module Tests
# ---------------------------------------------------------------------------


def test_artifacts_module_evaluates_correctly(complex_dag_run):
    """Test that an external artifacts module correctly injects state into a spun step."""
    # Setup
    step_name = "step_a"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")
    artifacts_path = os.path.join(ARTIFACTS_DIR, "complex_dag_step_a.py")

    # Act
    with Runner(flow_path, cwd=FLOWS_DIR, environment="conda").spin(
        task.pathspec,
        artifacts_module=artifacts_path,
        persist=True,
    ) as spin:

        # Assert
        spin_task = spin.task
        assert spin_task["my_output"].data == [10, 11, 12, 3]


def test_artifacts_module_injects_dynamic_data_in_join_step(
    complex_dag_run, complex_dag_step_d_artifacts, tmp_path
):
    """Test that dynamically generated artifacts are correctly loaded during a join step."""
    # Setup
    step_name = "step_d"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")

    # Setup: Create a temporary artifacts file with dynamic data
    temp_artifacts_file = tmp_path / "temp_complex_dag_step_d.py"
    temp_artifacts_file.write_text(f"ARTIFACTS = {repr(complex_dag_step_d_artifacts)}")

    # Act
    with Runner(flow_path, cwd=FLOWS_DIR, environment="conda").spin(
        task.pathspec,
        artifacts_module=str(temp_artifacts_file),
        persist=True,
    ) as spin:

        # Assert
        spin_task = spin.task
        assert spin_task["my_output"].data == [-1]


# ---------------------------------------------------------------------------
# Decorator & Config Tests
# ---------------------------------------------------------------------------


def test_timeout_decorator_enforces_time_limit(simple_config_run):
    """Test that a configured timeout decorator stops execution and raises an exception."""
    # Setup
    step_name = "start"
    task = simple_config_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_config_flow.py")

    # Act & Assert
    with pytest.raises(Exception):
        with Runner(
            flow_path, cwd=FLOWS_DIR, config_value=[("config", {"timeout": 2})]
        ).spin(
            task.pathspec,
            persist=True,
        ):
            pass


def test_skip_decorators_bypasses_timeout(simple_config_run):
    """Test that using skip_decorators=True successfully ignores the timeout limit."""
    # Setup
    step_name = "start"
    task = simple_config_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_config_flow.py")

    # Act
    with Runner(
        flow_path, cwd=FLOWS_DIR, config_value=[("config", {"timeout": 2})]
    ).spin(
        task.pathspec,
        skip_decorators=True,
        persist=True,
    ) as spin:

        # Assert: Should complete successfully even though step length > timeout
        assert spin.task.finished


def test_spin_preserves_explicit_top_level_decospecs(spin_decospec_run):
    """Test that spin respects top-level decorator specifications provided to Runner."""
    # Setup
    task = spin_decospec_run["start"].task
    flow_path = os.path.join(FLOWS_DIR, "spin_decospec_flow.py")

    # Act & Assert
    with pytest.raises(Exception, match="timed out"):
        with Runner(
            flow_path,
            cwd=FLOWS_DIR,
            decospecs=["timeout:seconds=1"],
            file_read_timeout=30,
            show_output=False,
        ).spin(
            task.pathspec,
            persist=True,
        ):
            pass


def test_spin_step_ignores_default_decospecs(spin_decospec_run):
    """Test that spin does NOT inadvertently apply METAFLOW_DEFAULT_DECOSPECS."""
    # Setup
    task = spin_decospec_run["start"].task
    flow_path = os.path.join(FLOWS_DIR, "spin_decospec_flow.py")

    # Act
    with Runner(
        flow_path,
        cwd=FLOWS_DIR,
        env={"METAFLOW_DEFAULT_DECOSPECS": "timeout:seconds=1"},
        file_read_timeout=30,
        show_output=False,
    ).spin(
        task.pathspec,
        persist=True,
    ) as spin:

        # Assert
        assert spin.task.finished
        assert spin.task["done"].data is True


# ---------------------------------------------------------------------------
# Internal State & Integration Tests
# ---------------------------------------------------------------------------


def test_spin_persists_internal_hidden_artifacts(simple_parameter_run):
    """Test that spinning a task retains internal Metaflow graph and state artifacts."""
    # Setup
    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")

    # Act
    with Runner(flow_path, cwd=FLOWS_DIR).spin(task.pathspec, persist=True) as spin:
        spin_task = spin.task

        # Assert
        assert "_graph_info" in spin_task
        assert "_foreach_stack" in spin_task


def test_spin_generates_cards_correctly(simple_card_run):
    """Test that spinning a flow with the @card decorator successfully outputs cards."""
    # Setup
    from metaflow.cards import get_cards

    step_name = "start"
    task = simple_card_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_card_flow.py")

    # Act
    with Runner(flow_path, cwd=FLOWS_DIR).spin(task.pathspec, persist=True) as spin:
        res = get_cards(spin.task, follow_resumed=False)

        # Assert
        assert res is not None, "Cards should be generated and retrievable"
        # Optional: assert len(res) > 0 if you expect a specific number of cards


def test_spin_with_flow_parameters_raises_error(simple_parameter_run):
    """Test that passing standard flow parameters to spin() raises an Unknown argument error."""
    # Setup
    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")

    # Act & Assert
    with pytest.raises(Exception, match="Unknown argument"):
        with Runner(flow_path, cwd=FLOWS_DIR).spin(
            task.pathspec,
            alpha=1.0,
            persist=True,
        ):
            pass


# ---------------------------------------------------------------------------
# WARNING: State-Modifying Test
# This test modifies the global metadata provider via `inspect_spin`.
# It is kept at the bottom of the file to prevent side-effects on other tests.
# ---------------------------------------------------------------------------


def test_inspect_spin_client_allows_artifact_access(simple_parameter_run):
    """Test accessing spun artifacts directly using the inspect_spin client."""
    # Setup
    from metaflow import inspect_spin, Task

    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")

    with tempfile.TemporaryDirectory() as _:
        # Setup: Run spin to generate artifacts
        with Runner(flow_path, cwd=FLOWS_DIR).spin(
            task.pathspec,
            persist=True,
        ) as spin:
            spin_task = spin.task
            spin_pathspec = spin_task.pathspec

            assert spin_task["a"] is not None
            assert spin_task["b"] is not None
            assert spin_pathspec is not None

        # Act: Set metadata provider to spin
        inspect_spin(FLOWS_DIR)
        client_task = Task(spin_pathspec, _namespace_check=False)

        # Assert: Verify task and artifacts are accessible via client
        assert client_task is not None
        assert hasattr(client_task, "artifacts")
        assert client_task.artifacts.a.data == 10
        assert client_task.artifacts.b.data == 20
        assert client_task.artifacts.alpha.data == 0.05
