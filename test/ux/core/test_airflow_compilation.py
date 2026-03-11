"""
Airflow compilation validation tests.

Compiles Metaflow flows to Airflow DAG Python files and validates them.
Tier 1: If Airflow is installed, loads into DagBag to check import errors.
Tier 2: Structural validation (Python AST parsing, checks for DAG object).

These tests catch DAG generation bugs in seconds without deploying.
"""

import ast
import json
import subprocess
import sys
import tempfile
import os
import pytest

pytestmark = [pytest.mark.airflow_compilation]


def _get_compile_env():
    """Get environment variables for compilation-only tests."""
    env = os.environ.copy()
    env["METAFLOW_DEFAULT_METADATA"] = "local"
    return env


def _compile_flow_to_dag(flow_path, **extra_tl_args):
    """Compile a flow to an Airflow DAG Python file."""
    from .test_utils import _resolve_flow_path

    full_path = _resolve_flow_path(flow_path)

    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        dag_file_path = f.name

    cmd = [sys.executable, full_path, "--no-pylint"]
    for k, v in extra_tl_args.items():
        if v is not None:
            cmd.extend([f"--{k.replace('_', '-')}", str(v)])
    cmd.extend(["airflow", "create", "--file", dag_file_path])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=_get_compile_env(),
        )
        if result.returncode != 0:
            # Clean up on failure
            try:
                os.unlink(dag_file_path)
            except OSError:
                pass
            stderr = result.stderr or ""
            stdout = result.stdout or ""
            if "No such command" in stderr or "No such command" in stdout:
                pytest.skip(
                    "airflow CLI not available (extension may override plugins)"
                )
            pytest.fail(f"Compilation failed:\nstderr: {stderr}\nstdout: {stdout}")

        with open(dag_file_path, "r") as f:
            dag_source = f.read()

        return dag_source, dag_file_path
    except Exception:
        try:
            os.unlink(dag_file_path)
        except OSError:
            pass
        raise


def _validate_dag_source(dag_source, dag_file_path=None):
    """Validate compiled DAG Python source.

    Checks:
    1. Valid Python syntax (AST parse)
    2. Contains a DAG object assignment
    3. If Airflow is available, loads into DagBag
    """
    errors = []

    # 1. Valid Python syntax
    try:
        tree = ast.parse(dag_source)
    except SyntaxError as e:
        return {"result": "FAIL", "diagnostics": [f"Syntax error: {e}"]}

    # 2. Check for DAG-related patterns
    source_lower = dag_source.lower()
    if "dag" not in source_lower:
        errors.append("No 'DAG' reference found in compiled output")

    # Check the AST for a DAG() call
    has_dag_call = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "DAG":
                has_dag_call = True
            elif isinstance(func, ast.Attribute) and func.attr == "DAG":
                has_dag_call = True

    if not has_dag_call:
        errors.append("No DAG() constructor call found in compiled output")

    # 3. Try DagBag validation if Airflow is available
    if dag_file_path:
        try:
            from airflow.models import DagBag

            dag_bag = DagBag(
                dag_folder=os.path.dirname(dag_file_path),
                include_examples=False,
            )
            if dag_bag.import_errors:
                for path, err in dag_bag.import_errors.items():
                    errors.append(f"DagBag import error for {path}: {err}")
        except ImportError:
            pass  # Airflow not installed -- skip DagBag validation

    return {"result": "OK" if not errors else "FAIL", "diagnostics": errors}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAirflowCompilation:
    """Compile each flow type to Airflow DAG and validate."""

    def _compile_and_validate(self, flow_path, **extra_tl_args):
        dag_source, dag_file_path = _compile_flow_to_dag(flow_path, **extra_tl_args)
        try:
            result = _validate_dag_source(dag_source, dag_file_path)
            assert (
                result["result"] == "OK"
            ), f"Validation failed: {result.get('diagnostics')}"
            return dag_source
        finally:
            try:
                os.unlink(dag_file_path)
            except OSError:
                pass

    def test_linear_flow(self):
        """Simple start->step->end flow compiles to valid Airflow DAG."""
        self._compile_and_validate("basic/helloworld.py")

    def test_branch_flow(self):
        """Parallel branch flow compiles to valid Airflow DAG."""
        self._compile_and_validate("dag/branch_flow.py")

    def test_foreach_flow(self):
        """Foreach flow compiles to valid Airflow DAG."""
        self._compile_and_validate("dag/foreach_flow.py")

    def test_retry_flow(self):
        """Flow with @retry compiles to valid Airflow DAG."""
        self._compile_and_validate("basic/retry_flow.py")

    def test_resources_flow(self):
        """Flow with @resources compiles to valid Airflow DAG."""
        self._compile_and_validate("basic/resources_flow.py")

    def test_trigger_flow(self):
        """Flow with @trigger compiles to valid Airflow DAG."""
        self._compile_and_validate("triggers/hello_static_trigger.py")

    def test_schedule_flow(self):
        """Flow with @schedule compiles to valid Airflow DAG."""
        self._compile_and_validate("lifecycle/schedule_flow.py")

    # ------------------------------------------------------------------
    # Invariant checks (specific bugs we've fixed)
    # ------------------------------------------------------------------

    def test_tags_are_list_not_tuple(self):
        """DAG tags must be a list, not a tuple (Airflow rejects tuples)."""
        dag_source = self._compile_and_validate("basic/helloworld.py")
        # Check that tags assignment uses list syntax
        tree = ast.parse(dag_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "tags":
                assert not isinstance(
                    node.value, ast.Tuple
                ), "DAG tags should be a list, not a tuple"
