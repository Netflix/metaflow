"""
Airflow compilation validation tests.

Compiles Metaflow flows to Airflow DAG Python files and validates them.
Tier 1: If Airflow is installed, loads into DagBag to check import errors.
Tier 2: Structural validation (Python AST parsing, checks for DAG object).

These tests catch DAG generation bugs in seconds without deploying.
"""

import ast
import os
import subprocess
import sys

import pytest

pytestmark = [pytest.mark.airflow_compilation]


# ---------------------------------------------------------------------------
# Core Validation Logic
# ---------------------------------------------------------------------------


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
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def compile_and_validate(tmp_path, monkeypatch):
    """
    Factory fixture to compile a flow to an Airflow DAG and validate it.
    Automatically handles environment variables and temporary file cleanup.
    """
    # Ensure compilation-only environment variables are set safely
    monkeypatch.setenv("METAFLOW_DEFAULT_METADATA", "local")

    def _impl(flow_path, **extra_tl_args):
        from .test_utils import _resolve_flow_path

        full_path = _resolve_flow_path(flow_path)
        dag_file_path = tmp_path / "compiled_dag.py"

        cmd = [sys.executable, full_path, "--no-pylint"]
        for k, v in extra_tl_args.items():
            if v is not None:
                cmd.extend([f"--{k.replace('_', '-')}", str(v)])
        cmd.extend(["airflow", "create", str(dag_file_path)])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            stderr = result.stderr or ""
            stdout = result.stdout or ""
            if "No such command" in stderr or "No such command" in stdout:
                pytest.skip(
                    "airflow CLI not available (extension may override plugins)"
                )
            if "ConnectionRefusedError" in stderr or "ConnectionError" in stderr:
                pytest.skip("Airflow backend not configured (connection refused)")
            if "is not supported" in stderr:
                pytest.skip(f"Feature not supported by Airflow: {stderr.strip()}")
            pytest.fail(f"Compilation failed:\nstderr: {stderr}\nstdout: {stdout}")

        # Read the generated DAG and validate it
        dag_source = dag_file_path.read_text()
        validation = _validate_dag_source(dag_source, str(dag_file_path))

        assert (
            validation["result"] == "OK"
        ), f"Validation failed: {validation.get('diagnostics')}"
        return dag_source

    return _impl


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_path",
    [
        "basic/helloworld.py",
        "dag/branch_flow.py",
        "dag/foreach_flow.py",
        "basic/retry_flow.py",
        "basic/resources_flow.py",
        "lifecycle/schedule_flow.py",
    ],
    ids=["linear", "branch", "foreach", "retry", "resources", "schedule"],
)
def test_airflow_dag_compilation(compile_and_validate, flow_path):
    """Core Metaflow flow patterns compile to structurally valid Airflow DAGs."""
    compile_and_validate(flow_path)


# ---------------------------------------------------------------------------
# Invariant checks (specific bugs we've fixed)
# ---------------------------------------------------------------------------


def test_tags_are_list_not_tuple(compile_and_validate):
    """DAG tags must be a list, not a tuple (Airflow rejects tuples)."""
    dag_source = compile_and_validate("basic/helloworld.py")

    # Check that tags assignment uses list syntax
    tree = ast.parse(dag_source)
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "tags":
            assert not isinstance(
                node.value, ast.Tuple
            ), "DAG tags should be a list, not a tuple"
