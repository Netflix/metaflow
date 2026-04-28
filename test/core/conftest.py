"""Pytest configuration for core integration tests.

This conftest provides the small set of fixtures every core test consumes:

- ``metaflow_runner`` — runs a ``FlowSpec`` subclass via the cli or api
  executor and yields a small result object.
- ``executor`` — parametrises over ``["cli", "api"]`` so tests opt in by
  taking the fixture; tests that only support one executor pass it
  literally to ``metaflow_runner``.
- ``backend_marker`` — applies the right backend marker to every collected
  item, sourced from ``METAFLOW_CORE_MARKER`` (set by tox).

There is no graph × test cartesian product here. Each test file declares
its own ``FlowSpec`` and its own pytest functions.
"""

from __future__ import annotations

import inspect
import os
import shlex
import subprocess
import sys
import textwrap
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pytest

_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)

# ---------------------------------------------------------------------------
# Backend marker — sourced from tox setenv. One env var, one marker.
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config, items):
    """Apply the active backend marker to every collected item.

    Tox env sets ``METAFLOW_CORE_MARKER`` to ``local`` / ``gcs`` / etc.;
    that marker is stamped on every item so ``pytest -m local`` selects
    the right subset on a multi-backend invocation.
    """
    marker_name = os.environ.get("METAFLOW_CORE_MARKER", "local")
    marker = getattr(pytest.mark, marker_name)
    for item in items:
        item.add_marker(marker)


# ---------------------------------------------------------------------------
# Run options shared by every test
# ---------------------------------------------------------------------------

_DEFAULT_RUN_OPTIONS: tuple[str, ...] = (
    "--max-workers=50",
    "--max-num-splits=10000",
    "--tag=刺身 means sashimi",
    "--tag=multiple tags should be ok",
)

_DEFAULT_TOP_OPTIONS_LOCAL = (
    "--metadata=local",
    "--datastore=local",
    "--environment=local",
    "--event-logger=nullSidecarLogger",
    "--no-pylint",
    "--quiet",
)


@pytest.fixture(scope="session")
def top_options() -> tuple[str, ...]:
    """Top-level metaflow CLI options for the active backend.

    Sourced from ``METAFLOW_CORE_TOP_OPTIONS`` (tox setenv); falls back to
    a local-datastore default that works on a fresh checkout without tox.
    """
    raw = os.environ.get("METAFLOW_CORE_TOP_OPTIONS")
    if raw:
        return tuple(shlex.split(raw))
    return _DEFAULT_TOP_OPTIONS_LOCAL


@pytest.fixture(scope="session")
def default_run_options() -> tuple[str, ...]:
    return _DEFAULT_RUN_OPTIONS


def _executor_choices() -> tuple[str, ...]:
    """Read the executors enabled for this backend from tox setenv.

    Defaults to (cli, api). For scheduler-only backends like argo/sfn,
    tox should set METAFLOW_CORE_EXECUTORS=scheduler.
    """
    raw = os.environ.get("METAFLOW_CORE_EXECUTORS", "cli,api")
    return tuple(e for e in raw.split(",") if e)


@pytest.fixture(params=_executor_choices())
def executor(request) -> str:
    """Parametrise tests over the executors enabled by the active tox env.

    Tests that only support one executor should take ``metaflow_runner``
    directly and pass ``executor=`` literally instead of using this
    fixture.
    """
    return request.param


# ---------------------------------------------------------------------------
# Result object + runner fixture
# ---------------------------------------------------------------------------


@dataclass
class FlowRun:
    """Outcome of a flow run, agnostic to executor.

    Attributes
    ----------
    returncode : int
        0 on success, non-zero on failure.
    run_id : str | None
        Run id pulled from ``run-id`` file. ``None`` on a failed run that
        never wrote the file.
    flow_name : str
        Name of the FlowSpec class that was run.
    stdout, stderr : str
        Captured output. May be empty on the api success path (we don't
        read the log files unless asked).
    """

    returncode: int
    run_id: str | None
    flow_name: str
    stdout: str = ""
    stderr: str = ""

    @property
    def successful(self) -> bool:
        return self.returncode == 0

    def run(self):
        """Return the metaflow Run object. Lazy import so tests that
        only check returncode don't pay the import cost."""
        from metaflow import Flow, Run  # noqa: F401

        if self.run_id is None:
            raise RuntimeError(f"flow {self.flow_name!r} produced no run_id")
        return Flow(self.flow_name, _namespace_check=False)[self.run_id]


def _write_flow_module(tmp_path: Path, flow_cls: type) -> Path:
    """Materialise a FlowSpec class as a standalone module in tmp_path.

    Test files often contain multiple FlowSpec subclasses (one per test
    function). Runner / metaflow bail with "Multiple FlowSpec classes
    found" when the file contains more than one, so we emit ONLY the
    target class plus the surrounding module's imports/top-level
    helpers. The generated file is ``test_flow.py`` so it has a stable
    name.
    """
    flow_module = sys.modules[flow_cls.__module__]
    flow_source = inspect.getsource(flow_cls)

    # Reuse the module's imports + top-level helpers so the FlowSpec can
    # reference them. Grab everything up to the first decorator (`@…`) or
    # `class …` line at column 0 — that's the start of the first FlowSpec
    # definition, and stopping there avoids duplicating any decorators
    # that `inspect.getsource(flow_cls)` will already include.
    module_source = inspect.getsource(flow_module)
    prologue_lines: list[str] = []
    for line in module_source.splitlines():
        if line.startswith("@") or line.startswith("class "):
            break
        prologue_lines.append(line)
    prologue = "\n".join(prologue_lines)

    guard = textwrap.dedent(
        f"""

        if __name__ == "__main__":
            {flow_cls.__name__}()
        """
    )

    flow_path = tmp_path / "test_flow.py"
    flow_path.write_text(prologue + "\n\n" + flow_source + guard)
    return flow_path


def _build_subprocess_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "LANG": "en_US.UTF-8",
            "LC_ALL": "en_US.UTF-8",
            "PYTHONIOENCODING": "utf_8",
        }
    )
    if extra:
        env.update(extra)
    return env


@pytest.fixture
def metaflow_runner(tmp_path, monkeypatch, top_options, default_run_options):
    """Yield a callable that runs a FlowSpec via cli or api.

    Usage::

        def test_basic_artifact(metaflow_runner, executor):
            run = metaflow_runner(BasicArtifactFlow, executor=executor)
            assert run.successful
            assert run.run()["end"].task.data.data == "abc"

    The returned :class:`FlowRun` object can also be used to drive a
    subsequent ``runner.resume(...)`` call.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METAFLOW_USER", os.environ.get("METAFLOW_USER", "tester"))
    monkeypatch.setenv("METAFLOW_CLICK_API_PROCESS_CONFIG", "0")
    pythonpath = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", f"{_CORE_DIR}{os.pathsep}{pythonpath}")

    # Expand {nonce}/{{nonce}} placeholders in env vars so cloud-backend
    # tests get a fresh datastore prefix per test invocation. The tox
    # setenv blocks intentionally write the placeholder; we substitute
    # it here per-test (matching the original run_tests.py behavior).
    nonce = uuid.uuid4().hex
    for k, v in list(os.environ.items()):
        if isinstance(v, str) and "{nonce}" in v:
            monkeypatch.setenv(k, v.replace("{nonce}", nonce))

    # Reset metaflow's parent-process caches so each test sees its own
    # .metaflow dir. Without these, Flow(name) lookups inherit the
    # metadata path bound by the first test in the worker.
    import metaflow.client.core as _mf_client_core
    from metaflow.plugins.datastores.local_storage import LocalStorage

    monkeypatch.setattr(_mf_client_core, "current_metadata", False)
    monkeypatch.setattr(_mf_client_core, "current_namespace", False)
    monkeypatch.setattr(LocalStorage, "datastore_root", None)

    runners: list = []

    def _run(
        flow_cls: type,
        *,
        executor: str = "cli",
        extra_run_options: Iterable[str] = (),
        resume: bool = False,
        resume_step: str | None = None,
        expect_fail: bool = False,
    ) -> FlowRun:
        flow_path = _write_flow_module(tmp_path, flow_cls)
        flow_name = flow_cls.__name__
        run_options = list(default_run_options) + list(extra_run_options)
        run_id_file = tmp_path / "run-id"
        # F3: ensure the run-id file is fresh — without this, a second
        # run() call (e.g. resume scenarios) returns the prior run's id
        # if the new invocation fails before rewriting it.
        run_id_file.unlink(missing_ok=True)

        if executor == "cli":
            cmd = (
                ["python3", "-B", str(flow_path)]
                + list(top_options)
                + (["resume"] + ([resume_step] if resume_step else []) if resume else ["run"])
                + ["--run-id-file", str(run_id_file)]
                + run_options
            )
            env = _build_subprocess_env()
            cp = subprocess.run(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            run_id = run_id_file.read_text().strip() if run_id_file.exists() else None
            return FlowRun(
                returncode=cp.returncode,
                run_id=run_id,
                flow_name=flow_name,
                stdout=cp.stdout.decode("utf-8", errors="replace"),
                stderr=cp.stderr.decode("utf-8", errors="replace"),
            )

        if executor == "scheduler":
            # Real argo / sfn scheduler integration: deploy + trigger +
            # poll Flow().finished. Implemented as a thin subprocess
            # adapter around the metaflow CLI's `argo-workflows` /
            # `step-functions` subcommands. Skipped at runtime when the
            # METAFLOW_CORE_SCHEDULER env var isn't set so the executor
            # fixture parametrisation doesn't fail-collect on a backend
            # that isn't actually argo/sfn.
            scheduler = os.environ.get("METAFLOW_CORE_SCHEDULER")
            if not scheduler:
                pytest.skip(
                    "scheduler executor selected but METAFLOW_CORE_SCHEDULER "
                    "is unset — skipping until argo/sfn infra is wired up."
                )
            return _run_scheduler(
                flow_path,
                flow_name,
                top_options,
                run_options,
                scheduler,
                run_id_file,
            )

        if executor == "api":
            from metaflow import Runner
            from metaflow.runner.click_api import (
                click_to_python_types,
                extract_all_params,
            )
            from metaflow.cli import start
            from metaflow.cli_components.run_cmds import run as run_cli

            top_kwargs = _click_args_to_kwargs(start, list(top_options))
            run_kwargs = _click_args_to_kwargs(run_cli, list(run_options))
            run_kwargs["run_id_file"] = str(run_id_file)
            if resume_step:
                run_kwargs["step_to_rerun"] = resume_step

            r = Runner(str(flow_path), show_output=False, env=os.environ.copy(), **top_kwargs)
            runners.append(r)
            try:
                if resume:
                    result = r.resume(**run_kwargs)
                else:
                    result = r.run(**run_kwargs)
                rc = result.command_obj.process.returncode
            except RuntimeError:
                rc = 1
            run_id = run_id_file.read_text().strip() if run_id_file.exists() else None
            return FlowRun(returncode=rc, run_id=run_id, flow_name=flow_name)

        raise ValueError(f"unknown executor {executor!r}")

    yield _run

    for r in runners:
        try:
            r.cleanup()
        except Exception:
            pass


def _run_scheduler(
    flow_path: Path,
    flow_name: str,
    top_options: Sequence[str],
    run_options: Sequence[str],
    scheduler: str,
    run_id_file: Path,
) -> "FlowRun":
    """Run a flow via argo-workflows or step-functions subcommands.

    Steps: ``<scheduler> create`` to deploy, ``<scheduler> trigger
    --run-id-file`` to start a run, then poll ``Flow(name)[run_id]``
    until ``finished`` or ``METAFLOW_CORE_SCHEDULER_TIMEOUT`` (default
    600 s) elapses.
    """
    env = _build_subprocess_env()
    base_cmd = ["python3", "-B", str(flow_path)] + list(top_options)
    create_cmd = base_cmd + [scheduler, "create"]
    cp_create = subprocess.run(
        create_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
    )
    if cp_create.returncode != 0:
        return FlowRun(
            returncode=cp_create.returncode,
            run_id=None,
            flow_name=flow_name,
            stdout=cp_create.stdout.decode("utf-8", errors="replace"),
            stderr=cp_create.stderr.decode("utf-8", errors="replace"),
        )

    trigger_cmd = base_cmd + [scheduler, "trigger", "--run-id-file", str(run_id_file)]
    cp_trigger = subprocess.run(
        trigger_cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if cp_trigger.returncode != 0:
        return FlowRun(
            returncode=cp_trigger.returncode,
            run_id=None,
            flow_name=flow_name,
            stdout=cp_trigger.stdout.decode("utf-8", errors="replace"),
            stderr=cp_trigger.stderr.decode("utf-8", errors="replace"),
        )

    run_id = run_id_file.read_text().strip() if run_id_file.exists() else None
    if run_id is None:
        return FlowRun(returncode=1, run_id=None, flow_name=flow_name)

    timeout = int(os.environ.get("METAFLOW_CORE_SCHEDULER_TIMEOUT", "600"))
    deadline = time.time() + timeout
    from metaflow import Flow

    while time.time() < deadline:
        try:
            run = Flow(flow_name, _namespace_check=False)[run_id]
            if run.finished:
                rc = 0 if run.successful else 1
                return FlowRun(returncode=rc, run_id=run_id, flow_name=flow_name)
        except Exception:
            pass
        time.sleep(10)
    return FlowRun(returncode=1, run_id=run_id, flow_name=flow_name)


def _click_args_to_kwargs(click_cmd, cli_options: Sequence[str]) -> dict:
    """Translate a CLI option list into Runner kwargs.

    Lifted from the original harness but isolated here so the rest of
    metaflow_runner stays readable.
    """
    from metaflow.runner.click_api import click_to_python_types, extract_all_params

    _, _, param_opts, _, _ = extract_all_params(click_cmd)
    out: dict = {}
    has_value = False
    secondary_supplied = False
    for arg in cli_options:
        if "=" in arg:
            given_opt, val = arg.split("=", 1)
            has_value = True
        else:
            given_opt = arg
            val = None
        for key, each_param in param_opts.items():
            py_type = click_to_python_types[type(each_param.type)]
            if given_opt in each_param.opts:
                secondary_supplied = False
            elif given_opt in each_param.secondary_opts:
                secondary_supplied = True
            else:
                continue
            value = val if has_value else (False if secondary_supplied else True)
            if each_param.multiple:
                out.setdefault(key, []).append(py_type(value))
            else:
                out[key] = py_type(value)
        has_value = False
        secondary_supplied = False
    return out
