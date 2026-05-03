"""
Core integration tests for Metaflow.

Each pytest item corresponds to one (graph, test, executor) combination.
All context configuration (Metaflow env vars, top_options, disabled tests, etc.)
comes from the environment — set by the tox env's setenv block. There is no
Python context file; the tox env IS the context.

Usage:
    tox -c test/core/tox.ini -e core-local       # local backend via tox
    tox -c test/core/tox.ini -e core-gcs         # gcs marker via tox
    pytest test/core/ -m local                   # local backend, all tests
    pytest test/core/ -m local -n auto           # parallel with xdist
    pytest test/core/ -m local \\
        --core-tests BasicArtifact \\
        --core-graphs single-linear-step         # targeted run
"""

import importlib.util
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import Tuple

import pytest

from metaflow._vendor import click
from metaflow.cli import start
from metaflow.cli_components.run_cmds import run
from metaflow_test.formatter import FlowFormatter

_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)

_skip_api_executor = False
try:
    from metaflow import Runner
    from metaflow.runner.click_api import click_to_python_types, extract_all_params
except ImportError:
    _skip_api_executor = True

_SASHIMI = "刺身 means sashimi"

_DEFAULT_RUN_OPTIONS = [
    "--max-workers=50",
    "--max-num-splits=10000",
    "--tag=%s" % _SASHIMI,
    "--tag=multiple tags should be ok",
]


def _log(msg, formatter=None, context=None, processes=None):
    parts = []
    if formatter:
        parts.append(str(formatter))
    if context:
        parts.append("context '%s'" % context["name"])
    prefix = " / ".join(parts)
    line = ("[%s] %s" % (prefix, msg)) if prefix else msg
    click.echo(line)
    if processes:
        for p in processes:
            if p.stdout:
                click.echo(p.stdout, nl=False)
            if p.stderr:
                click.echo(p.stderr, nl=False)


@contextmanager
def _isolated_client_globals():
    """Save and restore metaflow.client.core module-level globals.

    MetadataCheck.__init__ calls namespace() / default_namespace() which mutate
    current_namespace and current_metadata in metaflow.client.core.  Running
    checkers in-process (rather than in a check_flow.py subprocess) means those
    mutations would otherwise bleed across tests in the same worker process.

    We also save/restore the metadata provider's class-level _INFO cache.
    LocalMetadataProvider uses MetadataProviderMeta which caches the result of
    default_info() (based on os.getcwd()) in a class variable _INFO.  Without
    restoring it, test N's tempdir leaks into test N+1 after test N cleans up.
    """
    import metaflow.client.core as _core
    from metaflow.plugins.metadata_providers.local import (
        LocalMetadataProvider as _LMP,
    )

    saved_namespace = _core.current_namespace
    saved_metadata = _core.current_metadata
    # LocalMetadataProvider caches default_info() (os.getcwd()-based path) in
    # the class-level _INFO attribute via MetadataProviderMeta.  Without
    # restoring it, test N caches tempdir_N then deletes it, and test N+1
    # inherits the stale path → MetaflowNotFound.
    saved_lmp_info = _LMP._INFO
    try:
        yield
    finally:
        _core.current_namespace = saved_namespace
        _core.current_metadata = saved_metadata
        _LMP._INFO = saved_lmp_info


def _context_from_env() -> dict:
    """Build the context dict that _run_flow() expects, from tox setenv vars."""
    top_options = shlex.split(os.environ.get("METAFLOW_CORE_TOP_OPTIONS", ""))
    ctx = {
        "name": os.environ.get("METAFLOW_CORE_MARKER", "local"),
        "python": "python3",
        "top_options": top_options,
        "run_options": _DEFAULT_RUN_OPTIONS,
        # env is intentionally empty: all Metaflow config vars are already in
        # os.environ via tox setenv and will be inherited by _run_flow().
        "env": {},
    }
    scheduler = os.environ.get("METAFLOW_CORE_SCHEDULER", "")
    if scheduler:
        ctx["scheduler"] = scheduler
        ctx["scheduler_timeout"] = int(
            os.environ.get("METAFLOW_CORE_SCHEDULER_TIMEOUT", "600")
        )
    return ctx


def _run_flow(formatter, context, core_checks, env_base, executor):
    """Execute one (formatter, context, executor) test combination.

    Replaces the run_test() call that previously required importing run_tests.py.
    Returns (returncode, path_to_flow_file, error_details) where error_details
    is a human-readable string with subprocess stdout/stderr on failure (empty
    string on success).

    Fixes vs the original run_tests.run_test():
      - api executor: Runner.run/resume() RuntimeError caught and converted to
        a non-zero returncode instead of propagating as an unhandled exception.
      - resume path: adds an early return when the resume itself fails, preventing
        the subsequent open("run-id") from raising FileNotFoundError.
    """

    def run_cmd(mode, args=None):
        cmd = [context["python"], "-B", "test_flow.py"]
        cmd.extend(context["top_options"])
        cmd.append(mode)
        if args:
            cmd.extend(args)
        cmd.extend(("--run-id-file", "run-id"))
        cmd.extend(context["run_options"])
        return cmd

    def construct_arg_dict(params_opts, cli_options):
        result_dict = {}
        has_value = False
        secondary_supplied = False
        for arg in cli_options:
            if "=" in arg:
                given_opt, val = arg.split("=", 1)
                has_value = True
            else:
                given_opt = arg
            for key, each_param in params_opts.items():
                py_type = click_to_python_types[type(each_param.type)]
                if given_opt in each_param.opts:
                    secondary_supplied = False
                elif given_opt in each_param.secondary_opts:
                    secondary_supplied = True
                else:
                    continue
                value = val if has_value else (False if secondary_supplied else True)
                if each_param.multiple:
                    result_dict.setdefault(key, []).append(py_type(value))
                else:
                    result_dict[key] = py_type(value)
            has_value = False
            secondary_supplied = False
        return result_dict

    def construct_arg_dicts_from_click_api():
        _, _, param_opts, _, _ = extract_all_params(start)
        top_level_dict = construct_arg_dict(param_opts, context["top_options"])
        _, _, param_opts, _, _ = extract_all_params(run)
        run_level_dict = construct_arg_dict(param_opts, context["run_options"])
        run_level_dict["run_id_file"] = "run-id"
        return top_level_dict, run_level_dict

    cwd = os.getcwd()  # restored in finally so callers keep their original cwd
    runner = None  # defined before outer try so finally can always reference it
    tempdir = tempfile.mkdtemp("_metaflow_test")
    _success = False  # keep tempdir on failure so error messages remain valid
    try:
        os.chdir(tempdir)
        with open("test_flow.py", "w") as f:
            f.write(formatter.flow_code)
        shutil.copytree(
            os.path.join(_CORE_DIR, "metaflow_test"),
            os.path.join(tempdir, "metaflow_test"),
        )
        for file in formatter.copy_files:
            shutil.copy2(
                os.path.join(_CORE_DIR, "tests", file),
                os.path.join(tempdir, file),
            )

        path = os.path.join(tempdir, "test_flow.py")
        original_env = os.environ.copy()
        try:
            nonce = str(uuid.uuid4())

            # Build a hermetic subprocess env from only the vars that tox
            env = dict(original_env)

            env.update(env_base)
            for k, v in context.get("env", {}).items():
                env[k] = v.format(nonce=nonce)
            # Expand {nonce} placeholders written as {{nonce}} in tox.ini setenv.
            # Use str.replace (not .format) to avoid KeyError on JSON-valued vars.
            # Replace {{nonce}} first so the double-brace form is fully consumed
            # before the single-brace form is handled.  If tox 4 converts {{
            # to { before the test runs, only the second replace fires; if it
            # does not, the first replace removes both surrounding braces.
            for k, v in list(env.items()):
                if isinstance(v, str) and "{nonce}" in v:
                    env[k] = v.replace("{{nonce}}", nonce).replace("{nonce}", nonce)

            pythonpath = original_env.get("PYTHONPATH", ".")
            env.update(
                {
                    "LANG": "en_US.UTF-8",
                    "LC_ALL": "en_US.UTF-8",
                    "PATH": original_env.get("PATH", "."),
                    "PYTHONIOENCODING": "utf_8",
                    "PYTHONPATH": "%s:%s" % (_CORE_DIR, pythonpath),
                }
            )
            # Preserve pytest-cov / coverage.py state.  Both read COVERAGE_FILE
            # and COV_CORE_* lazily while a test is running; clearing os.environ
            # before they do causes silent coverage loss (not a test failure).
            # PYTEST_* vars (e.g. PYTEST_CURRENT_TEST) are also needed by some
            # pytest plugins throughout the test body.
            _cov_prefixes = ("COV", "COVERAGE_", "PYTEST_")
            _saved_cov = {
                k: v
                for k, v in original_env.items()
                if any(k.startswith(p) for p in _cov_prefixes)
            }
            os.environ.clear()
            os.environ.update(env)
            os.environ.update(_saved_cov)

            called_processes = []
            _error_details = []  # accumulates stderr/stdout from failed subprocesses

            def _proc_output(procs):
                """Return a single string with stdout+stderr from all processes."""
                parts = []
                for p in procs:
                    if p.stdout:
                        out = (
                            p.stdout.decode("utf-8", errors="replace")
                            if isinstance(p.stdout, bytes)
                            else p.stdout
                        )
                        if out.strip():
                            parts.append("stdout:\n" + out)
                    if p.stderr:
                        err = (
                            p.stderr.decode("utf-8", errors="replace")
                            if isinstance(p.stderr, bytes)
                            else p.stderr
                        )
                        if err.strip():
                            parts.append("stderr:\n" + err)
                return "\n".join(parts)

            # ----------------------------------------------------------------
            # Run the flow
            # ----------------------------------------------------------------
            if executor == "cli":
                called_processes.append(
                    subprocess.run(
                        run_cmd("run"),
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
            elif executor == "api":
                top_level_dict, run_level_dict = construct_arg_dicts_from_click_api()
                runner = Runner(
                    "test_flow.py", show_output=False, env=env, **top_level_dict
                )
                # Runner.run() raises RuntimeError when the subprocess fails.
                # Catch it and convert to a non-zero CompletedProcess so the
                # rest of _run_flow() handles all executor paths uniformly.
                try:
                    result = runner.run(**run_level_dict)
                    with open(
                        result.command_obj.log_files["stdout"], encoding="utf-8"
                    ) as f:
                        stdout = f.read()
                    with open(
                        result.command_obj.log_files["stderr"], encoding="utf-8"
                    ) as f:
                        stderr = f.read()
                    called_processes.append(
                        subprocess.CompletedProcess(
                            result.command_obj.command,
                            result.command_obj.process.returncode,
                            stdout,
                            stderr,
                        )
                    )
                except RuntimeError as e:
                    _log("api executor failed: %s" % e, formatter, context)
                    called_processes.append(
                        subprocess.CompletedProcess([], 1, b"", b"")
                    )
            elif executor == "scheduler":
                scheduler = context.get("scheduler")
                if not scheduler:
                    raise ValueError(
                        "Context %s uses 'scheduler' executor but has no 'scheduler' key"
                        % context["name"]
                    )
                if formatter.should_resume:
                    _log(
                        "skipping resume test (not supported by scheduler executor)",
                        formatter,
                        context,
                    )
                    return 0, path, ""

                create_cmd = [context["python"], "-B", "test_flow.py"]
                create_cmd.extend(context["top_options"])
                create_cmd.extend([scheduler, "create"])
                called_processes.append(
                    subprocess.run(
                        create_cmd,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
                if called_processes[-1].returncode:
                    _log(
                        "scheduler create failed",
                        formatter,
                        context,
                        processes=called_processes,
                    )
                    _error_details.append("scheduler create failed")
                    _error_details.append(_proc_output(called_processes))
                    return (
                        called_processes[-1].returncode,
                        path,
                        "\n".join(_error_details),
                    )

                trigger_cmd = [context["python"], "-B", "test_flow.py"]
                trigger_cmd.extend(context["top_options"])
                trigger_cmd.extend([scheduler, "trigger", "--run-id-file", "run-id"])
                called_processes.append(
                    subprocess.run(
                        trigger_cmd,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                )
                if called_processes[-1].returncode:
                    if not formatter.should_fail:
                        _log(
                            "scheduler trigger failed",
                            formatter,
                            context,
                            processes=called_processes,
                        )
                        _error_details.append("scheduler trigger failed")
                        _error_details.append(_proc_output(called_processes))
                        return (
                            called_processes[-1].returncode,
                            path,
                            "\n".join(_error_details),
                        )
                elif formatter.should_fail:
                    return 1, path, ""

                run_id = open("run-id").read().strip()
                timeout = context.get("scheduler_timeout", 600)
                deadline = time.time() + timeout
                run_succeeded = None
                from metaflow import Flow

                while time.time() < deadline:
                    try:
                        flow_run = Flow(formatter.flow_name, _namespace_check=False)[
                            run_id
                        ]
                        if flow_run.finished:
                            run_succeeded = flow_run.successful
                            break
                    except Exception:
                        pass
                    time.sleep(10)

                if run_succeeded is None:
                    _log(
                        "scheduler run timed out after %ds" % timeout,
                        formatter,
                        context,
                        processes=called_processes,
                    )
                    _error_details.append("scheduler run timed out after %ds" % timeout)
                    return 1, path, "\n".join(_error_details)

                called_processes.append(
                    subprocess.CompletedProcess(
                        trigger_cmd, 0 if run_succeeded else 1, b"", b""
                    )
                )

            # ----------------------------------------------------------------
            # Handle first-run outcome
            # ----------------------------------------------------------------
            if called_processes[-1].returncode:
                if formatter.should_fail:
                    pass  # expected failure, fall through to check results
                elif formatter.should_resume:
                    _log("Resuming flow as expected", formatter, context)
                    if executor == "cli":
                        called_processes.append(
                            subprocess.run(
                                run_cmd(
                                    "resume",
                                    (
                                        [formatter.resume_step]
                                        if formatter.resume_step
                                        else []
                                    ),
                                ),
                                env=env,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                check=False,
                            )
                        )
                    elif executor == "api":
                        _, resume_level_dict = construct_arg_dicts_from_click_api()
                        if formatter.resume_step:
                            resume_level_dict["step_to_rerun"] = formatter.resume_step
                        try:
                            result = runner.resume(**resume_level_dict)
                            with open(
                                result.command_obj.log_files["stdout"], encoding="utf-8"
                            ) as f:
                                stdout = f.read()
                            with open(
                                result.command_obj.log_files["stderr"], encoding="utf-8"
                            ) as f:
                                stderr = f.read()
                            called_processes[-1] = subprocess.CompletedProcess(
                                result.command_obj.command,
                                result.command_obj.process.returncode,
                                stdout,
                                stderr,
                            )
                        except RuntimeError as e:
                            _log("api resume failed: %s" % e, formatter, context)
                            called_processes[-1] = subprocess.CompletedProcess(
                                [], 1, b"", b""
                            )
                    # Guard: if the resume itself failed, return early so we never
                    # reach open("run-id") on a file that was never written.
                    if called_processes[-1].returncode:
                        _log(
                            "resume failed",
                            formatter,
                            context,
                            processes=called_processes,
                        )
                        _error_details.append("resume failed")
                        _error_details.append(_proc_output(called_processes))
                        return (
                            called_processes[-1].returncode,
                            path,
                            "\n".join(_error_details),
                        )
                else:
                    _log("flow failed", formatter, context, processes=called_processes)
                    _error_details.append("flow failed")
                    _error_details.append(_proc_output(called_processes))
                    return (
                        called_processes[-1].returncode,
                        path,
                        "\n".join(_error_details),
                    )
            elif formatter.should_fail:
                return 1, path, ""

            # ----------------------------------------------------------------
            # Check results — run in-process; failures raise AssertionError
            # ----------------------------------------------------------------
            run_id = open("run-id").read().strip()

            # Dynamically import the generated flow class from test_flow.py.
            # We are already os.chdir'd to tempdir so the path is reachable.
            _mod_name = "_core_test_flow_%s_%s" % (formatter.flow_name, id(formatter))
            _spec = importlib.util.spec_from_file_location(_mod_name, "test_flow.py")
            _flow_module = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_flow_module)
            flow = getattr(_flow_module, formatter.flow_name)(use_cli=False)
            sys.modules.pop(_mod_name, None)

            from metaflow_test import new_checker

            for check_spec in core_checks.values():
                with _isolated_client_globals():
                    checker = new_checker(
                        check_spec["class"], flow, run_id, context["top_options"]
                    )
                    formatter.test.check_results(flow, checker)

            ret = 0
            _success = True
        finally:
            if runner is not None:
                runner.cleanup()
            os.environ.clear()
            os.environ.update(original_env)
            # Reset LocalMetadataProvider and LocalStorage class-level caches so
            # each test starts with a clean slate.  Both are set lazily from
            # os.getcwd() the first time they are accessed; stale values from a
            # previous test's (now-deleted) tempdir cause MetaflowNotFound in the
            # next test's in-process MetadataCheck.
            try:
                from metaflow.plugins.metadata_providers.local import (
                    LocalMetadataProvider as _LMP,
                )
                from metaflow.plugins.datastores.local_storage import (
                    LocalStorage as _LS,
                )

                _LMP._INFO = None
                _LS.datastore_root = None
            except ImportError:
                pass

        return ret, path, ""
    finally:
        os.chdir(cwd)
        if _success:
            shutil.rmtree(tempdir, ignore_errors=True)
        # on failure, tempdir is kept so the path in pytest.fail(...) is still valid


def test_flow_triple(flow_triple: Tuple, core_checks: dict) -> None:
    """Run one (graph, test, executor) combination.

    Each item runs as an independent pytest test, enabling parallel execution
    via pytest-xdist and per-test timeout/failure isolation.

    core_checks is injected from the session-scoped fixture in conftest.py;
    override it there to restrict or extend which checkers run.
    """
    graph, test, executor = flow_triple

    if executor == "api" and _skip_api_executor:
        pytest.skip("metaflow.Runner not available — skipping api executor")

    context = _context_from_env()

    # METAFLOW_USER must be set before metaflow imports so that the cached
    # USER value is non-root (required for the api executor on root hosts).
    env_base = {
        "METAFLOW_CLICK_API_PROCESS_CONFIG": "0",
        "METAFLOW_TEST_PRINT_FLOW": "1",
        "METAFLOW_USER": os.environ.get("METAFLOW_USER", "tester"),
    }

    formatter = FlowFormatter(graph, test)
    ret, path, details = _run_flow(
        formatter=formatter,
        context=context,
        core_checks=core_checks,
        env_base=env_base,
        executor=executor,
    )

    if ret != 0:
        marker = os.environ.get("METAFLOW_CORE_MARKER", "local")
        msg = "Core test failed: %s/%s/%s/%s\n  flow path: %s" % (
            marker,
            graph["name"],
            test.__class__.__name__,
            executor,
            path,
        )
        if details:
            msg += "\n\n" + details
        pytest.fail(msg)
