import os
import signal
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import metaflow.task as task_module


@pytest.fixture
def flow():
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    try:
        yield SimpleNamespace(_task_ok=True, _success=True)
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_sigterm_unwinds_finalization_and_marks_attempt_failed(flow):
    finalized = []
    with pytest.raises(SystemExit) as result:
        with task_module._handle_task_sigterm(flow):
            try:
                os.kill(os.getpid(), signal.SIGTERM)
            finally:
                finalized.append(True)

    assert result.value.code == 143
    assert finalized == [True]
    assert flow._task_ok is False and flow._success is False
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL


def test_second_sigterm_during_cleanup_terminates_process():
    source = textwrap.dedent(
        """
        import os
        import signal
        from types import SimpleNamespace

        from metaflow.task import _handle_task_sigterm

        flow = SimpleNamespace(_task_ok=True, _success=True)
        with _handle_task_sigterm(flow):
            try:
                os.kill(os.getpid(), signal.SIGTERM)
            except SystemExit:
                print("finalizing", flush=True)
                os.kill(os.getpid(), signal.SIGTERM)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == -signal.SIGTERM
    assert result.stdout == "finalizing\n"


def test_custom_handler_remains_in_control(flow):
    observed = []

    def custom(signum, frame):
        observed.append(signum)

    signal.signal(signal.SIGTERM, custom)
    with task_module._handle_task_sigterm(flow):
        assert signal.getsignal(signal.SIGTERM) is custom
        os.kill(os.getpid(), signal.SIGTERM)
    assert observed == [signal.SIGTERM]
    assert signal.getsignal(signal.SIGTERM) is custom


def test_ignored_sigterm_is_preserved(flow):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with task_module._handle_task_sigterm(flow):
        os.kill(os.getpid(), signal.SIGTERM)
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN


def test_workload_can_replace_handler(flow):
    def custom(signum, frame):
        pass

    with task_module._handle_task_sigterm(flow):
        signal.signal(signal.SIGTERM, custom)
    assert signal.getsignal(signal.SIGTERM) is custom


def test_exception_restores_default_handler(flow):
    with pytest.raises(RuntimeError):
        with task_module._handle_task_sigterm(flow):
            raise RuntimeError("workload failed")
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL


def test_worker_thread_does_not_install_process_signal_handler(flow):
    def run():
        with task_module._handle_task_sigterm(flow):
            return signal.getsignal(signal.SIGTERM)

    with ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(run).result(timeout=2) == signal.SIG_DFL


@pytest.mark.parametrize(
    "decorator,terminate,phase",
    [
        ("", True, "step"),
        ("@catch", True, "step"),
        ("@catch", False, "step"),
        ("", True, "persist"),
        ("@catch", True, "finished"),
    ],
    ids=[
        "sigterm",
        "sigterm-with-catch",
        "normal-with-catch",
        "sigterm-during-persist",
        "sigterm-during-finished",
    ],
)
def test_task_subprocess_finalization_and_status(tmp_path, decorator, terminate, phase):
    source = tmp_path / "sigterm_flow.py"
    source.write_text(
        textwrap.dedent(
            """
            import os
            import signal
            from metaflow import FlowSpec, catch, step
            from metaflow.datastore.task_datastore import TaskDataStore
            from metaflow.decorators import StepDecorator

            if "{phase}" == "persist":
                original = TaskDataStore.persist
                def persist(self, flow):
                    if getattr(flow, "_current_step", None) == "start":
                        os.kill(os.getpid(), signal.SIGTERM)
                    return original(self, flow)
                TaskDataStore.persist = persist
            elif "{phase}" == "finished":
                original = StepDecorator.task_finished
                def finished(self, step_name, *args, **kwargs):
                    if step_name == "start":
                        os.kill(os.getpid(), signal.SIGTERM)
                    return original(self, step_name, *args, **kwargs)
                StepDecorator.task_finished = finished

            class SigtermFlow(FlowSpec):
                {decorator}
                @step
                def start(self):
                    if {terminate} and "{phase}" == "step":
                        os.kill(os.getpid(), signal.SIGTERM)
                    self.next(self.end)

                @step
                def end(self):
                    print("REACHED_END")

            if __name__ == "__main__":
                SigtermFlow()
            """
        ).format(decorator=decorator, terminate=terminate, phase=phase)
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("METAFLOW_")
    }
    env.update(
        {
            "PYTHONPATH": str(Path(task_module.__file__).parents[1]),
            "METAFLOW_HOME": str(tmp_path / "config"),
            "METAFLOW_DEFAULT_METADATA": "local",
            "METAFLOW_DEFAULT_DATASTORE": "local",
            "METAFLOW_DEFAULT_ENVIRONMENT": "local",
            "METAFLOW_DEBUG_SIDECAR": "1",
            "METAFLOW_DEFAULT_MONITOR": "nullSidecarMonitor",
            "METAFLOW_USER": "test",
            "USER": "test",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            str(source),
            "--no-pylint",
            "--event-logger",
            "debugLogger",
            "run",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    logs = result.stdout + result.stderr
    interrupted = terminate and phase == "step"
    assert (result.returncode != 0) == (interrupted and not decorator), logs
    assert ("REACHED_END" in logs) == (not interrupted or bool(decorator)), logs
    assert "'module': 'metaflow.task', 'name': 'end'" in logs, logs
    assert "'module': 'metaflow.task', 'name': 'duration'" in logs, logs
    datastore = tmp_path / ".metaflow" / "SigtermFlow"
    statuses = [
        json.loads(path.read_text())
        for path in datastore.rglob("sysmeta_attempt_ok*.json")
    ]
    assert [
        row["value"]
        for row in statuses
        if row["step_name"] == "start" and "attempt_id:0" in row["tags"]
    ] == [str(not interrupted)], (statuses, logs)
    attempts = [
        json.loads(path.read_text())
        for path in datastore.rglob("0.runtime")
        if path.parent.parent.name == "start"
    ]
    assert len(attempts) == 1, (attempts, logs)
    assert attempts[0]["return_code"] == (143 if interrupted else 0), (attempts, logs)
    assert attempts[0]["success"] is (not interrupted), (attempts, logs)
