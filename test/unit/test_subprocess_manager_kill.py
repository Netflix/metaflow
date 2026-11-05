import subprocess
import time

import pytest

from metaflow.runner.subprocess_manager import kill_processes_and_descendants


@pytest.fixture
def spawn():
    procs = []

    def _spawn(*cmd):
        proc = subprocess.Popen(cmd)
        procs.append(proc)
        return proc

    yield _spawn
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait()


def _wait_for_exit(proc, timeout=5):
    deadline = time.time() + timeout
    while proc.poll() is None and time.time() < deadline:
        time.sleep(0.05)
    return proc.poll()


def test_kill_terminates_process_without_children(spawn):
    # pkill exits non-zero when nothing matches -P <pid>. That must not stop
    # the process itself from being signalled.
    proc = spawn("sleep", "30")

    kill_processes_and_descendants([str(proc.pid)], termination_timeout=0.1)

    assert _wait_for_exit(proc) is not None


def test_kill_terminates_process_with_children(spawn):
    proc = spawn("bash", "-c", "sleep 30 & wait")
    time.sleep(0.2)

    kill_processes_and_descendants([str(proc.pid)], termination_timeout=0.1)

    assert _wait_for_exit(proc) is not None
