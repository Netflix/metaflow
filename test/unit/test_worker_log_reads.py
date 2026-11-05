import subprocess
import sys
import time

import pytest

from metaflow import runtime
from metaflow.runtime import Worker


@pytest.fixture
def make_worker(mocker):
    procs = []

    def _make(script):
        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", script],
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append(proc)
        mocker.patch.object(Worker, "_launch", return_value=proc)
        task = mocker.MagicMock(retries=0, user_code_retries=0, is_cloned=False)
        worker = Worker(task, 1024 * 1024, None)
        worker.logged = lambda: [
            call.args[0] for call in task.log.call_args_list if call.kwargs.get("pid")
        ]
        return worker, proc

    yield _make
    for proc in procs:
        proc.kill()
        proc.wait()


def _drain_until(worker, fd, condition, timeout=5):
    deadline = time.time() + timeout
    while not condition() and time.time() < deadline:
        worker.read_logline(fd)
        time.sleep(0.01)


def test_partial_line_does_not_block_the_runtime(make_worker):
    worker, proc = make_worker(
        "import sys, time; sys.stdout.write('partial'); time.sleep(30)"
    )
    fd = proc.stdout.fileno()
    time.sleep(0.5)

    start = time.time()
    worker.read_logline(fd)

    assert time.time() - start < 1


def test_lines_written_together_are_all_emitted_without_more_output(make_worker):
    worker, proc = make_worker(
        "import sys, time\n"
        "sys.stdout.write(''.join('line %d\\n' % i for i in range(30)))\n"
        "time.sleep(30)"
    )
    time.sleep(0.5)

    # The poller only fires again when more data reaches the pipe, so a single
    # read has to deliver everything that is already available.
    worker.read_logline(proc.stdout.fileno())

    emitted = [line for line in worker.logged() if line.startswith("line")]
    assert emitted == ["line %d" % i for i in range(30)]


def test_line_split_across_reads_is_emitted_once(make_worker):
    worker, proc = make_worker(
        "import sys, time\n"
        "sys.stdout.write('hello ')\n"
        "time.sleep(0.2)\n"
        "sys.stdout.write('world\\n')\n"
        "time.sleep(30)"
    )
    fd = proc.stdout.fileno()

    _drain_until(worker, fd, lambda: "hello world" in worker.logged())

    assert worker.logged().count("hello world") == 1


def test_trailing_partial_line_is_kept_when_task_exits(make_worker):
    worker, proc = make_worker("import sys; sys.stdout.write('no newline')")
    proc.wait()

    worker.terminate()

    assert "no newline" in worker.logged()


def test_partial_line_buffer_is_bounded(make_worker, monkeypatch):
    monkeypatch.setattr(runtime, "MAX_PARTIAL_LOG_LINE", 10)
    worker, proc = make_worker(
        "import sys, time; sys.stdout.write('x' * 50); time.sleep(30)"
    )
    fd = proc.stdout.fileno()

    _drain_until(worker, fd, lambda: "x" * 50 in worker.logged())

    assert "x" * 50 in worker.logged()
    assert worker._partial_lines[fd] == b""
