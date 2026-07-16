import json
import threading
from types import SimpleNamespace

from metaflow.mflog import publisher_health
from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.sidecar import Sidecar


def test_health_event_is_written_outside_task_stderr(monkeypatch, tmp_path):
    task_stderr = tmp_path / "mflog_stderr"
    monkeypatch.setenv(publisher_health.HEALTH_ENABLED_ENV_VAR, "1")
    monkeypatch.setenv("MFLOG_STDERR", str(task_stderr))
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/step/task")
    monkeypatch.setenv("MF_ATTEMPT", "0")

    publisher_health.write_health_event("publisher_heartbeat", thread_alive=True)

    health_path = tmp_path / publisher_health.DEFAULT_HEALTH_FILENAME
    record = json.loads(health_path.read_text().strip())
    assert health_path != task_stderr
    assert record["event"] == "publisher_heartbeat"
    assert record["pathspec"] == "Flow/1/step/task"
    assert record["thread_alive"] is True


def test_upload_failure_records_return_code(monkeypatch, mocker):
    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._health_logging_enabled = True
    publisher._state_lock = threading.Lock()
    publisher._last_upload_at = None
    publisher._last_success_at = None
    publisher._last_upload_status = None
    publisher._upload_started_at = None
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.call", return_value=70
    )
    write_event = mocker.patch(
        "metaflow.mflog.save_logs_periodically.write_health_event"
    )

    return_code = publisher._upload_logs([10, 20])

    assert return_code == 70
    assert publisher._last_upload_status == "failure"
    assert write_event.call_args_list[-1].args[0] == "upload_failure"
    assert write_event.call_args_list[-1].kwargs["return_code"] == 70


def test_process_monitor_records_unexpected_exit(mocker):
    class StopAfterWait:
        stopped = False

        def is_set(self):
            return self.stopped

        def set(self):
            self.stopped = True

        def wait(self, timeout):
            self.stopped = True

    health = SimpleNamespace(
        PROCESS_POLL_INTERVAL_SECONDS=0,
        write_health_event=mocker.Mock(),
    )
    sidecar = Sidecar.__new__(Sidecar)
    sidecar.sidecar_process = SimpleNamespace(
        _process=SimpleNamespace(pid=1234, poll=lambda: 9)
    )
    sidecar._publisher_health = health
    sidecar._publisher_monitor_stop = StopAfterWait()
    sidecar._publisher_shutdown_requested = False

    sidecar._monitor_publisher_process()

    health.write_health_event.assert_called_once_with(
        "publisher_process_exit",
        publisher_pid=1234,
        return_code=9,
        expected=False,
    )


def test_health_loop_records_dead_publisher_thread(mocker):
    class StopAfterWait:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, timeout):
            self.stopped = True

    publisher = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    publisher._health_stop = StopAfterWait()
    publisher._thread = SimpleNamespace(is_alive=lambda: False)
    publisher._state_lock = threading.Lock()
    publisher._upload_started_at = None
    publisher._last_upload_status = "success"
    publisher._last_upload_at = None
    publisher._last_success_at = None
    publisher._thread_dead_reported = False
    publisher.is_alive = True
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.health_interval_seconds",
        return_value=0.1,
    )
    write_event = mocker.patch(
        "metaflow.mflog.save_logs_periodically.write_health_event"
    )

    publisher._health_loop()

    assert [call.args[0] for call in write_event.call_args_list] == [
        "publisher_heartbeat",
        "publisher_thread_dead",
    ]
