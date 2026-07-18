from metaflow.mflog import publisher_health
from metaflow.mflog.mflog import parse


def test_uploader_log_is_written_to_task_stderr(monkeypatch, tmp_path):
    task_stderr = tmp_path / "mflog_stderr"
    monkeypatch.setenv("MFLOG_STDERR", str(task_stderr))

    written = publisher_health.write_uploader_log("publisher heartbeat")

    payload = task_stderr.read_bytes()
    assert written == len(payload)
    assert parse(payload).msg == b"publisher heartbeat"
