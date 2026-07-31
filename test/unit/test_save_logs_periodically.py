import os
import sys
import textwrap
from types import SimpleNamespace

import metaflow.mflog.save_logs_periodically as save_logs_periodically_module
from metaflow.mflog import BASH_SAVE_LOGS_ARGS
from metaflow.mflog.mflog import parse
from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar


def _new_sidecar(enable_tracing):
    sidecar = SaveLogsPeriodicallySidecar.__new__(SaveLogsPeriodicallySidecar)
    sidecar._enable_tracing = enable_tracing
    return sidecar


def _read_uploader_messages(path):
    if not path.exists():
        return []
    return [
        parse(line).msg.decode()
        for line in path.read_bytes().splitlines(keepends=True)
        if line.startswith(b"[MFLOG|")
    ]


def _patch_save_logs_child_process(monkeypatch, tmp_path):
    # sidecar_subprocess starts a subprocess. That subprocess runs python,
    # which runs metaflow.mflog.save_logs. That python file has the upload code,
    # and for s3 datastore the upload code will call s3 api.
    # This sitecustomize mocks that child python setup, so the child raises from
    # the fake s3 api and the test can verify the exception is captured.
    sitecustomize_dir = tmp_path / "child_pythonpath"
    sitecustomize_dir.mkdir()
    (sitecustomize_dir / "sitecustomize.py").write_text(
        textwrap.dedent(
            """
            class FakeS3Storage(object):
                TYPE = "s3"

                @classmethod
                def get_datastore_root_from_config(cls, echo, create_on_absent=True):
                    return "s3://metaflow-test"


            class S3ApiFailure(Exception):
                pass


            class FakeS3Api(object):
                def put_logs(self):
                    raise S3ApiFailure("PutObject failed with AccessDenied")


            class FakeTaskDataStore(object):
                def save_logs(self, logsource, stream_data):
                    FakeS3Api().put_logs()


            class FakeFlowDataStore(object):
                def __init__(self, *args, **kwargs):
                    pass

                def get_task_datastore(self, *args, **kwargs):
                    return FakeTaskDataStore()


            import metaflow.datastore
            import metaflow.plugins

            metaflow.datastore.FlowDataStore = FakeFlowDataStore
            metaflow.plugins.DATASTORES = [FakeS3Storage]
            """
        )
    )
    # BASH_SAVE_LOGS_ARGS uses "python", so put a test python first in PATH.
    # This makes the subprocess use the same env as the test, plus the mock
    # setup above from sitecustomize.
    python_bin = tmp_path / "bin"
    python_bin.mkdir()
    python = python_bin / "python"
    python.write_text("#!/bin/sh\nexec %s \"$@\"\n" % sys.executable)
    python.chmod(0o755)
    monkeypatch.setenv(
        "PATH",
        os.pathsep.join([str(python_bin), os.environ.get("PATH", "")]),
    )
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(
            [
                str(sitecustomize_dir),
                os.getcwd(),
                os.environ.get("PYTHONPATH", ""),
            ]
        ),
    )


def _configure_child_save_logs_env(monkeypatch, tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    stdout.write_bytes(b"out\n")
    stderr.write_bytes(b"err\n")
    monkeypatch.setenv("MF_PATHSPEC", "Flow/1/step/task")
    monkeypatch.setenv("MF_ATTEMPT", "0")
    monkeypatch.setenv("MF_DATASTORE", "s3")
    monkeypatch.setenv("MF_DATASTORE_ROOT", "s3://metaflow-test")
    monkeypatch.setenv("MFLOG_STDOUT", str(stdout))
    monkeypatch.setenv("MFLOG_STDERR", str(stderr))


def test_sidecar_tracing_defaults_to_false(monkeypatch, mocker, tmp_path):
    mocker.patch.object(save_logs_periodically_module, "Thread")
    # Verify that the environment variable alone won't accidentally enable tracing.
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(tmp_path / "trace"))

    sidecar = SaveLogsPeriodicallySidecar()

    assert sidecar._enable_tracing is False


def test_init_enables_tracing_from_options(mocker):
    mocker.patch.object(save_logs_periodically_module, "Thread")

    sidecar = SaveLogsPeriodicallySidecar(options={"enable_tracing": True})

    assert sidecar._enable_tracing is True


def test_call_save_logs_captures_upload_failure_diagnostics(
    monkeypatch, mocker, tmp_path
):
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    sidecar = _new_sidecar(True)
    process = SimpleNamespace(
        communicate=mocker.Mock(
            return_value=(
                b"[save_logs] upload_start datastore=s3 files=[]\n",
                b"[save_logs] upload_failure datastore=s3 files=[] "
                b"error=S3ApiFailure('PutObject failed') "
                b"elapsed_seconds=0.001\n",
            )
        ),
        returncode=0,
    )
    popen = mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.Popen",
        return_value=process,
    )

    returncode = sidecar._call_save_logs()

    assert returncode == 0
    messages = _read_uploader_messages(uploader_stdout)
    assert messages == [
        "[save_logs stdout] [save_logs] upload_start datastore=s3 files=[]",
        "[save_logs stderr] [save_logs] upload_failure datastore=s3 files=[] "
        "error=S3ApiFailure('PutObject failed') elapsed_seconds=0.001",
    ]
    popen.assert_called_once_with(
        BASH_SAVE_LOGS_ARGS,
        stdout=save_logs_periodically_module.subprocess.PIPE,
        stderr=save_logs_periodically_module.subprocess.PIPE,
    )
    process.communicate.assert_called_once_with()


def test_call_save_logs_captures_s3_api_failure_from_child_process(
    monkeypatch, tmp_path
):
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    _configure_child_save_logs_env(monkeypatch, tmp_path)
    _patch_save_logs_child_process(monkeypatch, tmp_path)
    sidecar = _new_sidecar(True)

    returncode = sidecar._call_save_logs()

    assert returncode == 0
    messages = _read_uploader_messages(uploader_stdout)
    assert len(messages) == 2
    assert messages[0].startswith("[save_logs stdout] [save_logs] upload_start ")
    assert "datastore=s3" in messages[0]
    assert messages[1].startswith("[save_logs stderr] [save_logs] upload_failure ")
    assert "datastore=s3" in messages[1]
    assert "S3ApiFailure('PutObject failed with AccessDenied')" in messages[1]


def test_call_save_logs_confirms_absence_of_logs_when_child_crashes(
    monkeypatch, mocker, tmp_path
):
    uploader_stdout = tmp_path / "periodical_uploader_stdout"
    monkeypatch.setenv("PERIODICAL_UPLOADER_STDOUT", str(uploader_stdout))
    sidecar = _new_sidecar(True)
    process = SimpleNamespace(
        communicate=mocker.Mock(return_value=(b"", b"")),
        returncode=-9,
    )
    mocker.patch(
        "metaflow.mflog.save_logs_periodically.subprocess.Popen",
        return_value=process,
    )

    returncode = sidecar._call_save_logs()

    assert returncode == -9
    assert _read_uploader_messages(uploader_stdout) == []
    process.communicate.assert_called_once_with()
