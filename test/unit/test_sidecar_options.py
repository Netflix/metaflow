import json
from types import SimpleNamespace

import pytest

from metaflow.mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.sidecar import Sidecar
from metaflow.sidecar.sidecar_subprocess import SidecarSubProcess
from metaflow.sidecar.sidecar_worker import deserialize_options, instantiate_worker


def test_sidecar_passes_detached_options_to_subprocess(mocker):
    options = {"enabled": True, "nested": {"value": 1}}
    subprocess = mocker.patch("metaflow.sidecar.sidecar.SidecarSubProcess")
    sidecar = Sidecar("heartbeat", options=options)
    options["nested"]["value"] = 2

    sidecar.start()

    subprocess.assert_called_once_with(
        "heartbeat", options={"enabled": True, "nested": {"value": 1}}
    )


@pytest.mark.parametrize(
    "options",
    ["enabled", {"callback": lambda: None}],
    ids=["not-a-dictionary", "not-json-serializable"],
)
def test_sidecar_rejects_invalid_options(options):
    with pytest.raises(TypeError, match="Sidecar options"):
        Sidecar("heartbeat", options=options)


def test_subprocess_serializes_options_on_command_line(mocker):
    sidecar = SidecarSubProcess.__new__(SidecarSubProcess)
    sidecar._worker_type = "save_logs_periodically"
    sidecar._options = {"enable_tracing": True}
    sidecar._logger = mocker.Mock()
    start_subprocess = mocker.patch.object(
        sidecar, "_start_subprocess", return_value=None
    )

    sidecar.start()

    command = start_subprocess.call_args.args[0]
    assert json.loads(command[-1]) == {"enable_tracing": True}


def test_worker_receives_deserialized_options():
    class Worker(object):
        def __init__(self, options=None):
            self.options = options

    provider = SimpleNamespace(get_worker=lambda: Worker)

    worker = instantiate_worker(
        provider, deserialize_options('{"enable_tracing": true}')
    )

    assert worker.options == {"enable_tracing": True}


def test_worker_without_options_uses_legacy_constructor():
    class Worker(object):
        def __init__(self):
            self.started = True

    provider = SimpleNamespace(get_worker=lambda: Worker)

    worker = instantiate_worker(provider, {})

    assert worker.started is True


def test_monitor_records_unexpected_publisher_exit(mocker):
    class StopAfterOnePoll:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, timeout):
            self.stopped = True

    process = SimpleNamespace(pid=1234, poll=lambda: 9)
    sidecar = Sidecar.__new__(Sidecar)
    sidecar.sidecar_process = SimpleNamespace(_process=process)
    sidecar._debug_hooks = SimpleNamespace(
        PROCESS_POLL_INTERVAL_SECONDS=0,
        _trace=mocker.Mock(),
        _count=mocker.Mock(),
    )
    sidecar._monitor_stop = StopAfterOnePoll()
    sidecar._shutdown_requested = False

    sidecar._monitor_process()

    assert sidecar._debug_hooks._trace.call_args_list == [
        mocker.call(
            "publisher_process_start",
            publisher_pid=1234,
            previous_publisher_pid=None,
        ),
        mocker.call(
            "publisher_process_exit",
            publisher_pid=1234,
            return_code=9,
            expected=False,
        ),
    ]


@pytest.mark.parametrize(
    "options",
    [{"enable_tracing": "yes"}, {"unsupported": True}],
    ids=["invalid-type", "unknown-option"],
)
def test_log_publisher_rejects_invalid_options(options):
    with pytest.raises((TypeError, ValueError)):
        SaveLogsPeriodicallySidecar(options=options)
