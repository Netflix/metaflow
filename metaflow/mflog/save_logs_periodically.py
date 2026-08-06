import os
import sys
import time
import subprocess
from threading import Event, Thread

from metaflow.sidecar import MessageTypes
from metaflow.util import to_unicode
from . import update_delay, BASH_SAVE_LOGS_ARGS, TASK_LOG_SOURCE
from .mflog import decorate

UPLOADER_DIAGNOSTICS_FALLBACK = "uploader_diagnostics_fallback"
DEBUG_LOG_TRANSFER_ENV_VAR = "METAFLOW_DEBUG_LOG_TRANSFER"
FAULT_MODE_ENV_VAR = "METAFLOW_DEBUG_LOG_UPLOAD_FAULT"
FAULT_DELAY_ENV_VAR = "METAFLOW_DEBUG_LOG_UPLOAD_FAULT_DELAY_SECONDS"
FAULT_HANG_ENV_VAR = "METAFLOW_DEBUG_LOG_UPLOAD_FAULT_HANG_SECONDS"
FAULT_MARKER_PATH = "/logs/metaflow_log_upload_fault_triggered"
FAULT_DELAY_SECONDS = 8
FAULT_HANG_SECONDS = 300
FAULT_EXIT_CODE = 70

FAULT_PROCESS_EXIT = "publisher_process_exit"
FAULT_THREAD_FAILURE = "publisher_thread_failure"
FAULT_UPLOAD_FAILURE = "upload_failure"
FAULT_UPLOAD_HANG = "upload_hang"
FAULT_MODES = {
    FAULT_PROCESS_EXIT,
    FAULT_THREAD_FAILURE,
    FAULT_UPLOAD_FAILURE,
    FAULT_UPLOAD_HANG,
}
UPLOAD_FAULT_MODES = {FAULT_UPLOAD_FAILURE, FAULT_UPLOAD_HANG}
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _write_uploader_log(message):
    try:
        payload = decorate(TASK_LOG_SOURCE, "%s\n" % message)
        with open(os.environ["PERIODICAL_UPLOADER_LOG_PATH"], "ab", buffering=0) as log:
            log.write(payload)
    except BaseException as error:
        _write_uploader_log_failure(message, error)


def _write_uploader_log_failure(message, error):
    try:
        # Don't write this to MFLOG_STDERR. The user code and this sidecar can
        # write to the same file at the same time and it is easy to create a
        # race condition there. It also changes the regular stderr log size and
        # can make this sidecar think regular logs changed again.
        # Put this beside MFLOG_STDERR with a fixed name, so if the uploader
        # log path is bad we still have some file to look at.
        stderr_dir = os.path.dirname(os.environ["MFLOG_STDERR"]) or "."
        path = os.path.join(stderr_dir, UPLOADER_DIAGNOSTICS_FALLBACK)
        payload = decorate(
            TASK_LOG_SOURCE,
            "[save_logs_periodically] failed to write uploader diagnostics "
            "error=%r message=%r\n" % (error, message),
        )
        with open(path, "ab", buffering=0) as log:
            log.write(payload)
    except BaseException:
        # No more fallback here. This is only for uploader diagnostics. The
        # important thing is to not let this stop the regular log upload.
        pass


def _write_save_logs_output(stream, output):
    for line in output.splitlines():
        _write_uploader_log("[save_logs %s] %s" % (stream, to_unicode(line)))


def _debug_log_transfer_enabled():
    return (
        os.environ.get(DEBUG_LOG_TRANSFER_ENV_VAR, "").strip().lower()
        in _TRUE_VALUES
    )


def _float_env(name, default):
    try:
        return max(0, float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def _debug_fault_mode():
    if not _debug_log_transfer_enabled():
        return None
    mode = os.environ.get(FAULT_MODE_ENV_VAR)
    return mode if mode in FAULT_MODES else None


def _write_fault_marker(mode):
    try:
        marker_dir = os.path.dirname(FAULT_MARKER_PATH)
        if marker_dir:
            os.makedirs(marker_dir, exist_ok=True)
        with open(FAULT_MARKER_PATH, "w", encoding="utf-8") as marker:
            marker.write(mode)
    except BaseException as error:
        _write_uploader_log(
            "[save_logs_periodically] failed to write fault marker "
            "mode=%r error=%r" % (mode, error)
        )


class SaveLogsPeriodicallySidecar(object):
    def __init__(self, options=None):
        options = options or {}
        if set(options) - {"enable_debug_logs"}:
            raise ValueError("Unsupported save_logs_periodically option")
        self._enable_debug_logs = options.get("enable_debug_logs", False)
        if not isinstance(self._enable_debug_logs, bool):
            raise TypeError("enable_debug_logs must be a boolean")
        self._fault_mode = _debug_fault_mode() if self._enable_debug_logs else None
        self._fault_triggered = Event()
        self._fault_delay_seconds = _float_env(
            FAULT_DELAY_ENV_VAR, FAULT_DELAY_SECONDS
        )
        self._thread = Thread(target=self._update_loop)
        self.is_alive = True
        self._thread.start()
        if self._fault_mode in (FAULT_PROCESS_EXIT, FAULT_THREAD_FAILURE):
            self._fault_thread = Thread(
                target=self._inject_fault,
                name="log-publisher-fault-injection",
                daemon=True,
            )
            self._fault_thread.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.is_alive = False

    @classmethod
    def get_worker(cls):
        return cls

    def _call_save_logs(self):
        if not self._enable_debug_logs:
            return subprocess.call(BASH_SAVE_LOGS_ARGS)

        process = subprocess.Popen(
            BASH_SAVE_LOGS_ARGS,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        _write_save_logs_output("stdout", stdout)
        _write_save_logs_output("stderr", stderr)
        return process.returncode

    def _activate_fault(self):
        _write_fault_marker(self._fault_mode)
        _write_uploader_log(
            "[save_logs_periodically] fault_injection_triggered mode=%s"
            % self._fault_mode
        )
        self._fault_triggered.set()

    def _inject_fault(self):
        time.sleep(getattr(self, "_fault_delay_seconds", FAULT_DELAY_SECONDS))
        self._activate_fault()
        if self._fault_mode == FAULT_PROCESS_EXIT:
            os._exit(FAULT_EXIT_CODE)
        if self._fault_mode == FAULT_THREAD_FAILURE:
            self.is_alive = False

    def _activate_upload_fault_if_due(self, start_time):
        fault_triggered = getattr(self, "_fault_triggered", None)
        if (
            getattr(self, "_fault_mode", None) in UPLOAD_FAULT_MODES
            and fault_triggered is not None
            and not fault_triggered.is_set()
            and time.time() - start_time
            >= getattr(self, "_fault_delay_seconds", FAULT_DELAY_SECONDS)
        ):
            self._activate_fault()

    def _upload_logs(self):
        if getattr(self, "_fault_triggered", None) is not None and (
            self._fault_triggered.is_set()
        ):
            if self._fault_mode == FAULT_UPLOAD_HANG:
                hang_seconds = _float_env(FAULT_HANG_ENV_VAR, FAULT_HANG_SECONDS)
                _write_uploader_log(
                    "[save_logs_periodically] simulated upload_hang "
                    "sleep_seconds=%.3f" % hang_seconds
                )
                time.sleep(hang_seconds)
                return None
        return self._call_save_logs()

    def _update_loop(self):
        def _file_size(path):
            if os.path.exists(path):
                return os.path.getsize(path)
            else:
                return 0

        # these env vars are set by mflog.mflog_env
        FILES = [os.environ["MFLOG_STDOUT"], os.environ["MFLOG_STDERR"]]
        start_time = time.time()
        sizes = [0 for _ in FILES]
        while self.is_alive:
            new_sizes = list(map(_file_size, FILES))
            if new_sizes != sizes:
                previous_sizes = sizes
                sizes = new_sizes
                self._activate_upload_fault_if_due(start_time)
                if self._enable_debug_logs:
                    elapsed = time.time() - start_time
                    for path, previous, current in zip(
                        FILES, previous_sizes, new_sizes
                    ):
                        _write_uploader_log(
                            "[save_logs_periodically] file=%s previous_size=%d "
                            "current_size=%d delta=%d elapsed_seconds=%.3f"
                            % (path, previous, current, current - previous, elapsed),
                        )
                try:
                    self._upload_logs()
                except:
                    pass
            time.sleep(update_delay(time.time() - start_time))

        if (
            getattr(self, "_fault_mode", None) == FAULT_THREAD_FAILURE
            and self._fault_triggered.is_set()
        ):
            raise RuntimeError("Injected log publisher thread failure")
