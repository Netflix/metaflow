import os
import sys
import time
import subprocess
from threading import Thread

from metaflow.sidecar import MessageTypes
from metaflow.util import to_unicode
from . import update_delay, BASH_SAVE_LOGS_ARGS, TASK_LOG_SOURCE
from .mflog import decorate

UPLOADER_DIAGNOSTICS_FALLBACK = "uploader_diagnostics_fallback"


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


class SaveLogsPeriodicallySidecar(object):
    def __init__(self, options=None):
        options = options or {}
        if set(options) - {"enable_debug_logs"}:
            raise ValueError("Unsupported save_logs_periodically option")
        self._enable_debug_logs = options.get("enable_debug_logs", False)
        if not isinstance(self._enable_debug_logs, bool):
            raise TypeError("enable_debug_logs must be a boolean")
        self._thread = Thread(target=self._update_loop)
        self.is_alive = True
        self._thread.start()

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
                    self._call_save_logs()
                except:
                    pass
            time.sleep(update_delay(time.time() - start_time))
