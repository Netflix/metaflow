import os
import sys
import time
import subprocess
from threading import Thread

from metaflow.sidecar import MessageTypes
from metaflow.util import to_unicode
from . import update_delay, BASH_SAVE_LOGS_ARGS, TASK_LOG_SOURCE
from .mflog import decorate


def _write_uploader_log(message):
    # This is intentionally not best effort. If PERIODICAL_UPLOADER_STDOUT is
    # incorrectly configured, it should be easy to detect in normal traced runs.
    # Otherwise a user sidecar uploader issue can happen while this diagnostics
    # log silently doesn't exist.
    #
    # Adding another hard-coded log file location to record the error of lacking
    # this log file path in the env variable complicates things. Once this path
    # is setup correctly, it should not normally change, so the chance that this
    # happens in some flows but not others is very low.
    payload = decorate(TASK_LOG_SOURCE, "%s\n" % message)
    with open(os.environ["PERIODICAL_UPLOADER_STDOUT"], "ab", buffering=0) as log:
        log.write(payload)


def _write_save_logs_output(stream, output):
    for line in output.splitlines():
        _write_uploader_log("[save_logs %s] %s" % (stream, to_unicode(line)))


class SaveLogsPeriodicallySidecar(object):
    def __init__(self, options=None):
        options = options or {}
        if set(options) - {"enable_tracing"}:
            raise ValueError("Unsupported save_logs_periodically option")
        self._enable_tracing = options.get("enable_tracing", False)
        if not isinstance(self._enable_tracing, bool):
            raise TypeError("enable_tracing must be a boolean")
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
        if not self._enable_tracing:
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
                if self._enable_tracing:
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
