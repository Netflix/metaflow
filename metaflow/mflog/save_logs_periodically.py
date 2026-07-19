import os
import sys
import time
import subprocess
from threading import Thread

from metaflow.sidecar import MessageTypes
from . import update_delay, BASH_SAVE_LOGS_ARGS, TASK_LOG_SOURCE
from .mflog import decorate


def _write_uploader_log(message):
    payload = decorate(TASK_LOG_SOURCE, "%s\n" % message)
    with open(os.environ["PERIODICAL_UPLOADER_STDOUT"], "ab", buffering=0) as log:
        log.write(payload)


def _write_save_logs_process_log(log, message):
    log.write(decorate(TASK_LOG_SOURCE, "%s\n" % message))


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

        # PERIODICAL_UPLOADER_STDOUT contains messages from this long-lived
        # sidecar. SAVE_LOGS_PROCESS_STDOUT captures stdout and stderr from
        # each short-lived save_logs subprocess spawned by the sidecar.
        with open(os.environ["SAVE_LOGS_PROCESS_STDOUT"], "ab", buffering=0) as output:
            started_at = time.time()
            _write_save_logs_process_log(output, "[save_logs_process] process_start")
            try:
                return_code = subprocess.call(
                    BASH_SAVE_LOGS_ARGS,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
            except BaseException as error:
                _write_save_logs_process_log(
                    output,
                    "[save_logs_process] process_failure error=%r "
                    "elapsed_seconds=%.3f" % (error, time.time() - started_at),
                )
                raise
            _write_save_logs_process_log(
                output,
                "[save_logs_process] process_exit return_code=%d "
                "elapsed_seconds=%.3f" % (return_code, time.time() - started_at),
            )
            return return_code

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
