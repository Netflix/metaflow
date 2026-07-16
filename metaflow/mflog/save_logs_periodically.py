import os
import sys
import time
import subprocess
import threading
import traceback

from metaflow.sidecar import MessageTypes
from . import update_delay, BASH_SAVE_LOGS_ARGS
from .publisher_health import (
    health_interval_seconds,
    health_logging_enabled,
    last_event_age,
    write_health_event,
)


class SaveLogsPeriodicallySidecar(object):
    def __init__(self):
        self._health_logging_enabled = health_logging_enabled()
        if self._health_logging_enabled:
            self._state_lock = threading.Lock()
            self._health_stop = threading.Event()
            self._last_upload_at = None
            self._last_success_at = None
            self._last_upload_status = None
            self._upload_started_at = None
            self._thread_dead_reported = False

        self._thread = threading.Thread(target=self._update_loop)
        self.is_alive = True
        self._thread.start()

        if self._health_logging_enabled:
            write_health_event("publisher_thread_start", thread_name=self._thread.name)
            self._health_thread = threading.Thread(
                target=self._health_loop,
                name="log-publisher-health",
                daemon=True,
            )
            self._health_thread.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.is_alive = False
            if self._health_logging_enabled:
                write_health_event("publisher_shutdown_requested")
                self._health_stop.set()

    @classmethod
    def get_worker(cls):
        return cls

    def _update_loop(self):
        def _file_size(path):
            if os.path.exists(path):
                return os.path.getsize(path)
            else:
                return 0

        try:
            # these env vars are set by mflog.mflog_env
            FILES = [os.environ["MFLOG_STDOUT"], os.environ["MFLOG_STDERR"]]
            start_time = time.time()
            sizes = [0 for _ in FILES]
            while self.is_alive:
                new_sizes = list(map(_file_size, FILES))
                if new_sizes != sizes:
                    sizes = new_sizes
                    try:
                        self._upload_logs(new_sizes)
                    except:
                        pass
                time.sleep(update_delay(time.time() - start_time))
        except BaseException as error:
            if self._health_logging_enabled:
                write_health_event(
                    "publisher_thread_failure",
                    error=repr(error),
                    traceback=traceback.format_exc(),
                )
            raise

    def _upload_logs(self, sizes):
        if not self._health_logging_enabled:
            return subprocess.call(BASH_SAVE_LOGS_ARGS)

        started_at = time.monotonic()
        with self._state_lock:
            self._upload_started_at = started_at
        write_health_event("upload_start", local_bytes=sum(sizes), local_sizes=sizes)

        return_code = None
        error = None
        try:
            return_code = subprocess.call(BASH_SAVE_LOGS_ARGS)
            return return_code
        except BaseException as ex:
            error = repr(ex)
            raise
        finally:
            finished_at = time.time()
            status = "success" if error is None and return_code == 0 else "failure"
            with self._state_lock:
                self._upload_started_at = None
                self._last_upload_at = finished_at
                self._last_upload_status = status
                if status == "success":
                    self._last_success_at = finished_at
            write_health_event(
                "upload_%s" % status,
                duration_seconds=time.monotonic() - started_at,
                return_code=return_code,
                error=error,
                local_bytes=sum(sizes),
                local_sizes=sizes,
            )

    def _health_loop(self):
        interval = health_interval_seconds()
        while not self._health_stop.is_set():
            thread_alive = self._thread.is_alive()
            with self._state_lock:
                upload_started_at = self._upload_started_at
                fields = {
                    "publisher_thread_alive": thread_alive,
                    "upload_in_progress": upload_started_at is not None,
                    "upload_age_seconds": (
                        time.monotonic() - upload_started_at
                        if upload_started_at is not None
                        else None
                    ),
                    "last_upload_status": self._last_upload_status,
                    "last_upload_age_seconds": last_event_age(self._last_upload_at),
                    "last_success_age_seconds": last_event_age(self._last_success_at),
                }

            write_health_event("publisher_heartbeat", **fields)
            if self.is_alive and not thread_alive and not self._thread_dead_reported:
                self._thread_dead_reported = True
                write_health_event("publisher_thread_dead", **fields)
            self._health_stop.wait(interval)
