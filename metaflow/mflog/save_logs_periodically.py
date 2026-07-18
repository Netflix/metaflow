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
    SUPPORTED_OPTIONS = {"enable_tracing"}

    def __init__(self, options=None):
        self._options = dict(options or {})
        unknown_options = set(self._options) - self.SUPPORTED_OPTIONS
        if unknown_options:
            raise ValueError(
                "Unsupported save_logs_periodically options: %s"
                % ", ".join(sorted(unknown_options))
            )
        enable_tracing = self._options.get("enable_tracing")
        if enable_tracing is not None and not isinstance(enable_tracing, bool):
            raise TypeError("enable_tracing must be a boolean")

        self._health_logging_enabled = health_logging_enabled()
        if self._health_logging_enabled:
            self._state_lock = threading.Lock()
            self._health_stop = threading.Event()
            self._last_upload_at = None
            self._last_success_at = None
            self._last_upload_status = None
            self._upload_started_at = None
            self._thread_dead_reported = False

        self._debug_hooks = self._load_debug_hooks()
        if self._debug_hooks is not None:
            self._debug_state_lock = threading.Lock()
            self._debug_last_success_at = None
            self._debug_last_remote_sizes = {}
            self._debug_upload_started_at = None
            self._debug_last_upload_verified = None
            self._debug_stuck_reported = False
            self._debug_health_interval_seconds = self._debug_hooks._float_env(
                self._debug_hooks.HEALTH_INTERVAL_ENV_VAR,
                self._debug_hooks.HEALTH_TRACE_INTERVAL_SECONDS,
            )
            self._stuck_upload_seconds = self._debug_hooks._float_env(
                self._debug_hooks.STUCK_UPLOAD_ENV_VAR,
                self._debug_hooks.STUCK_UPLOAD_SECONDS,
            )
            self._redirect_stderr()
            self._debug_hooks._trace("publisher_worker_start")

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

        if self._debug_hooks is not None:
            self._debug_health_thread = threading.Thread(
                target=self._debug_health_loop,
                name="log-publisher-debug-health",
                daemon=True,
            )
            self._debug_health_thread.start()

    def _load_debug_hooks(self=None):
        enable_tracing = getattr(self, "_options", {}).get("enable_tracing")
        if enable_tracing is False:
            return None
        try:
            from metaflow_extensions.nflx.plugins import log_upload_tracing

            if (
                enable_tracing is True
                or log_upload_tracing.debug_log_transfer_enabled()
            ):
                return log_upload_tracing
        except ImportError:
            pass
        return None

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
        debug_hooks = getattr(self, "_debug_hooks", None)

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
            if getattr(self, "_health_logging_enabled", False):
                write_health_event(
                    "publisher_thread_failure",
                    error=repr(error),
                    traceback=traceback.format_exc(),
                )
            if debug_hooks is not None:
                debug_hooks._trace(
                    "publisher_thread_failure",
                    error=repr(error),
                    traceback=traceback.format_exc(),
                )
            raise

    def _upload_logs(self, sizes):
        upload_call = lambda: subprocess.call(BASH_SAVE_LOGS_ARGS)
        if getattr(self, "_debug_hooks", None) is not None:
            upload_call = lambda: self._run_periodic_upload(
                lambda: subprocess.call(BASH_SAVE_LOGS_ARGS)
            )

        if not self._health_logging_enabled:
            return upload_call()

        started_at = time.monotonic()
        with self._state_lock:
            self._upload_started_at = started_at
        write_health_event("upload_start", local_bytes=sum(sizes), local_sizes=sizes)

        return_code = None
        error = None
        try:
            return_code = upload_call()
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

    def _redirect_stderr(self):
        try:
            output_path = self._debug_hooks.PUBLISHER_STDERR_PATH
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            output = open(output_path, "a", buffering=1)
            sys.stdout = output
            sys.stderr = output
        except Exception as error:
            self._debug_hooks._trace(
                "publisher_stderr_redirect_failure", error=repr(error)
            )

    def _run_periodic_upload(self, upload_call):
        hooks = self._debug_hooks
        if hasattr(self, "_debug_state_lock"):
            state_lock = self._debug_state_lock
            state_prefix = "_debug"
        else:
            state_lock = self._state_lock
            state_prefix = ""

        upload_started_at_attr = "%s_upload_started_at" % state_prefix
        stuck_reported_attr = "%s_stuck_reported" % state_prefix
        last_success_at_attr = "%s_last_success_at" % state_prefix
        last_remote_sizes_attr = "%s_last_remote_sizes" % state_prefix
        last_upload_verified_attr = "%s_last_upload_verified" % state_prefix

        with state_lock:
            setattr(self, upload_started_at_attr, time.monotonic())
            setattr(self, stuck_reported_attr, False)
            last_success_at = getattr(self, last_success_at_attr)
            previous_remote_sizes = getattr(self, last_remote_sizes_attr)

        verified, remote_sizes, return_code = hooks._run_upload(
            "periodic", last_success_at, previous_remote_sizes, upload_call
        )

        with state_lock:
            setattr(self, upload_started_at_attr, None)
            setattr(self, last_upload_verified_attr, verified)
            if remote_sizes:
                setattr(self, last_remote_sizes_attr, remote_sizes)
            if verified:
                setattr(self, last_success_at_attr, time.time())
        return return_code

    def _debug_health_loop(self):
        hooks = self._debug_hooks
        while self.is_alive:
            with self._debug_state_lock:
                upload_started_at = self._debug_upload_started_at
                upload_age = (
                    time.monotonic() - upload_started_at
                    if upload_started_at is not None
                    else None
                )
                last_success_at = self._debug_last_success_at
                destination_sizes = dict(self._debug_last_remote_sizes)
                last_upload_verified = self._debug_last_upload_verified
                should_report_stuck = (
                    upload_age is not None
                    and upload_age >= self._stuck_upload_seconds
                    and not self._debug_stuck_reported
                )
                if should_report_stuck:
                    self._debug_stuck_reported = True

            fields = {
                "publisher_thread_alive": self._thread.is_alive(),
                "upload_in_progress": upload_started_at is not None,
                "upload_age_seconds": upload_age,
                "local_sizes": hooks._local_log_sizes(),
                "destination_sizes": destination_sizes,
                "last_upload_verified": last_upload_verified,
                "last_success_age_seconds": hooks._last_success_age(last_success_at),
            }
            hooks._trace("publisher_health", **fields)
            hooks._gauge("last_success_age_seconds", fields["last_success_age_seconds"])
            hooks._gauge("upload_age_seconds", upload_age)
            if should_report_stuck:
                hooks._trace("upload_stuck", phase="periodic", **fields)
                hooks._count("stuck")
            time.sleep(self._debug_health_interval_seconds)

    def shutdown(self):
        if self._debug_hooks is None:
            return
        with self._debug_state_lock:
            upload_started_at = self._debug_upload_started_at
        self._debug_hooks._trace(
            "publisher_worker_stop",
            publisher_thread_alive=self._thread.is_alive(),
            upload_in_progress=upload_started_at is not None,
            upload_age_seconds=(
                time.monotonic() - upload_started_at
                if upload_started_at is not None
                else None
            ),
            last_success_age_seconds=self._debug_hooks._last_success_age(
                self._debug_last_success_at
            ),
        )
