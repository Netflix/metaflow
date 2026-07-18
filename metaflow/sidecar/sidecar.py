import json
import threading

from .sidecar_subprocess import SidecarSubProcess


class Sidecar(object):
    def __init__(self, sidecar_type, options=None):
        # Needs to be here because this file gets loaded by lots of things and SIDECARS
        # may not be fully populated by then
        from metaflow.plugins import SIDECARS

        self._sidecar_type = sidecar_type
        self._options = self._normalize_options(options)
        self._publisher_health = None
        if self._sidecar_type == "save_logs_periodically":
            from metaflow.mflog import publisher_health

            if publisher_health.health_logging_enabled():
                self._publisher_health = publisher_health
                self._publisher_monitor_stop = threading.Event()
                self._publisher_shutdown_requested = False

        self._debug_hooks = self._load_debug_hooks()
        if self._debug_hooks is not None:
            self._monitor_stop = threading.Event()
            self._monitor_thread = None
            self._shutdown_requested = False
        self._has_valid_worker = False
        t = SIDECARS.get(self._sidecar_type)
        if t is not None and t.get_worker() is not None:
            self._has_valid_worker = True
        self.sidecar_process = None
        # Whether to send msg in a thread-safe fashion.
        self._threadsafe_send_enabled = False

    def start(self):
        if not self.is_active and self._has_valid_worker:
            self.sidecar_process = SidecarSubProcess(
                self._sidecar_type, options=self._options
            )
            if self._publisher_health is not None:
                process = getattr(self.sidecar_process, "_process", None)
                self._publisher_health.write_health_event(
                    "publisher_process_start",
                    publisher_pid=getattr(process, "pid", None),
                    start_succeeded=process is not None,
                )
                self._publisher_monitor_thread = threading.Thread(
                    target=self._monitor_publisher_process,
                    name="log-publisher-process-health",
                    daemon=True,
                )
                self._publisher_monitor_thread.start()

            if self._debug_hooks is not None:
                if getattr(self.sidecar_process, "_process", None) is None:
                    self._debug_hooks._trace("publisher_process_start_failure")
                self._monitor_thread = threading.Thread(
                    target=self._monitor_process,
                    name="log-publisher-process-monitor",
                    daemon=True,
                )
                self._monitor_thread.start()

    def _monitor_publisher_process(self):
        process = getattr(self.sidecar_process, "_process", None)
        while not self._publisher_monitor_stop.is_set():
            return_code = process.poll() if process is not None else None
            if process is None or return_code is not None:
                self._publisher_health.write_health_event(
                    "publisher_process_exit",
                    publisher_pid=getattr(process, "pid", None),
                    return_code=return_code,
                    expected=self._publisher_shutdown_requested,
                )
                self._publisher_monitor_stop.set()
                return
            self._publisher_monitor_stop.wait(
                self._publisher_health.PROCESS_POLL_INTERVAL_SECONDS
            )

    def _load_debug_hooks(self):
        if self._sidecar_type != "save_logs_periodically":
            return None
        enable_tracing = self._options.get("enable_tracing")
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

    @staticmethod
    def _normalize_options(options):
        if options is None:
            return {}
        if not isinstance(options, dict):
            raise TypeError("Sidecar options must be a dictionary")
        try:
            return json.loads(json.dumps(options))
        except (TypeError, ValueError) as error:
            raise TypeError("Sidecar options must be JSON-serializable: %s" % error)

    def _monitor_process(self):
        observed_process = None
        observed_exit = False
        while not self._monitor_stop.is_set():
            process = getattr(self.sidecar_process, "_process", None)
            if process is not observed_process:
                previous_process = observed_process
                event = (
                    "publisher_process_start"
                    if observed_process is None
                    else "publisher_process_restart"
                )
                observed_process = process
                observed_exit = False
                self._debug_hooks._trace(
                    event,
                    publisher_pid=getattr(process, "pid", None),
                    previous_publisher_pid=(
                        getattr(previous_process, "pid", None)
                        if event == "publisher_process_restart"
                        else None
                    ),
                )

            if process is not None and not observed_exit:
                return_code = process.poll()
                if return_code is not None:
                    observed_exit = True
                    self._debug_hooks._trace(
                        "publisher_process_exit",
                        publisher_pid=process.pid,
                        return_code=return_code,
                        expected=self._shutdown_requested,
                    )
                    self._debug_hooks._count("process_exit")
            self._monitor_stop.wait(self._debug_hooks.PROCESS_POLL_INTERVAL_SECONDS)

    def enable_threadsafe_send(self):
        self._threadsafe_send_enabled = True

    def disable_threadsafe_send(self):
        self._threadsafe_send_enabled = False

    def send(self, msg):
        if self.is_active:
            self.sidecar_process.send(
                msg, thread_safe_send=self._threadsafe_send_enabled
            )

    def terminate(self):
        if self.is_active:
            if self._publisher_health is not None:
                process = getattr(self.sidecar_process, "_process", None)
                self._publisher_shutdown_requested = True
                self._publisher_health.write_health_event(
                    "publisher_process_shutdown_requested",
                    publisher_pid=getattr(process, "pid", None),
                    return_code=process.poll() if process is not None else None,
                )

            if self._debug_hooks is None:
                self.sidecar_process.kill()
                return

            process = getattr(self.sidecar_process, "_process", None)
            return_code = process.poll() if process is not None else None
            if return_code is not None:
                self._debug_hooks._trace(
                    "publisher_process_found_dead_at_shutdown",
                    publisher_pid=getattr(process, "pid", None),
                    return_code=return_code,
                )
            self._shutdown_requested = True
            self._debug_hooks._trace(
                "publisher_process_shutdown_requested",
                publisher_pid=getattr(process, "pid", None),
                return_code=return_code,
            )
            try:
                self.sidecar_process.kill()
            finally:
                self._monitor_stop.set()

    @property
    def is_active(self):
        return self.sidecar_process is not None
