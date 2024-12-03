import os
import sys
from ..debug import debug
from contextlib import contextmanager
from typing import Optional, Union, Dict, Any


class SystemMonitor(object):
    def __init__(self):
        self._monitor = None
        self._flow_name = None

    def __del__(self):
        if self._flow_name == "not_a_real_flow":
            self.monitor.terminate()

    def init_system_monitor(
        self, flow_name: str, monitor: "metaflow.monitor.NullMonitor"
    ):
        self._flow_name = flow_name
        self._monitor = monitor

    def _init_system_monitor_outside_flow(self):
        from .system_utils import DummyFlow
        from .system_utils import init_environment_outside_flow
        from metaflow.plugins import MONITOR_SIDECARS
        from metaflow.metaflow_config import DEFAULT_MONITOR

        self._flow_name = "not_a_real_flow"
        _flow = DummyFlow(self._flow_name)
        _environment = init_environment_outside_flow(_flow)
        _monitor = MONITOR_SIDECARS[DEFAULT_MONITOR](_flow, _environment)
        return _monitor

    @property
    def monitor(self) -> Optional["metaflow.monitor.NullMonitor"]:
        if self._monitor is None:
            # This happens if the monitor is being accessed outside of a flow
            self._debug("Started monitor outside of a flow")
            self._monitor = self._init_system_monitor_outside_flow()
            self._monitor.start()
        return self._monitor

    @staticmethod
    def _debug(msg: str):
        """
        Log a debug message to stderr.

        Parameters
        ----------
        msg : str
            Message to log.

        """
        if os.environ.get("METAFLOW_DEBUG_SIDECAR", "0").lower() not in (
            "0",
            "false",
            "",
        ):
            print("system monitor: %s" % msg, file=sys.stderr)

    @contextmanager
    def measure(self, name: str):
        """
        Context manager to measure the execution duration and counter of a block of code.

        Parameters
        ----------
        name : str
            The name to associate with the timer and counter.

        Yields
        ------
        None
        """
        # Delegating the context management to the monitor's measure method
        with self.monitor.measure(name):
            yield

    @contextmanager
    def count(self, name: str):
        """
        Context manager to increment a counter.

        Parameters
        ----------
        name : str
            The name of the counter.

        Yields
        ------
        None
        """
        # Delegating the context management to the monitor's count method
        with self.monitor.count(name):
            yield

    def gauge(self, gauge: "metaflow.monitor.Gauge"):
        """
        Log a gauge.

        Parameters
        ----------
        gauge : metaflow.monitor.Gauge
            The gauge to log.

        """
        self.monitor.gauge(gauge)
