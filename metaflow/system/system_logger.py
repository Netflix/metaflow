import os
import sys
from typing import Dict, Any, Optional, Union


class SystemLogger(object):
    def __init__(self):
        self._logger = None
        self._flow_name = None
        self._context = {}
        self._is_context_updated = False

    def __del__(self):
        if self._flow_name == "not_a_real_flow":
            self.logger.terminate()

    def update_context(self, context: Dict[str, Any]):
        """
        Update the global context maintained by the system logger.

        Parameters
        ----------
        context : Dict[str, Any]
            A dictionary containing the context to update.

        """
        self._is_context_updated = True
        self._context.update(context)

    def init_system_logger(
        self, flow_name: str, logger: "metaflow.event_logger.NullEventLogger"
    ):
        self._flow_name = flow_name
        self._logger = logger

    def _init_logger_outside_flow(self):
        from .system_utils import DummyFlow
        from .system_utils import init_environment_outside_flow
        from metaflow.plugins import LOGGING_SIDECARS
        from metaflow.metaflow_config import DEFAULT_EVENT_LOGGER

        self._flow_name = "not_a_real_flow"
        _flow = DummyFlow(self._flow_name)
        _environment = init_environment_outside_flow(_flow)
        _logger = LOGGING_SIDECARS[DEFAULT_EVENT_LOGGER](_flow, _environment)
        return _logger

    @property
    def logger(self) -> Optional["metaflow.event_logger.NullEventLogger"]:
        if self._logger is None:
            # This happens if the logger is being accessed outside of a flow
            # We start a logger with a dummy flow and a default environment
            self._debug("Started logger outside of a flow")
            self._logger = self._init_logger_outside_flow()
            self._logger.start()
        return self._logger

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

    def log_event(
        self, level: str, module: str, name: str, payload: Optional[Any] = None
    ):
        """
        Log an event to the event logger.

        Parameters
        ----------
        level : str
            Log level of the event. Can be one of "info", "warning", "error", "critical", "debug".
        module : str
            Module of the event. Usually the name of the class, function, or module that the event is being logged from.
        name : str
            Name of the event. Used to qualify the event type.
        payload : Optional[Any], default None
            Payload of the event. Contains the event data.
        """
        self.logger.log(
            {
                "level": level,
                "module": module,
                "name": name,
                "payload": payload if payload is not None else {},
                "context": self._context,
                "is_context_updated": self._is_context_updated,
            }
        )
        self._is_context_updated = False
