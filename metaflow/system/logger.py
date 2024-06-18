import sys
from typing import Dict, Any, Optional, Union


class SystemLogger(object):
    def __init__(self):
        self._logger = None
        self._flow_name = None

    def __del__(self):
        if self._flow_name == "not_a_real_flow":
            self.logger.terminate()

    def init_environment_outside_flow(
        self, flow: Union["metaflow.flowspec.FlowSpec", "metaflow.sidecar.DummyFlow"]
    ):
        from metaflow.plugins import ENVIRONMENTS
        from metaflow.metaflow_config import DEFAULT_ENVIRONMENT
        from metaflow.metaflow_environment import MetaflowEnvironment

        return [
            e
            for e in ENVIRONMENTS + [MetaflowEnvironment]
            if e.TYPE == DEFAULT_ENVIRONMENT
        ][0](flow)

    def init_system_logger(
        self, flow_name: str, logger: "metaflow.event_logger.NullEventLogger"
    ):
        self._flow_name = flow_name
        self._logger = logger

    def init_logger_outside_flow(self):
        from .dummy_flow import DummyFlow
        from metaflow.plugins import LOGGING_SIDECARS
        from metaflow.metaflow_config import DEFAULT_EVENT_LOGGER

        self._flow_name = "not_a_real_flow"
        _flow = DummyFlow(self._flow_name)
        _environment = self.init_environment_outside_flow(_flow)
        _logger = LOGGING_SIDECARS[DEFAULT_EVENT_LOGGER](_flow, _environment)
        return _logger

    @property
    def logger(self) -> Optional["metaflow.event_logger.NullEventLogger"]:
        if self._logger is None:
            # This happens if the logger is being accessed outside of a flow
            # We start a logger with a dummy flow and a default environment
            self._debug("Started logger outside of a flow")
            self._logger = self.init_logger_outside_flow()
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

        Returns
        -------
        None
        """
        print("system logger: %s" % msg, file=sys.stderr)

    def log_event(
        self,
        msg: Optional[str] = None,
        event_name: Optional[str] = None,
        log_stream: Optional[str] = None,
        other_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Log an event to the event logger.

        Parameters
        ----------
        msg : str, optional default None
            Message to log.
        event_name : str, optional default None
            Name of the event to log. Used for grouping similar event types by event name.
        log_stream : str, optional default None
            Name of the log stream to log to. Used for grouping events by log stream.
        other_context : Dict[str, Any], optional default None
            Additional context to log with the event. The additional context will have to be handled by
            the event logger implementation.
        """
        self.logger.log_event(msg, event_name, log_stream, other_context)