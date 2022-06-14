import sys

from metaflow.sidecar import MessageTypes, Message
from metaflow.monitor import BaseMonitor, Metric


class DebugMonitor(BaseMonitor):
    TYPE = "debugMonitor"

    @classmethod
    def get_sidecar_worker_class(cls):
        return DebugMonitorSidecar


class DebugMonitorSidecar(object):
    def __init__(self):
        pass

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.CONTEXT:
            self._context = msg.payload
        elif msg.msg_type == MessageTypes.SHUTDOWN:
            self._shutdown()
        elif msg.msg_type == MessageTypes.EVENT:
            for v in msg.payload.values():
                metric = Metric.deserialize(v)
                print(
                    "%s for %s: %s"
                    % (metric.metric_type, metric.name, str(metric.value)),
                    file=sys.stderr,
                )

    def _shutdown(self):
        sys.stderr.flush()
