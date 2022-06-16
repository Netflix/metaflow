import sys

from metaflow.sidecar import MessageTypes, Message
from metaflow.monitor import NullMonitor, Metric


class DebugMonitor(NullMonitor):
    TYPE = "debugMonitor"

    @classmethod
    def get_worker(cls):
        return DebugMonitorSidecar


class DebugMonitorSidecar(object):
    def __init__(self):
        pass

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.MUST_SEND:
            print("DebugMonitor[must_send]: %s" % str(msg.payload), file=sys.stderr)
        elif msg.msg_type == MessageTypes.SHUTDOWN:
            print("DebugMonitor[shutdown]: got shutdown!", file=sys.stderr)
            self._shutdown()
        elif msg.msg_type == MessageTypes.BEST_EFFORT:
            for v in msg.payload.values():
                metric = Metric.deserialize(v)
                print(
                    "DebugMonitor[metric]: %s for %s: %s"
                    % (metric.metric_type, metric.name, str(metric.value)),
                    file=sys.stderr,
                )

    def _shutdown(self):
        sys.stderr.flush()
