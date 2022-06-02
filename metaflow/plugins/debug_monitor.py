from __future__ import print_function

import sys

from metaflow.sidecar_messages import MessageTypes, Message
from metaflow.monitor import Metric


class DebugMonitorSidecar(object):
    def __init__(self):
        self._env = None

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.START:
            self._env = msg.payload.get("env", None)
        elif msg.msg_type == MessageTypes.SHUTDOWN:
            print("Debug monitor got shutdown!", file=sys.stderr)
            self._shutdown()
        elif msg.msg_type == MessageTypes.LOG_EVENT:
            for v in msg.payload.values():
                metric = Metric.deserialize(v)
                metric.env = self._env
                print(
                    "%s for %s: %s"
                    % (metric.metric_type, metric.name, str(metric.value)),
                    file=sys.stderr,
                )

    def _shutdown(self):
        sys.stderr.flush()
