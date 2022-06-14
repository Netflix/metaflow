import sys

from metaflow.event_logger import BaseEventLogger
from metaflow.sidecar import Message, MessageTypes


class DebugEventLogger(BaseEventLogger):
    TYPE = "debugLogger"

    @classmethod
    def get_worker(cls):
        return DebugEventLoggerSidecar


class DebugEventLoggerSidecar(object):
    def __init__(self):
        pass

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.SHUTDOWN:
            print("Debug[shutdown]: got shutdown!", file=sys.stderr)
            self._shutdown()
        elif msg.msg_type == MessageTypes.BEST_EFFORT:
            print("Debug[event]: %s" % str(msg.payload), file=sys.stderr)

    def _shutdown(self):
        sys.stderr.flush()
