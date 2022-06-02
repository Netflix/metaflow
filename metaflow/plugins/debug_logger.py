import sys

from metaflow.sidecar_messages import Message, MessageTypes


class DebugEventLoggerSidecar(object):
    def __init__(self):
        pass

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.SHUTDOWN:
            print("Debug event: got shutdown!")
            self._shutdown()
        elif msg.msg_type == MessageTypes.LOG_EVENT:
            print("Debug event logging %s" % str(msg.payload), file=sys.stderr)

    def _shutdown(self):
        sys.stderr.flush()
