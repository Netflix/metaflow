import sys

from metaflow.sidecar_messages import Message


class DebugEventLogger(object):
    TYPE = "debugLogger"

    def log(self, msg):
        sys.stderr.write("event_logger: " + str(msg) + "\n")

    def process_message(self, msg):
        # type: (Message) -> None
        self.log(msg.payload)

    def shutdown(self):
        pass
