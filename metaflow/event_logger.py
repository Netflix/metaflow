from .sidecar import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes


class EventLogger(object):

    def __init__(self, logger_type):
        # type: (str) -> None
        self.sidecar_process = None
        self.logger_type = logger_type

    def start(self):
        self.sidecar_process = SidecarSubProcess(self.logger_type)

    def log(self, payload):
        msg = Message(MessageTypes.LOG_EVENT, payload)
        self.sidecar_process.msg_handler(msg)

    def terminate(self):
        self.sidecar_process.kill()
