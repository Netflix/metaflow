from .sidecar import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes


class NullEventLogger(object):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def log(self, payload):
        pass

    def terminate(self):
        pass


class EventLogger(NullEventLogger):
    def __init__(self, logger_type, env, flow_name):
        # type: (str, str, str) -> None
        self.sidecar_process = None
        self.logger_type = logger_type
        self.env_info = env.get_environment_info()
        self.env_info["flow_name"] = flow_name

    def start(self):
        if self.sidecar_process is None:
            self.sidecar_process = SidecarSubProcess(
                self.logger_type, {"env": self.env_info}
            )

    def log(self, payload):
        if self.sidecar_process is not None:
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.send(msg)

    def terminate(self):
        if self.sidecar_process is not None:
            self.sidecar_process.kill()
