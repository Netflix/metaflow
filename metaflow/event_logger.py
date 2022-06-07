from .sidecar import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes


class NullEventLogger(object):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def add_context(self, **kwargs):
        pass

    def log(self, payload):
        pass

    def terminate(self):
        pass


class EventLogger(NullEventLogger):
    def __init__(self, logger_type, env, **kwargs):
        # type: (str, str, str) -> None
        self.sidecar_process = None
        self.logger_type = logger_type
        self._context = {"env": env.get_environment_info()}
        # The additional keywords are added to the context for the monitor.
        # One use case is adding tags like flow_name etc.
        if kwargs:
            # Make sure we copy it as we don't want it to change
            self._context["user_context"] = dict(kwargs)

    def start(self):
        if self.sidecar_process is None:
            self.sidecar_process = SidecarSubProcess(self.logger_type, self._context)

    def add_context(self, **kwargs):
        self._context["user_context"].update(kwargs)
        if self.sidecar_process is not None:
            msg = Message(MessageTypes.UPDATE_CONTEXT, self._context)
            self.sidecar_process.send(msg)

    def log(self, payload):
        if self.sidecar_process is not None:
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.send(msg)

    def terminate(self):
        if self.sidecar_process is not None:
            self.sidecar_process.kill()
