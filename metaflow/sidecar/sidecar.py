from tracemalloc import is_tracing
from .sidecar_subprocess import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes


class Sidecar(object):
    TYPE = ""

    def __init__(self, flow, env):
        self.sidecar_process = None

    def start(self):
        if not self._is_active and self.get_sidecar_worker_class() is not None:
            self.sidecar_process = SidecarSubProcess(self._get_sidecar_worker_type())

    def add_to_context(self, **kwargs):
        if self._is_active:
            msg = Message(MessageTypes.CONTEXT, self._get_context())
            self.sidecar_process.send(msg)

    def terminate(self):
        if self._is_active:
            self.sidecar_process.kill()

    @classmethod
    def get_sidecar_worker_class(cls):
        raise NotImplementedError()

    def _send(self, msg):
        if self._is_active:
            self.sidecar_process.send(msg)

    @property
    def _is_active(self):
        return self.sidecar_process is not None

    def _get_context(self):
        return None

    @classmethod
    def _get_sidecar_worker_type(cls):
        return cls.TYPE
