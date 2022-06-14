from metaflow.sidecar import Message, MessageTypes, Sidecar


class BaseEventLogger(object):
    TYPE = "nullSidecarLogger"

    def __init__(self, flow, env):
        self._sidecar = Sidecar(self.TYPE)

    def start(self):
        return self._sidecar.start()

    def terminate(self):
        return self._sidecar.terminate()

    def add_context(self, context):
        pass

    def log(self, payload):
        if self._sidecar.is_active:
            msg = Message(MessageTypes.BEST_EFFORT, payload)
            self._sidecar.send(msg)

    @classmethod
    def get_worker(cls):
        return None


# Backward compatible name
class EventLogger(BaseEventLogger):
    pass
