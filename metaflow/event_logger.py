from metaflow.sidecar import Message, MessageTypes, Sidecar


class BaseEventLogger(Sidecar):
    def log(self, payload):
        if self._is_active:
            msg = Message(MessageTypes.EVENT, payload)
            self._send(msg)


# Backward compatible name
class EventLogger(BaseEventLogger):
    pass


class NullLogger(BaseEventLogger):
    TYPE = "nullSidecarLogger"

    @classmethod
    def get_sidecar_worker_class(cls):
        return None
