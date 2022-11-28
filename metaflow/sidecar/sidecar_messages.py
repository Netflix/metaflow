import json


# Define message enums
# Unfortunately we can't use enums because they are not supported
# officially in Python2
# INVALID: Not a valid message
# MUST_SEND: Will attempt to send until successful and not send any BEST_EFFORT
#            messages until then. A newer MUST_SEND message will take precedence on
#            any currently unsent one
# BEST_EFFORT: Will try to send once and drop if not possible
# SHUTDOWN: Signal termination; also best effort
class MessageTypes(object):
    INVALID, MUST_SEND, BEST_EFFORT, SHUTDOWN = range(1, 5)


class Message(object):
    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self.payload = payload

    def serialize(self):
        msg = {
            "msg_type": self.msg_type,
            "payload": self.payload,
        }
        return json.dumps(msg) + "\n"

    @staticmethod
    def deserialize(json_msg):
        try:
            return Message(**json.loads(json_msg))
        except json.decoder.JSONDecodeError:
            return Message(MessageTypes.INVALID, None)
