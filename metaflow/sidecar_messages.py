import json

# Define message enums
# Unfortunately we can't use enums because they are not supported
# officially in Python2
class MessageTypes(object):
    SHUTDOWN, LOG_EVENT = range(1, 3)

class Message(object):

    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self.payload = payload

    def serialize(self):
        msg = {
            'msg_type': self.msg_type,
            'payload': self.payload,
        }
        return json.dumps(msg)+"\n"

def deserialize(json_msg):
    return Message(**json.loads(json_msg))