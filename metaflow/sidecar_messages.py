import json
from enum import IntEnum

# Define message enums
class MessageTypes(IntEnum):
    SHUTDOWN = 1
    LOG_EVENT = 2


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
    parsed_json_msg = json.loads(json_msg)
    return Message(MessageTypes(parsed_json_msg['msg_type']),
                   parsed_json_msg['payload'])

