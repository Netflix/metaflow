import json
import struct
import traceback


class Channel(object):
    """
    Channel is a higher level abstraction over a low-level bytestream.

    You can send and receive JSON serializable object directly with this interface

    For now this class does not do much, but we could imagine some sort compression or other
    transformation being added here
    """

    def __init__(self, stream):
        self._stream = stream
        self._fmt = struct.Struct("<I")

    def send(self, obj):
        try:
            to_send = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
            sz = len(to_send)
            self._stream.write(self._fmt.pack(sz))
            self._stream.write(to_send)
        except EOFError as e:
            raise RuntimeError("Cannot send object over streaming interface: %s" % e)
        except BaseException as e:
            raise ValueError("Cannot serialize object: %s" % traceback.format_exc())

    def recv(self, timeout=None):
        # To receive, we first receive the size of the object and then the object itself
        try:
            sz_bytes = self._stream.read(self._fmt.size, timeout)
            msg_sz = self._fmt.unpack(sz_bytes)[0]
            obj_bytes = self._stream.read(msg_sz, timeout)
            return json.loads(obj_bytes)
        except EOFError as e:
            raise RuntimeError("Cannot receive object over streaming interface: %s" % e)
        except BaseException as e:
            raise ValueError("Cannot deserialize object: %s" % traceback.format_exc())

    def fileno(self):
        return self._stream.fileno()
