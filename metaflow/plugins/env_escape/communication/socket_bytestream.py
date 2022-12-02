import errno
import socket
import sys

from .bytestream import ByteStream
from .utils import __try_op__

CONNECT_TIMEOUT = 2
CONNECT_RETRY = 5
RECV_RETRY = 2
WRITE_RETRY = 2
MAX_MSG_SIZE = 2097152  # Send/Receive 2 MB at a time


class SocketByteStream(ByteStream):
    @classmethod
    def connect(cls, host, port):
        family, socktype, proto, _, sockaddr = socket.getaddrinfo(
            host, port, socket.AF_INET, socket.SOCK_STREAM
        )
        try:
            sock = socket.socket(family=family, type=socktype)
            sock.settimeout(CONNECT_TIMEOUT)
            __try_op__("connect", sock.connect, CONNECT_RETRY, sockaddr)
            return cls(sock)
        except BaseException:
            sock.close()
            raise

    @classmethod
    def unixconnect(cls, path):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(CONNECT_TIMEOUT)
            __try_op__("unixconnect", sock.connect, CONNECT_RETRY, path)
            return cls(sock)
        except BaseException:
            sock.close()
            raise

    def __init__(self, sock):
        self._sock = sock
        self._sock.settimeout(None)  # Make the socket blocking
        self._is_closed = False

    def read(self, count, timeout=None):
        result = bytearray(count)
        with memoryview(result) as m:
            while count > 0:
                try:
                    if timeout is not None:
                        # Yes, technically should divide by RECV_COUNT...
                        self._socket.settimeout(timeout)
                    nbytes = __try_op__(
                        "receive",
                        self._sock.recv_into,
                        RECV_RETRY,
                        m,
                        min(count, MAX_MSG_SIZE),
                    )
                    # If we don't receive anything, we reached EOF
                    if nbytes == 0:
                        raise socket.error()
                    count -= nbytes
                    m = m[nbytes:]
                except socket.timeout:
                    continue
                except socket.error as e:
                    self.close()
                    raise EOFError(e)
        return result

    def write(self, data):
        with memoryview(data) as m:
            total_count = m.nbytes
            while total_count > 0:
                try:
                    nbytes = __try_op__(
                        "send", self._sock.send, WRITE_RETRY, m[:MAX_MSG_SIZE]
                    )
                    m = m[nbytes:]
                    total_count -= nbytes
                except socket.timeout:
                    continue
                except socket.error as e:
                    self.close()
                    raise EOFError(e)

    def close(self):
        if self._is_closed:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        self._sock.close()
        self._is_closed = True

    @property
    def is_closed(self):
        return self._is_closed

    def fileno(self):
        try:
            return self._sock.fileno()
        except socket.error:
            self.close()
            exc = sys.exc_info()[1]
            found_error = None
            if hasattr(exc, "errno"):
                found_error = exc.errno
            else:
                found_error = exc[0]
            if found_error == errno.EBADF:
                raise EOFError()
            else:
                raise
