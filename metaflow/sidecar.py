from __future__ import print_function

import subprocess
import fcntl
import select
import os
import sys
import platform

from fcntl import F_SETFL
from os import O_NONBLOCK

from .sidecar_messages import Message, MessageTypes
from .debug import debug

MESSAGE_WRITE_TIMEOUT_IN_MS = 1000

NULL_SIDECAR_PREFIX = "nullSidecar"

# for python 2 compatibility
try:
    blockingError = BlockingIOError
except:
    blockingError = OSError


class PipeUnavailableError(Exception):
    """raised when unable to write to pipe given allotted time"""

    pass


class NullSidecarError(Exception):
    """raised when trying to poll or interact with the fake subprocess in the null sidecar"""

    pass


class MsgTimeoutError(Exception):
    """raised when trying unable to send message to sidecar in allocated time"""

    pass


class NullPoller(object):
    def poll(self, timeout):
        raise NullSidecarError()


class SidecarSubProcess(object):
    def __init__(self, worker_type, context=None):
        # type: (str, dict) -> None
        self._worker_type = worker_type
        self._process = None
        self._poller = None
        self._context = context
        self.start()

    def start(self):

        if (
            self._worker_type is not None
            and self._worker_type.startswith(NULL_SIDECAR_PREFIX)
        ) or (platform.system() == "Darwin" and sys.version_info < (3, 0)):
            # if on darwin and running python 2 disable sidecars
            # there is a bug with importing poll from select in some cases
            #
            # TODO: Python 2 shipped by Anaconda allows for
            # `from select import poll`. We can consider enabling sidecars
            # for that distribution if needed at a later date.
            self._poller = NullPoller()

        else:
            from select import poll

            python_version = sys.executable
            cmdline = [
                python_version,
                "-u",
                os.path.dirname(__file__) + "/sidecar_worker.py",
                self._worker_type,
            ]
            debug.sidecar_exec(cmdline)

            self._process = self._start_subprocess(cmdline)

            if self._process is not None:
                fcntl.fcntl(self._process.stdin, F_SETFL, O_NONBLOCK)
                self._poller = poll()
                self._poller.register(self._process.stdin.fileno(), select.POLLOUT)

                if self._context is not None:
                    self._emit_msg(Message(MessageTypes.START, self._context))
            else:
                # unable to start subprocess, fallback to Null sidecar
                self.logger(
                    "Unable to start subprocess for sidecar %s" % self._worker_type
                )
                self._poller = NullPoller()

    def kill(self):
        try:
            msg = Message(MessageTypes.SHUTDOWN, None)
            self._emit_msg(msg)
        except:
            pass

    def send(self, msg, retries=3):
        try:
            self._emit_msg(msg)
        except MsgTimeoutError:
            # drop message, do not retry on timeout
            self.logger("Unable to send message due to timeout")
        except Exception as ex:
            if isinstance(ex, PipeUnavailableError):
                self.logger("Restarting sidecar %s" % self._worker_type)
                self.start()
            if retries > 0:
                self.logger("Retrying msg send to sidecar")
                return self.send(msg, retries - 1)
            else:
                self.logger("Error sending log message")
                self.logger(repr(ex))

    def _start_subprocess(self, cmdline):
        for _ in range(3):
            try:
                # Set stdout=sys.stdout & stderr=sys.stderr
                # to print to console the output of sidecars.
                return subprocess.Popen(
                    cmdline,
                    stdin=subprocess.PIPE,
                    stdout=sys.stdout if debug.sidecar else subprocess.DEVNULL,
                    stderr=sys.stderr if debug.sidecar else subprocess.DEVNULL,
                    bufsize=0,
                )
            except blockingError as be:
                self.logger("Warning: sidecar popen failed: %s" % repr(be))
            except Exception as e:
                self.logger(repr(e))
                break

    def _emit_msg(self, msg):
        msg_ser = msg.serialize().encode("utf-8")
        written_bytes = 0
        while written_bytes < len(msg_ser):
            try:
                fds = self._poller.poll(MESSAGE_WRITE_TIMEOUT_IN_MS)
                if fds is None or len(fds) == 0:
                    raise MsgTimeoutError("Poller timed out")
                for fd, event in fds:
                    if event & select.POLLERR:
                        raise PipeUnavailableError("Pipe unavailable")
                    f = os.write(fd, msg_ser[written_bytes:])
                    written_bytes += f
            except NullSidecarError:
                # sidecar is disabled, ignore all messages
                break

    def logger(self, msg):
        if debug.sidecar:
            print(msg, file=sys.stderr)
