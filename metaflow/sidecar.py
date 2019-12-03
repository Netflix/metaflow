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
    """raised when tyring to poll or interact with the fake subprocess in the null sidecar"""
    pass


class MsgTimeoutError(Exception):
    """raised when tyring unable to send message to sidecar in allocated time"""
    pass


class NullPoller(object):
    def poll(self, timeout):
        raise NullSidecarError()


class SidecarSubProcess(object):

    def __init__(self, worker_type):
        # type: (str) -> None
        self.__worker_type = worker_type
        self.__process = None
        self.__poller = None
        self.start()

    def start(self):

        if (self.__worker_type is not None and \
                self.__worker_type.startswith(NULL_SIDECAR_PREFIX)) or \
                platform.system() == 'Darwin':
            self.__poller = NullPoller()

        else:
            from select import poll
            python_version = sys.executable

            cmdline = [python_version,
                       '-u',
                       os.path.dirname(__file__) + '/sidecar_worker.py',
                       self.__worker_type]
            debug.sidecar_exec(cmdline)

            self.__process = self.__start_subprocess(cmdline)

            if self.__process is not None:
                fcntl.fcntl(self.__process.stdin, F_SETFL, O_NONBLOCK)
                self.__poller = poll()
                self.__poller.register(self.__process.stdin.fileno(),
                                       select.POLLOUT)
            else:
                # unable to start subprocess, fallback to Null sidecar
                self.logger("unable to start subprocess for sidecar %s"
                      % self.__worker_type)
                self.__poller = NullPoller()

    def __start_subprocess(self, cmdline):
        for i in range(3):
            try:
                return subprocess.Popen(cmdline,
                                        stdin=subprocess.PIPE,
                                        stdout=open(os.devnull, 'w'),
                                        bufsize=0)
            except blockingError as be:
                self.logger("warning: sidecar popen failed: %s" % repr(be))
            except Exception as e:
                self.logger(repr(e))
                break

    def kill(self):
        try:
            msg = Message(MessageTypes.SHUTDOWN, None)
            self.emit_msg(msg)
        except:
            pass

    def emit_msg(self, msg):
        msg_ser = msg.serialize().encode('utf-8')
        written_bytes = 0
        while written_bytes < len(msg_ser):
            try:
                fds = self.__poller.poll(MESSAGE_WRITE_TIMEOUT_IN_MS)
                if fds is None or len(fds) == 0:
                    raise MsgTimeoutError("poller timed out")
                for fd, event in fds:
                    if event & select.POLLERR:
                        raise PipeUnavailableError('pipe unavailable')
                    f = os.write(fd, msg_ser[written_bytes:])
                    written_bytes += f
            except NullSidecarError:
                # sidecar is disabled, ignore all messages
                break

    def msg_handler(self, msg, retries=3):
        try:
            self.emit_msg(msg)
        except MsgTimeoutError:
            # drop message, do not retry on timeout
            self.logger("unable to send message due to timeout")
        except Exception as ex:
            if isinstance(ex, PipeUnavailableError):
                self.logger("restarting sidecar %s" % self.__worker_type)
                self.start()
            if retries > 0:
                self.logger("retrying msg send to sidecar")
                self.msg_handler(msg, retries-1)
            else:
                self.logger("error sending log message")
                self.logger(repr(ex))

    def logger(self, msg):
        print("metaflow logger: " + msg, file=sys.stderr)
