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
from ..debug import debug
from ..tracing import inject_tracing_vars

MUST_SEND_RETRY_TIMES = 4
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
    def __init__(self, worker_type):
        # type: (str, dict) -> None
        self._worker_type = worker_type

        # Sub-process launched and poller used
        self._process = None
        self._poller = None

        # Retry counts when needing to send a MUST_SEND message
        self._send_mustsend_remaining_tries = 0
        # Keep track of the `mustsend` across restarts
        self._cached_mustsend = None
        # Tracks if a previous message had an error
        self._prev_message_error = False

        self.start()

    def start(self):

        if (
            self._worker_type is not None
            and self._worker_type.startswith(NULL_SIDECAR_PREFIX)
        ) or (platform.system() == "Darwin" and sys.version_info < (3, 0)):
            # If on darwin and running python 2 disable sidecars
            # there is a bug with importing poll from select in some cases
            #
            # TODO: Python 2 shipped by Anaconda allows for
            # `from select import poll`. We can consider enabling sidecars
            # for that distribution if needed at a later date.
            self._poller = NullPoller()
            self._process = None
            self._logger("No sidecar started")
        else:
            self._starting = True
            from select import poll

            python_version = sys.executable
            cmdline = [
                python_version,
                "-u",
                os.path.dirname(__file__) + "/sidecar_worker.py",
                self._worker_type,
            ]
            self._logger("Starting sidecar")
            debug.sidecar_exec(cmdline)

            self._process = self._start_subprocess(cmdline)

            if self._process is not None:
                fcntl.fcntl(self._process.stdin, F_SETFL, O_NONBLOCK)
                self._poller = poll()
                self._poller.register(self._process.stdin.fileno(), select.POLLOUT)
            else:
                # unable to start subprocess, fallback to Null sidecar
                self._logger("Unable to start subprocess")
                self._poller = NullPoller()

    def kill(self):
        try:
            msg = Message(MessageTypes.SHUTDOWN, None)
            self._emit_msg(msg)
        except:
            pass

    def send(self, msg, retries=3):
        if msg.msg_type == MessageTypes.MUST_SEND:
            # If this is a must-send message, we treat it a bit differently. A must-send
            # message has to be properly sent before any of the other best effort messages.
            self._cached_mustsend = msg.payload
            self._send_mustsend_remaining_tries = MUST_SEND_RETRY_TIMES
            self._send_mustsend(retries)
        else:
            # Ignore return code for send.
            self._send_internal(msg, retries=retries)

    def _start_subprocess(self, cmdline):
        for _ in range(3):
            try:
                env = os.environ.copy()
                inject_tracing_vars(env)
                # Set stdout=sys.stdout & stderr=sys.stderr
                # to print to console the output of sidecars.
                return subprocess.Popen(
                    cmdline,
                    stdin=subprocess.PIPE,
                    env=env,
                    stdout=sys.stdout if debug.sidecar else subprocess.DEVNULL,
                    stderr=sys.stderr if debug.sidecar else subprocess.DEVNULL,
                    bufsize=0,
                )
            except blockingError as be:
                self._logger("Sidecar popen failed: %s" % repr(be))
            except Exception as e:
                self._logger("Unknown popen error: %s" % repr(e))
                break

    def _send_internal(self, msg, retries=3):
        if self._process is None:
            return False
        try:
            if msg.msg_type == MessageTypes.BEST_EFFORT:
                # If we have a mustsend to send, we need to send it first prior to
                # sending a best-effort message
                if self._send_mustsend_remaining_tries == -1:
                    # We could not send the "mustsend" so we don't try to send this out;
                    # restart sidecar so use the PipeUnavailableError caught below
                    raise PipeUnavailableError()
                elif self._send_mustsend_remaining_tries > 0:
                    self._send_mustsend()
                if self._send_mustsend_remaining_tries == 0:
                    self._emit_msg(msg)
                    self._prev_message_error = False
                    return True
            else:
                self._emit_msg(msg)
                self._prev_message_error = False
                return True
            return False
        except MsgTimeoutError:
            # drop message, do not retry on timeout
            self._logger("Unable to send message due to timeout")
            self._prev_message_error = True
        except Exception as ex:
            if isinstance(ex, (PipeUnavailableError, BrokenPipeError)):
                self._logger("Restarting sidecar due to broken/unavailable pipe")
                self.start()
                if self._cached_mustsend is not None:
                    self._send_mustsend_remaining_tries = MUST_SEND_RETRY_TIMES
                    # We don't send the "must send" here, letting it send "lazily" on the
                    # next message. The reason for this is to simplify the interactions
                    # with the retry logic.
            else:
                self._prev_message_error = True
            if retries > 0:
                self._logger("Retrying msg send to sidecar (due to %s)" % repr(ex))
                return self._send_internal(msg, retries - 1)
            else:
                self._logger(
                    "Error sending log message (exhausted retries): %s" % repr(ex)
                )
        return False

    def _send_mustsend(self, retries=3):
        if (
            self._cached_mustsend is not None
            and self._send_mustsend_remaining_tries > 0
        ):
            # If we don't succeed in sending the must-send, we will try again
            # next time.
            if self._send_internal(
                Message(MessageTypes.MUST_SEND, self._cached_mustsend), retries
            ):
                self._cached_mustsend = None
                self._send_mustsend_remaining_tries = 0
                return True
            else:
                self._send_mustsend_remaining_tries -= 1
                if self._send_mustsend_remaining_tries == 0:
                    # Mark as "failed after try"
                    self._send_mustsend_remaining_tries = -1
                return False

    def _emit_msg(self, msg):
        # If the previous message had an error, we want to prepend a "\n" to this message
        # to maximize the chance of this message being valid (for example, if the
        # previous message only partially sent for whatever reason, we want to "clear" it)
        msg = msg.serialize()
        if self._prev_message_error:
            msg = "\n" + msg
        msg_ser = msg.encode("utf-8")
        written_bytes = 0
        while written_bytes < len(msg_ser):
            # self._logger("Sent %d out of %d bytes" % (written_bytes, len(msg_ser)))
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

    def _logger(self, msg):
        if debug.sidecar:
            print("[sidecar:%s] %s" % (self._worker_type, msg), file=sys.stderr)
