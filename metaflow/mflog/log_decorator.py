"""
Stream decorators for mflog — wraps stdout/stderr with mflog markers.

Used by non-CLI task executors (e.g., Ray workers) that need to produce
properly decorated log output for S3 upload and live tailing.
"""

import logging
import sys

from .mflog import decorate
from .save_logs import save_logs
from . import TASK_LOG_SOURCE
from ..sidecar import Sidecar


class MFLogDecorate:
    """Wraps a file object to buffer partial lines and decorate complete lines
    with mflog timestamps/source markers before writing."""

    def __init__(self, file, backend_stream):
        self._file = file
        self._backend_stream = backend_stream
        self._buf = []
        self._buf_sz = 0
        self._max_sz = 1024 * 1024  # 1 MB buffer limit
        self._did_flush = False
        self._cleanup = str.maketrans({"\x08": None, "\r": "\n"})

    def _emit_line(self, line):
        self._file.write(decorate(TASK_LOG_SOURCE, line + "\n"))

    def _flush_buf(self):
        if self._buf:
            self._emit_line("".join(self._buf))
            self._buf = []
            self._buf_sz = 0

    def write(self, text):
        if text is None:
            self._file.write(text)
            return
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        text = text.translate(self._cleanup)
        if text.endswith("\n\n"):
            text = text[:-1]
        if not text:
            return

        # After flush(), swallow a bare newline to prevent double-decorated empty line.
        if text == "\n" and self._did_flush:
            self._did_flush = False
            return
        self._did_flush = False

        lines = text.split("\n")
        for part in lines[:-1]:
            self._buf.append(part)
            self._flush_buf()

        remainder = lines[-1]
        if remainder:
            self._buf.append(remainder)
            self._buf_sz += len(remainder)
        if self._buf_sz > self._max_sz:
            self._flush_buf()

    def flush(self):
        self._flush_buf()
        self._did_flush = True
        self._file.flush()

    def close(self):
        self.flush()
        self._file.close()

    def __getattr__(self, name):
        return getattr(self._backend_stream, name)


class LogsRedirector:
    """Context manager that redirects stdout/stderr through MFLogDecorate
    and starts a sidecar for periodic S3 upload."""

    def __init__(self, stdout_filename, stderr_filename):
        self._stdout_filename = stdout_filename
        self._stderr_filename = stderr_filename
        self._stdout = None
        self._stderr = None
        self._sidecar = None

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = MFLogDecorate(open(self._stdout_filename, "wb"), self._stdout)
        sys.stderr = MFLogDecorate(open(self._stderr_filename, "wb"), self._stderr)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if hasattr(handler, "stream"):
                handler.stream = sys.stdout
        self._sidecar = Sidecar("save_logs_periodically")
        return self._sidecar

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = self._stdout
        sys.stderr = self._stderr
        if self._sidecar:
            self._sidecar.terminate()
        save_logs()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if hasattr(handler, "stream"):
                handler.stream = sys.stdout


def simple_logger(body="", system_msg=False, head="", bad=False, timestamp=True, nl=True):
    """Simplified Metaflow logger for non-CLI contexts (e.g., Ray workers)."""
    import datetime

    parts = []
    if timestamp:
        dt = timestamp if not isinstance(timestamp, bool) else datetime.datetime.now()
        parts.append(dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " ")
    if head:
        parts.append(head)
    if bad:
        parts.append("[ERROR] ")
    elif system_msg:
        parts.append("[SYSTEM] ")
    parts.append(body)
    print("".join(parts), end="\n" if nl else "")
