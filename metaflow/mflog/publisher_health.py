import os

from . import TASK_LOG_SOURCE
from .mflog import decorate


def write_uploader_log(message):
    payload = decorate(TASK_LOG_SOURCE, "%s\n" % message)
    with open(os.environ["MFLOG_STDERR"], "ab", buffering=0) as log:
        log.write(payload)
    return len(payload)
