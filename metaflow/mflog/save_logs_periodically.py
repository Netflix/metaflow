import os
import sys
import time
import subprocess
from threading import Thread

from metaflow.sidecar import MessageTypes
from . import update_delay, BASH_SAVE_LOGS_ARGS


class SaveLogsPeriodicallySidecar(object):
    def __init__(self):
        self._thread = Thread(target=self._update_loop)
        self.is_alive = True
        self._thread.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.is_alive = False

    @classmethod
    def get_worker(cls):
        return cls

    def _update_loop(self):
        def _file_size(path):
            if os.path.exists(path):
                return os.path.getsize(path)
            else:
                return 0

        # these env vars are set by mflog.mflog_env
        FILES = [os.environ["MFLOG_STDOUT"], os.environ["MFLOG_STDERR"]]
        start_time = time.time()
        sizes = [0 for _ in FILES]
        while self.is_alive:
            new_sizes = list(map(_file_size, FILES))
            if new_sizes != sizes:
                sizes = new_sizes
                try:
                    subprocess.call(BASH_SAVE_LOGS_ARGS)
                except:
                    pass
            time.sleep(update_delay(time.time() - start_time))
