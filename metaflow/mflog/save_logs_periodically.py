import os
import sys
import time
import subprocess
from threading import Event, Thread

from metaflow.sidecar import MessageTypes
from . import update_delay, BASH_SAVE_LOGS_ARGS


class SaveLogsPeriodicallySidecar(object):
    def __init__(self):
        self._stop = Event()
        self._thread = Thread(target=self._update_loop)
        self._thread.daemon = True
        self._thread.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.shutdown()

    def shutdown(self):
        self._stop.set()
        self._thread.join()
        # flush any log output written since the last periodic upload
        try:
            subprocess.call(BASH_SAVE_LOGS_ARGS)
        except:
            pass

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
        while not self._stop.is_set():
            new_sizes = list(map(_file_size, FILES))
            if new_sizes != sizes:
                sizes = new_sizes
                try:
                    subprocess.call(BASH_SAVE_LOGS_ARGS)
                except:
                    pass
            self._stop.wait(timeout=update_delay(time.time() - start_time))
