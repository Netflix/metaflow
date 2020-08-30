import time
from subprocess import Popen, PIPE

from .cache_client import CacheClient, CacheServerUnreachable

class CacheSyncClient(CacheClient):

    def start(self, cmdline, env):
        self._proc = Popen(cmdline, env=env, stdin=PIPE)

    def send_request(self, blob):
        try:
            self._proc.stdin.write(blob)
            self._proc.stdin.flush()
        except BrokenPipeError:
            raise CacheServerUnreachable()

    def sleep(self, sec):
        time.sleep(sec)

