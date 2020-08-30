import json
from subprocess import Popen, PIPE

from .cache_client import CacheClient, CacheServerUnreachable

class CacheSyncClient(CacheClient):

    def start(self, cmdline, env):
        self._proc = Popen(cmdline, env=env, stdin=PIPE)

    def send_request(self, msg):
        print("SEND", msg)
        encoded = json.dumps(msg).encode('utf-8')
        try:
            self._proc.stdin.write(encoded + b'\n')
            self._proc.stdin.flush()
        except BrokenPipeError:
            raise CacheServerUnreachable()

