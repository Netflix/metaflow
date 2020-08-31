import time
from subprocess import Popen, PIPE

from .cache_client import CacheClient, CacheServerUnreachable

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 10

class CacheSyncClient(CacheClient):

    def start_server(self, cmdline, env):
        self._proc = Popen(cmdline, env=env, stdin=PIPE)
        self._prev_heartbeat = 0

    def send_request(self, blob):
        try:
            self._proc.stdin.write(blob)
            self._proc.stdin.flush()
        except BrokenPipeError:
            self.is_alive = False
            raise CacheServerUnreachable()

    def wait_iter(self, it, timeout):
        end = time.time() + timeout
        for obj in it:
            if obj is None:
                time.sleep(WAIT_FREQUENCY)
                if not self._heartbeat():
                    raise CacheServerUnreachable()
                elif time.time() > end:
                    raise CacheClientTimeout()
            else:
                yield obj

    def wait(self, fun, timeout):
        end = time.time() + timeout
        while time.time() < end:
            ret = fun()
            if ret is None:
                time.sleep(WAIT_FREQUENCY)
                if not self._heartbeat():
                    raise CacheServerUnreachable()
            else:
                return ret
        raise CacheClientTimeout()

    def request_and_return(self, reqs, ret):
        return ret

    def _heartbeat(self):
        if self._is_alive:
            now = time.time()
            if now - self._prev_heartbeat > HEARTBEAT_FREQUENCY:
                self._prev_heartbeat = now
                try:
                    self.ping()
                except CacheServerUnreachable:
                    self._is_alive = False
        return self._is_alive
