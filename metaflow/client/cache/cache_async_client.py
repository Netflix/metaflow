import time
import asyncio
from subprocess import PIPE

from .cache_client import CacheClient, CacheServerUnreachable

WAIT_FREQUENCY = 0.2
HEARTBEAT_FREQUENCY = 1

class CacheAsyncClient(CacheClient):

    async def start_server(self, cmdline, env):
        self._proc = await asyncio.create_subprocess_exec(*cmdline,
                                                          env=env,
                                                          stdin=PIPE)
        asyncio.create_task(self._heartbeat())

    async def send_request(self, blob):
        try:
            self._proc.stdin.write(blob)
            await self._proc.stdin.drain()
        except BrokenPipeError: # FIXME check error
            self.is_alive = False
            raise CacheServerUnreachable()

    async def wait_iter(self, it, timeout):
        end = time.time() + timeout
        for obj in it:
            if obj is None:
                await asyncio.sleep(WAIT_FREQUENCY)
                if not self.is_alive:
                    raise CacheServerUnreachable()
                elif time.time() > end:
                    raise CacheClientTimeout()
            else:
                yield obj

    async def wait(self, fun, timeout):
        def _repeat():
            while True:
                yield fun()

        async for obj in self.wait_iter(_repeat(), timeout):
            return obj

    async def request_and_return(self, reqs, ret):
        for req in reqs:
            await req
        return ret

    async def _heartbeat(self):
        while self.is_alive:
            try:
                await self.ping()
            except CacheServerUnreachable:
                self._is_alive = False
            await asyncio.sleep(HEARTBEAT_FREQUENCY)
