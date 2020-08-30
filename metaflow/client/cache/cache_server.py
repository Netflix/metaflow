import os
import io
import sys
import json
import fcntl
import hashlib
from itertools import chain
from datetime import datetime
from subprocess import Popen, PIPE
from collections import deque, OrderedDict

import click
from metaflow.procpoll import make_poll

from .cache_action import CacheAction,\
                          LO_PRIO,\
                          HI_PRIO,\
                          import_action_class

from .cache_store import CacheStore,\
                         key_filename

class CacheServerException(Exception):
    pass

def server_request(op,
                   action=None,
                   prio=None,
                   keys=None,
                   stream_key=None,
                   message=None,
                   disposable_keys=None,
                   idempotency_token=None):

    if idempotency_token is None:
        fields = [op]
        if action:
            fields.append(action)
        if keys:
            fields.extend(sorted(keys))
        if stream_key:
            fields.append(stream_key)
        token = hashlib.sha1('|'.join(fields).encode('utf-8')).hexdigest()
    else:
        token = idempotency_token

    return {
        'op': op,
        'action': action,
        'priority': prio,
        'keys': keys,
        'stream_key': stream_key,
        'message': message,
        'idempotency_token': token,
        'disposable_keys': disposable_keys
    }

def echo(msg):
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
    sys.stderr.write('CACHE [%s] %s\n' % (now, msg))

class MessageReader(object):

    def __init__(self, fd):
        # make fd non-blocking
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        self.buf = io.BytesIO()
        self.fd = fd

    def messages(self):
        while True:
            try:
                b = os.read(self.fd, 65536)
                if not b:
                    return
            except OSError as e:
                if e.errno == 11: # EAGAIN
                    return
            else:
                self.buf.write(b)
                if b'\n' in b:
                    new_buf = io.BytesIO()
                    self.buf.seek(0)
                    for line in self.buf:
                        if line.endswith(b'\n'):
                            try:
                                yield json.loads(line)
                            except:
                                raise
                                uni = line.decode('utf-8', errors='replace')
                                echo("WARNING: Corrupted message: %s" % uni)
                        else:
                            # return the last partial line back to the buffer
                            new_buf.write(line)
                    self.buf = new_buf

    def close(self):
        tail = self.buf.getvalue()
        if tail:
            uni = tail.decode('utf-8', errors='replace')
            echo("WARNING: Truncated message: %s" % uni)

def subprocess_cmd_and_env(mod):
    pypath = os.environ.get('PYTHONPATH', '')
    env = os.environ.copy()
    env['PYTHONPATH'] = ':'.join((os.getcwd(), pypath))
    return [sys.executable, '-m', 'metaflow.client.cache.%s' % mod], env

class Worker(object):

    def __init__(self, request, filestore):
        self.request = request
        self.prio = request['priority']
        self.filestore = filestore
        self.proc = None
        self.tempdir = self.filestore.open_tempdir(request['idempotency_token'],
                                                   request['action'],
                                                   request['stream_key'])
        if self.tempdir is None:
            self.echo("Store couldn't create a temp directory. "\
                      "WORKER NOT STARTED.")

    def start(self):

        with open(os.path.join(self.tempdir, 'request.json'), 'w') as f:
            stream = self.request['stream_key']
            request = {
                'message': self.request['message'],
                'keys': {key: key_filename(key) for key in self.request['keys']},
                'stream_key': key_filename(stream) if stream else None
            }
            json.dump(request, f)

        cmd, env = subprocess_cmd_and_env('cache_worker')
        cmdline = cmd + [\
                   '--request-file', 'request.json',\
                   self.request['action']\
                  ]

        self.proc = Popen(cmdline, cwd=self.tempdir, stdin=PIPE)
        self.fd = self.proc.stdin.fileno()

    def echo(self, msg):
        token = self.request['idempotency_token']
        pid_prefix = '[pid %d]' % self.proc.pid if self.proc else ''
        echo("Worker%s[token %s] %s" % (pid_prefix, token, msg))

    def terminate(self):
        ret = self.proc.wait()
        if ret:
            self.echo("crashed with error code %d" % ret)
        else:
            missing = self.filestore.commit(self.tempdir,
                                            self.request['keys'],
                                            self.request['stream_key'],
                                            self.request['disposable_keys'])
            if missing:
                self.echo("failed to produce the following keys: %s"\
                          % ','.join(missing))

        self.filestore.close_tempdir(self.tempdir)

    def kill(self):
        self.proc.kill()

class Scheduler(object):

    def __init__(self, filestore, max_workers):

        self.filestore = filestore
        self.max_workers = max_workers
        self.stdin_fileno = sys.stdin.fileno()
        self.stdin_reader = MessageReader(self.stdin_fileno)

        self.pending_requests = set()
        self.lo_prio_requests = deque()
        self.hi_prio_requests = deque()
        self.actions = []

    def process_incoming_request(self):
        for msg in self.stdin_reader.messages():
            op = msg['op']
            prio = msg['priority']
            action = msg['action']

            echo("MESSAGE: %s" % msg)

            if op == 'ping':
                pass
            elif op == 'init':
                actions = msg['message']['actions']
                self.validate_actions(actions)
                self.actions = frozenset('.'.join(act) for act in actions)
            elif op == 'action':
                if action not in self.actions:
                    raise CacheServerException("Unknown action: '%s'" % action)
                if msg['idempotency_token'] not in self.pending_requests:
                    self.pending_requests.add(msg['idempotency_token'])
                    if prio == HI_PRIO:
                        self.hi_prio_requests.append(msg)
                    elif prio == LO_PRIO:
                        self.lo_prio_requests.append(msg)
                    else:
                        raise CacheServerException("Unknown priority: '%s'" % prio)
            else:
                raise CacheServerException("Unknown op: '%s'" % op)

    def validate_actions(self, actions):
        for mod_str, cls_str in actions:
            try:
                cls = import_action_class(mod_str, cls_str)
                if not issubclass(cls, CacheAction):
                    raise CacheServerException("Invalid action: %s.%s"\
                                               % (mod_str, cls_str))
            except ImportError:
                raise CacheServerException("Import failed: %s.%s"\
                                           % (mod_str, cls_str))

    def schedule(self):

        def queued_request(queue):
            while queue:
                yield queue.popleft()

        for request in chain(queued_request(self.hi_prio_requests),
                             queued_request(self.lo_prio_requests)):

            worker = Worker(request, self.filestore)
            if worker.tempdir:
                worker.start()
                return worker

    def loop(self):

        workers = {HI_PRIO: {}, LO_PRIO: OrderedDict()}
        poller = make_poll()
        poller.add(self.stdin_fileno)

        def new_worker():
            worker = self.schedule()
            if worker:
                poller.add(worker.fd)
                workers[worker.prio][worker.fd] = worker
                return worker

        while True:
            for event in poller.poll(10000):

                if event.fd == self.stdin_fileno:
                    if event.is_terminated:
                        self.stdin_reader.close()
                        echo("Parent closed the connection. Terminating.")
                        return
                    else:
                        self.process_incoming_request()

                        num_workers = sum(map(len, workers.values()))
                        while num_workers < self.max_workers:
                            if new_worker():
                                num_workers += 1
                            else:
                                break

                        if self.hi_prio_requests and workers[LO_PRIO]:
                            _, worker = workers[LO_PRIO].popitem()
                            worker.kill()

                else:
                    if event.fd in workers[HI_PRIO]:
                        worker = workers[HI_PRIO][event.fd]
                    else:
                        worker = workers[LO_PRIO][event.fd]

                    if event.is_terminated:
                        worker.terminate()
                        token = worker.request['idempotency_token']
                        self.pending_requests.remove(token)
                        workers[worker.prio].pop(event.fd)
                        poller.remove(event.fd)
                        new_worker()

@click.command()
@click.option("--root",
              default='cache_data',
              help="Where to store cached objects on disk.")
@click.option("--max-actions",
              default=16,
              help="Maximum number of concurrent cache actions.")
@click.option("--max-size",
              default=10000,
              help="Maximum amount of disk space to use in MB.")
def cli(root=None,
        max_actions=None,
        max_size=None):
    store = CacheStore(root, max_size, echo)
    Scheduler(store, max_actions).loop()

if __name__ == '__main__':
    cli(auto_envvar_prefix='MFCACHE')
