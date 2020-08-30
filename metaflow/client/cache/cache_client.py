import os
import time

from .cache_server import server_request, subprocess_cmd_and_env
from .cache_store import object_path, is_safely_readable

PING_FREQUENCY = 1

class CacheServerUnreachable(Exception):
    pass

class CacheFuture(object):

    def __init__(self, keys, stream_key, client, action_cls, root):
        self.stream_key = stream_key
        self.action = action_cls
        self.client = client
        self.keys = keys
        self.key_paths = {key: object_path(root, key) for key in keys}
        if stream_key:
            self.key_paths[stream_key] = object_path(root, stream_key)
        self.key_objs = None

    @property
    def is_ready(self):
        return all(map(is_safely_readable, self.key_paths.values()))

    @property
    def is_streamable(self):
        return bool(self.stream_key)

    def wait(self, timeout):
        return self.client.wait(list(self.key_paths.values()), timeout)

    def get(self):
        def _read(path):
            with open(path, 'rb') as f:
                return f.read()

        if self.key_objs is None and self.is_ready:
            self.key_objs = {key: _read(path)\
                             for key, path in self.key_paths.items()}
        if self.key_objs:
            return self.action.response(self.key_objs)

    def stream(self, timeout):
        # if a symlink if found, we can use it
        # if a symlink is missing or invalid, we should fall back
        # to the corresponding non-streaming key
        def _stream():
            pass
        if self.is_streamable:
            return self.client.stream(self.action.stream_response(_stream()))

class CacheClient(object):

    def __init__(self, root, action_classes, max_actions=16, max_size=10000):

        for cls in action_classes:
            setattr(self, cls.__name__, self._action(cls))

        self._root = root
        self._prev_is_alive = 0
        self._is_dead = False
        self._init(root, action_classes, max_actions, max_size)

    def _init(self, root, action_classes, max_actions, max_size):

        cmd, env = subprocess_cmd_and_env('cache_server')
        cmdline = cmd + [\
                   '--root', os.path.abspath(root),\
                   '--max-actions', str(max_actions),\
                   '--max-size', str(max_size)\
                ]
        self.start(cmdline, env)

        msg = {
            'actions': [[c.__module__, c.__name__] for c in action_classes]
        }
        self._send('init', message=msg)

    def is_alive(self):
        now = time.time()
        if self._is_dead:
            return False
        elif now - self._prev_is_alive > PING_FREQUENCY:
            self._prev_is_alive = now
            try:
                self._send('ping')
                return True
            except CacheServerUnreachable:
                self._is_dead = True
                return False
        else:
            return True

    def _send(self, op, **kwargs):
        self.send_request(server_request(op, **kwargs))

    def _action(self, cls):
        def _call(*args, **kwargs):
            msg, keys, stream_key, disposable_keys =\
                 cls.format_request(*args, **kwargs)
            future = CacheFuture(keys, stream_key, self, cls, self._root)
            if future.is_ready:
                # cache hit
                print('HIT!')
                return future
            else:
                # cache miss
                self._send('action',
                           action='%s.%s' % (cls.__module__, cls.__name__),
                           prio=cls.PRIORITY,
                           keys=keys,
                           stream_key=stream_key,
                           message=msg,
                           disposable_keys=disposable_keys)
                return future
        return _call



