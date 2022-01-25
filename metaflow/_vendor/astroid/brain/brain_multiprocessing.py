# Copyright (c) 2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 David Gilman <davidgilman1@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

from metaflow._vendor.astroid.bases import BoundMethod
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.exceptions import InferenceError
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.scoped_nodes import FunctionDef


def _multiprocessing_transform():
    module = parse(
        """
    from multiprocessing.managers import SyncManager
    def Manager():
        return SyncManager()
    """
    )
    # Multiprocessing uses a getattr lookup inside contexts,
    # in order to get the attributes they need. Since it's extremely
    # dynamic, we use this approach to fake it.
    node = parse(
        """
    from multiprocessing.context import DefaultContext, BaseContext
    default = DefaultContext()
    base = BaseContext()
    """
    )
    try:
        context = next(node["default"].infer())
        base = next(node["base"].infer())
    except (InferenceError, StopIteration):
        return module

    for node in (context, base):
        for key, value in node.locals.items():
            if key.startswith("_"):
                continue

            value = value[0]
            if isinstance(value, FunctionDef):
                # We need to rebound this, since otherwise
                # it will have an extra argument (self).
                value = BoundMethod(value, node)
            module[key] = value
    return module


def _multiprocessing_managers_transform():
    return parse(
        """
    import array
    import threading
    import multiprocessing.pool as pool
    import queue

    class Namespace(object):
        pass

    class Value(object):
        def __init__(self, typecode, value, lock=True):
            self._typecode = typecode
            self._value = value
        def get(self):
            return self._value
        def set(self, value):
            self._value = value
        def __repr__(self):
            return '%s(%r, %r)'%(type(self).__name__, self._typecode, self._value)
        value = property(get, set)

    def Array(typecode, sequence, lock=True):
        return array.array(typecode, sequence)

    class SyncManager(object):
        Queue = JoinableQueue = queue.Queue
        Event = threading.Event
        RLock = threading.RLock
        BoundedSemaphore = threading.BoundedSemaphore
        Condition = threading.Condition
        Barrier = threading.Barrier
        Pool = pool.Pool
        list = list
        dict = dict
        Value = Value
        Array = Array
        Namespace = Namespace
        __enter__ = lambda self: self
        __exit__ = lambda *args: args

        def start(self, initializer=None, initargs=None):
            pass
        def shutdown(self):
            pass
    """
    )


register_module_extender(
    AstroidManager(), "multiprocessing.managers", _multiprocessing_managers_transform
)
register_module_extender(
    AstroidManager(), "multiprocessing", _multiprocessing_transform
)
