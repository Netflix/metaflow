 # flake8: noqa
try:
    from .utils_py3 import __aenter__
    from .utils_py3 import __aexit__
    from .utils_py3 import __aiter__
    from .utils_py3 import __anext__
    from .utils_py3 import __await__
    from .utils_py3 import await_
except (ImportError, SyntaxError):
    await_ = None


def identity(obj):
    return obj


class cached_property(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value
