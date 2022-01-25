import enum
import sys

PY38 = sys.version_info[:2] == (3, 8)
PY37_PLUS = sys.version_info >= (3, 7)
PY38_PLUS = sys.version_info >= (3, 8)
PY39_PLUS = sys.version_info >= (3, 9)
PY310_PLUS = sys.version_info >= (3, 10)
BUILTINS = "builtins"  # TODO Remove in 2.8


class Context(enum.Enum):
    Load = 1
    Store = 2
    Del = 3


# TODO Remove in 3.0 in favor of Context
Load = Context.Load
Store = Context.Store
Del = Context.Del
