import sys
import traceback

try:
    # Import from client
    from .data_transferer import DataTransferer
    from .stub import Stub
except ImportError:
    # Import from server
    from data_transferer import DataTransferer
    from stub import Stub


# This file is heavily inspired from the RPYC project
# The license for this project is reproduced here
# (from https://rpyc.readthedocs.io/en/latest/license.html):
# Copyright (c) 2005-2013
#   Tomer Filiba (tomerfiliba@gmail.com)
#   Copyrights of patches are held by their respective submitters
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

FIELD_EXC_MODULE = "m"
FIELD_EXC_NAME = "n"
FIELD_EXC_ARGS = "arg"
FIELD_EXC_ATTR = "atr"
FIELD_EXC_TB = "tb"
FIELD_EXC_USER = "u"
FIELD_EXC_SI = "si"
FIELD_EXC_STR = "s"
FIELD_EXC_REPR = "r"


def dump_exception(data_transferer, exception_type, exception_val, tb, user_data=None):
    if exception_type is StopIteration:  # Very common exception so we encode quickly
        return data_transferer.dump({FIELD_EXC_SI: True})
    local_formatted_exception = "".join(
        traceback.format_exception(exception_type, exception_val, tb)
    )
    exception_args = []
    exception_attrs = []
    str_repr = None
    repr_repr = None
    for name in dir(exception_val):
        if name == "args":
            # Handle arguments specifically to try to get all arguments even if
            # some require repr
            for arg in exception_val.args:
                if DataTransferer.can_simple_dump(arg):
                    exception_args.append(arg)
                else:
                    exception_args.append(repr(arg))
        elif name == "__str__":
            str_repr = str(exception_val)
        elif name == "__repr__":
            repr_repr = repr(exception_val)
        elif name.startswith("_") or name == "with_traceback":
            continue
        else:
            try:
                attr = getattr(exception_val, name)
            except AttributeError:
                continue
            if DataTransferer.can_simple_dump(attr):
                exception_attrs.append((name, attr))
            else:
                exception_attrs.append((name, repr(attr)))
    to_return = {
        FIELD_EXC_MODULE: exception_type.__module__,
        FIELD_EXC_NAME: exception_type.__name__,
        FIELD_EXC_ARGS: exception_args,
        FIELD_EXC_ATTR: exception_attrs,
        FIELD_EXC_TB: local_formatted_exception,
        FIELD_EXC_STR: str_repr,
        FIELD_EXC_REPR: repr_repr,
    }
    if user_data is not None:
        if DataTransferer.can_simple_dump(user_data):
            to_return[FIELD_EXC_USER] = user_data
        else:
            to_return[FIELD_EXC_USER] = repr(user_data)
    return data_transferer.dump(to_return)


def load_exception(data_transferer, json_obj):
    json_obj = data_transferer.load(json_obj)
    if json_obj.get(FIELD_EXC_SI) is not None:
        return StopIteration

    exception_module = json_obj.get(FIELD_EXC_MODULE)
    exception_name = json_obj.get(FIELD_EXC_NAME)
    exception_class = None
    if exception_module not in sys.modules:
        # Try to import the module
        try:
            # Use __import__ so that the user can access this exception
            __import__(exception_module, None, None, "*")
        except Exception:
            pass
    # Try again (will succeed if the __import__ worked)
    if exception_module in sys.modules:
        exception_class = getattr(sys.modules[exception_module], exception_name, None)
    if exception_class is None or issubclass(exception_class, Stub):
        # Best effort to "recreate" an exception. Note that in some cases, exceptions
        # may actually be both exceptions we can transfer as well as classes we
        # can transfer (stubs) but for exceptions, we don't want to use the stub
        # otherwise it will just ping pong.
        name = "%s.%s" % (exception_module, exception_name)
        exception_class = _remote_exceptions_class.setdefault(
            name,
            type(
                name,
                (RemoteInterpreterException,),
                {"__module__": "%s/%s" % (__name__, exception_module)},
            ),
        )
    exception_class = _wrap_exception(exception_class)
    raised_exception = exception_class.__new__(exception_class)
    raised_exception.args = json_obj.get(FIELD_EXC_ARGS)
    for name, attr in json_obj.get(FIELD_EXC_ATTR):
        try:
            if name in raised_exception.__user_defined__:
                setattr(raised_exception, "_original_%s" % name, attr)
            else:
                setattr(raised_exception, name, attr)
        except AttributeError:
            # In case some things are read only
            pass
    s = json_obj.get(FIELD_EXC_STR)
    if s:
        try:
            if "__str__" in raised_exception.__user_defined__:
                setattr(raised_exception, "_original___str__", s)
            else:
                setattr(raised_exception, "__str__", lambda x, s=s: s)
        except AttributeError:
            raised_exception._missing_str = True
    s = json_obj.get(FIELD_EXC_REPR)
    if s:
        try:
            if "__repr__" in raised_exception.__user_defined__:
                setattr(raised_exception, "_original___repr__", s)
            else:
                setattr(raised_exception, "__repr__", lambda x, s=s: s)
        except AttributeError:
            raised_exception._missing_repr = True
    user_args = json_obj.get(FIELD_EXC_USER)
    if user_args is not None:
        try:
            deserializer = getattr(raised_exception, "_deserialize_user")
        except AttributeError:
            raised_exception._missing_deserializer = True
        else:
            deserializer(user_args)
    raised_exception._remote_tb = json_obj[FIELD_EXC_TB]
    return raised_exception


def _wrap_exception(exception_class):
    to_return = _derived_exceptions.get(exception_class)
    if to_return is not None:
        return to_return

    class WithPrettyPrinting(exception_class):
        def __str__(self):
            try:
                text = super(WithPrettyPrinting, self).__str__()
            except:  # noqa E722
                text = "<Garbled exception>"
            # if getattr(self, "_missing_deserializer", False):
            #     text += (
            #         "\n\n===== WARNING: User data from the exception was not deserialized "
            #         "-- possible missing information =====\n"
            #     )
            # if getattr(self, "_missing_str", False):
            #     text += "\n\n===== WARNING: Could not set class specific __str__ "
            #     "-- possible missing information =====\n"
            # if getattr(self, "_missing_repr", False):
            #     text += "\n\n===== WARNING: Could not set class specific __repr__ "
            #     "-- possible missing information =====\n"
            remote_tb = getattr(self, "_remote_tb", "No remote traceback available")
            text += "\n\n===== Remote (on server) traceback =====\n"
            text += remote_tb
            text += "========================================\n"
            return text

    WithPrettyPrinting.__name__ = exception_class.__name__
    WithPrettyPrinting.__module__ = exception_class.__module__
    WithPrettyPrinting.__realclass__ = exception_class
    _derived_exceptions[exception_class] = WithPrettyPrinting
    return WithPrettyPrinting


class RemoteInterpreterException(Exception):
    """A 'generic exception' that is raised when the exception the gotten from
    the remote server cannot be instantiated locally"""

    pass


_remote_exceptions_class = {}  # Exception name -> type of that exception
_derived_exceptions = {}
