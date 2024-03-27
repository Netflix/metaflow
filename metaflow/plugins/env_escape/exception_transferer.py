import sys
import traceback

try:
    # Import from client
    from .data_transferer import DataTransferer
except ImportError:
    # Import from server
    from data_transferer import DataTransferer


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
    to_return = {
        FIELD_EXC_MODULE: exception_type.__module__,
        FIELD_EXC_NAME: exception_type.__name__,
        FIELD_EXC_ARGS: exception_args,
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


def load_exception(client, json_obj):
    from .stub import Stub

    json_obj = client.decode(json_obj)

    if json_obj.get(FIELD_EXC_SI) is not None:
        return StopIteration

    exception_module = json_obj.get(FIELD_EXC_MODULE)
    exception_name = json_obj.get(FIELD_EXC_NAME)
    exception_class = None
    # This name is already cannonical since we cannonicalize it on the server side
    full_name = "%s.%s" % (exception_module, exception_name)

    exception_class = client.get_local_class(full_name, is_returned_exception=True)

    if issubclass(exception_class, Stub):
        raised_exception = exception_class(_is_returned_exception=True)
        raised_exception.args = tuple(json_obj.get(FIELD_EXC_ARGS))
    else:
        raised_exception = exception_class(*json_obj.get(FIELD_EXC_ARGS))
    raised_exception._exception_str = json_obj.get(FIELD_EXC_STR, None)
    raised_exception._exception_repr = json_obj.get(FIELD_EXC_REPR, None)
    raised_exception._exception_tb = json_obj.get(FIELD_EXC_TB, None)

    user_args = json_obj.get(FIELD_EXC_USER)
    if user_args is not None:
        deserializer = client.get_exception_deserializer(full_name)
        if deserializer is not None:
            deserializer(raised_exception, user_args)
    return raised_exception


class ExceptionMetaClass(type):
    def __init__(cls, class_name, base_classes, class_dict):
        super(ExceptionMetaClass, cls).__init__(class_name, base_classes, class_dict)
        cls.__orig_str__ = cls.__str__
        cls.__orig_repr__ = cls.__repr__
        for n in ("_exception_str", "_exception_repr", "_exception_tb"):
            setattr(
                cls,
                n,
                property(
                    lambda self, n=n: getattr(self, "%s_val" % n, "<missing>"),
                    lambda self, v, n=n: setattr(self, "%s_val" % n, v),
                ),
            )

        def _do_str(self):
            text = self._exception_str
            text += "\n\n===== Remote (on server) traceback =====\n"
            text += self._exception_tb
            text += "========================================\n"
            return text

        cls.__str__ = _do_str
        cls.__repr__ = lambda self: self._exception_repr


class RemoteInterpreterException(Exception):
    """
    A 'generic' exception that was raised on the server side for which we have no
    equivalent exception on this side
    """

    pass
