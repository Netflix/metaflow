import functools
import pickle
from typing import Any

from .consts import (
    OP_GETATTR,
    OP_SETATTR,
    OP_DELATTR,
    OP_CALL,
    OP_CALLATTR,
    OP_CALLONCLASS,
    OP_DEL,
    OP_REPR,
    OP_STR,
    OP_HASH,
    OP_PICKLE,
    OP_DIR,
    OP_INIT,
    OP_SUBCLASSCHECK,
)

from .exception_transferer import ExceptionMetaClass

DELETED_ATTRS = frozenset(["__array_struct__", "__array_interface__"])

# These attributes are accessed directly on the stub (not directly forwarded)
LOCAL_ATTRS = (
    frozenset(
        [
            "___remote_class_name___",
            "___identifier___",
            "___connection___",
            "___local_overrides___",
            "___is_returned_exception___",
            "___exception_attributes___",
            "__class__",
            "__init__",
            "__del__",
            "__delattr__",
            "__dir__",
            "__doc__",
            "__getattr__",
            "__getattribute__",
            "__hash__",
            "__instancecheck__",
            "__subclasscheck__",
            "__init__",
            "__metaclass__",
            "__module__",
            "__name__",
            "__new__",
            "__reduce__",
            "__reduce_ex__",
            "__repr__",
            "__setattr__",
            "__slots__",
            "__str__",
            "__weakref__",
            "__dict__",
            "__methods__",
            "__exit__",
        ]
    )
    | DELETED_ATTRS
)

NORMAL_METHOD = 0
STATIC_METHOD = 1
CLASS_METHOD = 2


def fwd_request(stub, request_type, *args, **kwargs):
    connection = object.__getattribute__(stub, "___connection___")
    if connection:
        return connection.stub_request(stub, request_type, *args, **kwargs)
    raise RuntimeError(
        "Returned exception stub cannot be used to make further remote requests"
    )


class StubMetaClass(type):
    def __repr__(cls):
        if cls.__module__:
            return "<stub class '%s.%s'>" % (cls.__module__, cls.__name__)
        else:
            return "<stub class '%s'>" % (cls.__name__,)


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""

    # Compatibility 2/3. Remove when only 3 support
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)

    return type.__new__(metaclass, "temporary_class", (), {})


class Stub(with_metaclass(StubMetaClass, object)):
    """
    Local reference to a remote object.

    The stub looks and behaves like the remote object but all operations on the stub
    happen on the remote side (server).
    """

    __slots__ = ()
    # def __iter__(self):  # FIXME: Keep debugger QUIET!!
    #    raise AttributeError

    def __init__(
        self, connection, remote_class_name, identifier, _is_returned_exception=False
    ):
        self.___remote_class_name___ = remote_class_name
        self.___identifier___ = identifier
        self.___connection___ = connection
        # If it is a returned exception (ie: it was raised by the server), it behaves
        # a bit differently for methods like __str__ and __repr__ (we try not to get
        # stuff from the server)
        self.___is_returned_exception___ = _is_returned_exception

    def __del__(self):
        try:
            if not self.___is_returned_exception___:
                fwd_request(self, OP_DEL)
        except Exception:
            # raised in a destructor, most likely on program termination,
            # when the connection might have already been closed.
            # it's safe to ignore all exceptions here
            pass

    def __getattribute__(self, name):
        if name in LOCAL_ATTRS:
            if name == "__doc__":
                return self.__getattr__("__doc__")
            elif name in DELETED_ATTRS:
                raise AttributeError()
            else:
                return object.__getattribute__(self, name)
        elif name == "__call__":  # IronPython issue #10
            return object.__getattribute__(self, "__call__")
        elif name == "__array__":
            return object.__getattribute__(self, "__array__")
        else:
            return object.__getattribute__(self, name)

    def __getattr__(self, name):
        if name in DELETED_ATTRS or self.___is_returned_exception___:
            raise AttributeError()
        return fwd_request(self, OP_GETATTR, name)

    def __delattr__(self, name):
        if name in LOCAL_ATTRS:
            object.__delattr__(self, name)
        else:
            if self.___is_returned_exception___:
                raise AttributeError()
            return fwd_request(self, OP_DELATTR, name)

    def __setattr__(self, name, value):
        if (
            name in LOCAL_ATTRS
            or name in self.___local_overrides___
            or self.___is_returned_exception___
        ):
            object.__setattr__(self, name, value)
        else:
            if self.___is_returned_exception___:
                raise AttributeError()
            fwd_request(self, OP_SETATTR, name, value)

    def __dir__(self):
        return fwd_request(self, OP_DIR)

    def __hash__(self):
        return fwd_request(self, OP_HASH)

    def __repr__(self):
        if self.___is_returned_exception___:
            return self.__exception_repr__()
        return fwd_request(self, OP_REPR)

    def __str__(self):
        if self.___is_returned_exception___:
            return self.__exception_str__()
        return fwd_request(self, OP_STR)

    def __exit__(self, exc, typ, tb):
        raise NotImplementedError
        # FIXME
        # return fwd_request(self, OP_CTX_EXIT, exc)  # can't pass type nor traceback

    def __reduce_ex__(self, proto):
        # support for pickling
        return pickle.loads, (fwd_request(self, OP_PICKLE, proto),)

    @classmethod
    def __subclasshook__(cls, parent):
        if parent.__bases__[0] == Stub:
            raise NotImplementedError  # Follow the usual mechanism
        # If this is not a stub, we go over to the other side
        parent_name = "%s.%s" % (parent.__module__, parent.__name__)
        return cls.___class_connection___.stub_request(
            None, OP_SUBCLASSCHECK, cls.___class_remote_class_name___, parent_name, True
        )


def _make_method(method_type, connection, class_name, name, doc):
    if name == "__call__":

        def __call__(_self, *args, **kwargs):
            return fwd_request(_self, OP_CALL, *args, **kwargs)

        __call__.__doc__ = doc
        return __call__

    def method(_self, *args, **kwargs):
        return fwd_request(_self, OP_CALLATTR, name, *args, **kwargs)

    def static_method(connection, class_name, name, *args, **kwargs):
        return connection.stub_request(
            None, OP_CALLONCLASS, class_name, name, True, *args, **kwargs
        )

    def class_method(connection, class_name, name, cls, *args, **kwargs):
        return connection.stub_request(
            None, OP_CALLONCLASS, class_name, name, False, *args, **kwargs
        )

    if method_type == NORMAL_METHOD:
        m = method
        m.__doc__ = doc
        m.__name__ = name
        return m
    if method_type == STATIC_METHOD:
        m = functools.partial(static_method, connection, class_name, name)
        m.__doc__ = doc
        m.__name__ = name
        m = staticmethod(m)
        return m
    if method_type == CLASS_METHOD:
        m = functools.partial(class_method, connection, class_name, name)
        m.__doc__ = doc
        m.__name__ = name
        m = classmethod(m)
        return m


class MetaWithConnection(StubMetaClass):
    # The use of this metaclass is so that we can support two modes when
    # instantiating a subclass of Stub. Suppose we have a class Foo which is a stub.
    # There are two ways Foo is initialized:
    #  - when it is returned from the remote side, in which case we do
    #    Foo(class_name, connection, identifier)
    #  - when it is created locally and needs to actually forward to __init__ on
    #    the remote side. In this case, we want the user to be able to do
    #    Foo(*args, **kwargs) (whatever the usual arguments to __init__ are)
    #
    # With this metaclass, we do just that. We introspect arguments and look to
    # see if the first one is the connection which would indicate that we are in
    # the first case. If that is the case, we just pass everything down to the
    # super __call__ and go our merry way. If this is not the case, we will
    # use the connection we saved when creating this metaclass and call
    # OP_INIT to create the object

    def __new__(cls, class_name, base_classes, class_dict, connection):
        return type.__new__(cls, class_name, base_classes, class_dict)

    def __init__(cls, class_name, base_classes, class_dict, connection):
        cls.___class_remote_class_name___ = class_name
        cls.___class_connection___ = connection
        super(MetaWithConnection, cls).__init__(class_name, base_classes, class_dict)

    def __call__(cls, *args, **kwargs):
        if len(args) > 0 and id(args[0]) == id(cls.___class_connection___):
            return super(MetaWithConnection, cls).__call__(*args, **kwargs)
        else:
            if hasattr(cls, "__overriden_init__"):
                return cls.__overriden_init__(
                    None,
                    functools.partial(
                        cls.___class_connection___.stub_request,
                        None,
                        OP_INIT,
                        cls.___class_remote_class_name___,
                    ),
                    *args,
                    **kwargs
                )
            else:
                return cls.___class_connection___.stub_request(
                    None, OP_INIT, cls.___class_remote_class_name___, *args, **kwargs
                )

    def __subclasscheck__(cls, subclass):
        subclass_name = "%s.%s" % (subclass.__module__, subclass.__name__)
        if subclass.__bases__[0] == Stub:
            subclass_name = subclass.___class_remote_class_name___
        return cls.___class_connection___.stub_request(
            None,
            OP_SUBCLASSCHECK,
            cls.___class_remote_class_name___,
            subclass_name,
        )

    def __instancecheck__(cls, instance):
        if type(instance) == cls:
            # Fast path if it's just an object of this class
            return True
        # Goes to __subclasscheck__ above
        return cls.__subclasscheck__(type(instance))


class MetaExceptionWithConnection(StubMetaClass, ExceptionMetaClass):
    def __new__(cls, class_name, base_classes, class_dict, connection):
        return type.__new__(cls, class_name, base_classes, class_dict)

    def __init__(cls, class_name, base_classes, class_dict, connection):
        cls.___class_remote_class_name___ = class_name
        cls.___class_connection___ = connection

        # We call the one on ExceptionMetaClass which does everything needed (StubMetaClass
        # does not do anything special for init)
        ExceptionMetaClass.__init__(cls, class_name, base_classes, class_dict)

        # Restore __str__ and __repr__ to the original ones because we need to determine
        # if we call them depending on whether or not the object is a returned exception
        # or not
        cls.__exception_str__ = cls.__str__
        cls.__exception_repr__ = cls.__repr__
        cls.__str__ = cls.__orig_str__
        cls.__repr__ = cls.__orig_repr__

    def __call__(cls, *args, **kwargs):
        # Very similar to the other case but we also need to be able to detect
        # local instantiation of an exception so that we can set the __is_returned_exception__
        if len(args) > 0 and id(args[0]) == id(cls.___class_connection___):
            return super(MetaExceptionWithConnection, cls).__call__(*args, **kwargs)
        elif kwargs and kwargs.get("_is_returned_exception", False):
            return super(MetaExceptionWithConnection, cls).__call__(
                None, None, None, _is_returned_exception=True
            )
        else:
            return cls.___class_connection___.stub_request(
                None, OP_INIT, cls.___class_remote_class_name___, *args, **kwargs
            )

    # The issue is that for a proxied object that is also an exception, we now have
    # two classes representing it, one that includes the Stub class and one that doesn't
    # Concretely:
    #  - test.MyException would return a class that derives from Stub
    #  - test.MySubException would return a class that derives from Stub and test.MyException
    #    but WITHOUT the Stub portion (see get_local_class).
    #  - we want issubclass(test.MySubException, test.MyException) to return True and
    #    the same with instance checks.
    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))

    def __subclasscheck__(cls, subclass):
        # __mro__[0] is this class itself
        # __mro__[1] is the stub so we start checking at 2
        return any(
            [
                subclass.__mro__[i] in cls.__mro__[2:]
                for i in range(2, len(subclass.__mro__))
            ]
        )


def create_class(
    connection,
    class_name,
    overriden_methods,
    getattr_overrides,
    setattr_overrides,
    class_methods,
    parents,
):
    class_dict = {
        "__slots__": [
            "___remote_class_name___",
            "___identifier___",
            "___connection___",
            "___is_returned_exception___",
        ]
    }
    for name, doc in class_methods.items():
        method_type = NORMAL_METHOD
        if name.startswith("___s___"):
            name = name[7:]
            method_type = STATIC_METHOD
        elif name.startswith("___c___"):
            name = name[7:]
            method_type = CLASS_METHOD
        if name in overriden_methods:
            if name == "__init__":
                class_dict["__overriden_init__"] = overriden_methods["__init__"]

            elif method_type == NORMAL_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda obj, *args, **kwargs: override(
                        obj, functools.partial(orig_method, obj), *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
            elif method_type == STATIC_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda *args, **kwargs: override(
                        orig_method, *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
            elif method_type == CLASS_METHOD:
                class_dict[name] = (
                    lambda override, orig_method: lambda cls, *args, **kwargs: override(
                        cls, functools.partial(orig_method, cls), *args, **kwargs
                    )
                )(
                    overriden_methods[name],
                    _make_method(method_type, connection, class_name, name, doc),
                )
        elif name not in LOCAL_ATTRS:
            class_dict[name] = _make_method(
                method_type, connection, class_name, name, doc
            )

    # Check for any getattr/setattr overrides
    special_attributes = set(getattr_overrides.keys())
    special_attributes.update(set(setattr_overrides.keys()))
    overriden_attrs = set()
    for attr in special_attributes:
        getter = getattr_overrides.get(attr)
        setter = setattr_overrides.get(attr)
        if getter is not None:
            getter = lambda x, name=attr, inner=getter: inner(
                x, name, lambda y=x, name=name: y.__getattr__(name)
            )
        if setter is not None:
            setter = lambda x, value, name=attr, inner=setter: inner(
                x,
                name,
                lambda val, y=x, name=name: fwd_request(y, OP_SETATTR, name, val),
                value,
            )
            overriden_attrs.add(attr)
        class_dict[attr] = property(getter, setter)
    if parents:
        # This means this is also an exception so we add a few more things to it
        # so that it
        # This is copied from ExceptionMetaClass in exception_transferer.py
        for n in ("_exception_str", "_exception_repr", "_exception_tb"):
            class_dict[n] = property(
                lambda self, n=n: getattr(self, "%s_val" % n, "<missing>"),
                lambda self, v, n=n: setattr(self, "%s_val" % n, v),
            )

        def _do_str(self):
            text = self._exception_str
            text += "\n\n===== Remote (on server) traceback =====\n"
            text += self._exception_tb
            text += "========================================\n"
            return text

        class_dict["__exception_str__"] = _do_str
        class_dict["__exception_repr__"] = lambda self: self._exception_repr
    else:
        # If we are based on an exception, we already have __weakref__ so we don't add
        # it but not the case if we are not.
        class_dict["__slots__"].append("__weakref__")

    class_module, class_name_only = class_name.rsplit(".", 1)
    class_dict["___local_overrides___"] = overriden_attrs
    class_dict["__module__"] = class_module
    if parents:
        to_return = MetaExceptionWithConnection(
            class_name, (Stub, *parents), class_dict, connection
        )
    else:
        to_return = MetaWithConnection(class_name, (Stub,), class_dict, connection)
    to_return.__name__ = class_name_only
    return to_return
