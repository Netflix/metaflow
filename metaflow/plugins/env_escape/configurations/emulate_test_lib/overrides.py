from metaflow.plugins.env_escape.override_decorators import (
    local_override,
    local_getattr_override,
    local_setattr_override,
    remote_override,
    remote_getattr_override,
    remote_setattr_override,
    local_exception_deserialize,
    remote_exception_serialize,
)


@local_override({"test_lib.TestClass1": "print_value"})
def local_print_value(stub, func):
    v = func()
    return v + 5


@remote_override({"test_lib.TestClass1": "print_value"})
def remote_print_value(obj, func):
    v = func()
    return v + 3


@local_getattr_override({"test_lib.TestClass1": "override_value"})
def local_get_value2(stub, name, func):
    r = func()
    return r + 5


@remote_getattr_override({"test_lib.TestClass1": "override_value"})
def remote_get_value2(obj, name):
    r = getattr(obj, name)
    return r + 3


@local_setattr_override({"test_lib.TestClass1": "override_value"})
def local_set_value2(stub, name, func, v):
    r = func(v + 5)
    return r


@remote_setattr_override({"test_lib.TestClass1": "override_value"})
def remote_set_value2(obj, name, v):
    r = setattr(obj, name, v + 3)
    return r


@local_override({"test_lib.TestClass1": "unsupported_method"})
def unsupported_method(stub, func, *args, **kwargs):
    return NotImplementedError("Just because")


@local_exception_deserialize("test_lib.SomeException")
def some_exception_deserialize(ex, json_obj):
    ex.user_value = json_obj


@remote_exception_serialize("test_lib.SomeException")
def some_exception_serialize(ex):
    return 42


@local_exception_deserialize("test_lib.ExceptionAndClass")
def exception_and_class_deserialize(ex, json_obj):
    ex.user_value = json_obj


@remote_exception_serialize("test_lib.ExceptionAndClass")
def exception_and_class_serialize(ex):
    return 43


@local_exception_deserialize("test_lib.ExceptionAndClassChild")
def exception_and_class_child_deserialize(ex, json_obj):
    ex.user_value = json_obj


@remote_exception_serialize("test_lib.ExceptionAndClassChild")
def exception_and_class_child_serialize(ex):
    return 44
