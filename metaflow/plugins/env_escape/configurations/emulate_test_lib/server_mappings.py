import functools
import os
import sys

# HACK to pretend that we installed test_lib
sys.path.append(
    os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "test_lib_impl"))
)

import test_lib as lib

EXPORTED_CLASSES = {
    ("test_lib", "test_lib.alias"): {
        "TestClass1": lib.TestClass1,
        "TestClass2": lib.TestClass2,
        "BaseClass": lib.BaseClass,
        "ChildClass": lib.ChildClass,
        "ExceptionAndClass": lib.ExceptionAndClass,
        "ExceptionAndClassChild": lib.ExceptionAndClassChild,
    }
}

EXPORTED_EXCEPTIONS = {
    ("test_lib", "test_lib.alias"): {
        "SomeException": lib.SomeException,
        "MyBaseException": lib.MyBaseException,
        "ExceptionAndClass": lib.ExceptionAndClass,
        "ExceptionAndClassChild": lib.ExceptionAndClassChild,
    }
}

PROXIED_CLASSES = [functools.partial]

EXPORTED_FUNCTIONS = {"test_lib": {"test_func": lib.test_func}}

EXPORTED_VALUES = {"test_lib": {"test_value": lib.test_value}}
