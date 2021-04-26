import functools
import test_lib as lib

EXPORTED_CLASSES = {
    "test_lib": {
        "TestClass1": lib.TestClass1,
        "TestClass2": lib.TestClass2,
        "package.TestClass3": lib.TestClass3,
    }
}

EXPORTED_EXCEPTIONS = {
    "test_lib": {
        "SomeException": lib.SomeException,
        "MyBaseException": lib.MyBaseException,
    }
}

PROXIED_CLASSES = [functools.partial]

EXPORTED_FUNCTIONS = {"test_lib": {"test_func": lib.test_func}}

EXPORTED_VALUES = {"test_lib": {"test_value": lib.test_value}}
