import os
import sys

from html.parser import HTMLParser

from metaflow import FlowSpec, step, conda


def run_test(through_escape=False):
    # NOTE: This will be the same for both escaped path and non-escaped path
    # if the library test_lib is installed. For the unescaped path, we pretend
    # we installed the library by modifying the path
    if not through_escape:
        # HACK to pretend that we installed test_lib
        sys.path.append(
            os.path.realpath(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "metaflow",
                    "plugins",
                    "env_escape",
                    "configurations",
                    "test_lib_impl",
                )
            )
        )
        print("Path is %s" % str(sys.path))

    import test_lib as test

    print("-- Test aliasing --")
    if through_escape:
        # This tests package aliasing
        from test_lib.alias import TestClass1

    o1 = test.TestClass1(10)
    print("-- Test normal method with overrides --")
    if through_escape:
        expected_value = 10 + 8
    else:
        expected_value = 10
    assert o1.print_value() == expected_value

    print("-- Test property (no override) --")
    assert o1.value == 10
    o1.value = 15
    assert o1.value == 15
    if through_escape:
        expected_value = 15 + 8
    else:
        expected_value = 15
    assert o1.print_value() == expected_value

    print("-- Test property (with override) --")
    if through_escape:
        expected_value = 123 + 8
        expected_value2 = 200 + 16
    else:
        expected_value = 123
        expected_value2 = 200
    assert o1.override_value == expected_value
    o1.override_value = 200
    assert o1.override_value == expected_value2

    print("-- Test static method --")
    assert test.TestClass1.static_method(5) == 47
    assert o1.static_method(5) == 47

    print("-- Test class method --")
    assert test.TestClass1.class_method() == 25
    assert o1.class_method() == 25

    print("-- Test function --")
    assert test.test_func() == "In test func"

    print("-- Test value --")
    assert test.test_value == 1
    test.test_value = 2
    assert test.test_value == 2

    print("-- Test chaining of exported classes --")
    o2 = o1.to_class2(5)
    assert o2.something("foo") == "Test2:Something:foo"
    assert o2.__class__.__name__ == "TestClass2"
    assert o2.__class__.__module__ == "test_lib"

    print("-- Test Iterating --")
    for idx, i in enumerate(o2):
        assert idx == i - 15
    assert i == 19

    print("-- Test weird indirection --")
    o1.weird_indirection("foo")(10)
    assert o1.foo == 10
    o1.weird_indirection("_value")(20)
    assert o1.value == 20

    print("-- Test subclasses --")
    child_obj = test.ChildClass()
    child_obj_returned = o1.returnChild()
    for o in (child_obj, child_obj_returned):
        o.feed("<html>Hello<p>World!</p></html>")
        assert o.get_output() == ["html", "p", "p", "html"]

    print("-- Test isinstance/issubclass --")
    ex_child = test.ExceptionAndClassChild("I am a child")
    assert isinstance(ex_child, test.ExceptionAndClassChild)
    assert isinstance(ex_child, test.ExceptionAndClass)
    assert isinstance(ex_child, Exception)
    assert isinstance(ex_child, object)
    assert ex_child.__class__.__name__ == "ExceptionAndClassChild"
    assert ex_child.__class__.__module__ == "test_lib"

    assert issubclass(type(ex_child), test.ExceptionAndClass)
    assert issubclass(test.ExceptionAndClassChild, test.ExceptionAndClass)
    assert issubclass(type(ex_child), Exception)
    assert issubclass(test.ExceptionAndClassChild, Exception)
    assert issubclass(type(ex_child), object)
    assert issubclass(test.ExceptionAndClassChild, object)

    child_obj = test.ChildClass()
    child_obj_returned = o1.returnChild()

    # I can't find an easy way (yet) to test support for subclasses based on non
    # proxied types. It seems more minor for now so ignoring.
    for o in (child_obj, child_obj_returned):
        assert isinstance(o, test.ChildClass)
        assert isinstance(o, test.BaseClass)
        # assert isinstance(o, HTMLParser)
        assert isinstance(o, object)
        assert issubclass(type(o), test.BaseClass)
        # assert issubclass(type(o), HTMLParser)
        assert issubclass(type(o), object)
    assert issubclass(test.ChildClass, test.BaseClass)
    # assert issubclass(test.ChildClass, HTMLParser)
    assert issubclass(test.ChildClass, object)

    print("-- Test exceptions --")
    # Non proxied exceptions can't be returned as objects
    try:
        vexc = o1.raiseOrReturnValueError()
        assert not through_escape, "Should have raised through escape"
        assert isinstance(vexc, ValueError)
    except RuntimeError as e:
        assert (
            through_escape
            and "Cannot proxy value of type <class 'ValueError'>" in str(e)
        )

    try:
        excclass = o1.raiseOrReturnSomeException()
        assert not through_escape, "Should have raised through escape"
        assert isinstance(excclass, test.SomeException)
        assert excclass.__class__.__name__ == "SomeException"
        assert excclass.__class__.__module__ == "test_lib"
    except RuntimeError as e:
        assert (
            through_escape
            and "Cannot proxy value of type <class 'test_lib.SomeException'>" in str(e)
        )

    exception_and_class = o1.raiseOrReturnExceptionAndClass()
    assert isinstance(exception_and_class, test.ExceptionAndClass)
    assert isinstance(exception_and_class, test.MyBaseException)
    assert isinstance(exception_and_class, Exception)
    assert exception_and_class.method_on_exception() == "method_on_exception"
    assert str(exception_and_class).startswith("ExceptionAndClass Str:")

    exception_and_class_child = o1.raiseOrReturnExceptionAndClassChild()
    assert isinstance(exception_and_class_child, test.ExceptionAndClassChild)
    assert isinstance(exception_and_class_child, test.ExceptionAndClass)
    assert isinstance(exception_and_class_child, test.MyBaseException)
    assert isinstance(exception_and_class_child, Exception)
    assert exception_and_class_child.method_on_exception() == "method_on_exception"
    assert (
        exception_and_class_child.method_on_child_exception()
        == "method_on_child_exception"
    )
    assert str(exception_and_class_child).startswith("ExceptionAndClassChild Str:")

    try:
        o1.raiseOrReturnValueError(True)
        assert False, "Should have raised"
    except ValueError as e:
        assert True
    except Exception as e:
        assert False, "Should have been ValueError"

    try:
        o1.raiseOrReturnSomeException(True)
        assert False, "Should have raised"
    except test.SomeException as e:
        assert True
        if through_escape:
            assert e.user_value == 42
            assert "Remote (on server) traceback" in str(e)
    except Exception as e:
        assert False, "Should have been SomeException"

    try:
        o1.raiseOrReturnExceptionAndClass(True)
        assert False, "Should have raised"
    except test.ExceptionAndClass as e:
        assert True
        if through_escape:
            assert e.user_value == 43
            assert "Remote (on server) traceback" in str(e)
    except Exception as e:
        assert False, "Should have been ExceptionAndClass"

    try:
        o1.raiseOrReturnExceptionAndClassChild(True)
        assert False, "Should have raised"
    except test.ExceptionAndClassChild as e:
        assert True
        if through_escape:
            assert e.user_value == 44
            assert "Remote (on server) traceback" in str(e)
    except Exception as e:
        assert False, "Should have been ExceptionAndClassChild"


class EscapeTest(FlowSpec):
    @conda(disabled=True)
    @step
    def start(self):
        print("Starting escape test flow with interpreter %s" % sys.executable)
        self.next(self.native_exec, self.escape_exec)

    @conda(disabled=True)
    @step
    def native_exec(self):
        print("Running test natively using %s" % sys.executable)
        run_test()
        self.next(self.join)

    @conda
    @step
    def escape_exec(self):
        print("Running test through environment escape using %s" % sys.executable)
        run_test(True)
        self.next(self.join)

    @conda(disabled=True)
    @step
    def join(self, inputs):
        print("All done")
        self.next(self.end)

    @conda(disabled=True)
    @step
    def end(self):
        pass


if __name__ == "__main__":
    EscapeTest()
