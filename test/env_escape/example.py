import os
import sys

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

    if through_escape:
        # This tests package aliasing
        from test_lib.package import TestClass3
    else:
        from test_lib import TestClass3

    o1 = test.TestClass1(123)
    print("-- Test print_value --")
    if through_escape:
        # The server_mapping should add 5 here
        assert o1.print_value() == 128
    else:
        assert o1.print_value() == 123
    print("-- Test property --")
    assert o1.value == 123
    print("-- Test value override (get) --")
    assert o1.override_value == 123
    print("-- Test value override (set) --")
    o1.override_value = 456
    assert o1.override_value == 456

    print("-- Test static method --")
    assert test.TestClass1.somethingstatic(5) == 47
    assert o1.somethingstatic(5) == 47
    print("-- Test class method --")
    assert test.TestClass1.somethingclass() == 25
    assert o1.somethingclass() == 25

    print("-- Test set and get --")
    o1.value = 2
    if through_escape:
        # The server_mapping should add 5 here
        assert o1.print_value() == 7
    else:
        assert o1.print_value() == 2

    print("-- Test function --")
    assert test.test_func() == "In test func"

    print("-- Test value --")
    assert test.test_value == 1
    test.test_value = 2
    assert test.test_value == 2

    print("-- Test chaining of exported classes --")
    o2 = o1.to_class2(5)
    assert o2.something("foo") == "In Test2 with foo"
    print("-- Test Iterating --")
    for i in o2:
        print("Got %d" % i)

    print("-- Test exception --")
    o3 = TestClass3()
    try:
        o3.raiseSomething()
    except test.SomeException as e:
        print("Caught the local exception: %s" % str(e))

    print("-- Test returning proxied object --")
    o3.weird_indirection("foo")(10)
    assert o3.foo == 10


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
