from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ConstantsTest(MetaflowTest):
    """
    Test that an artifact defined in the first step
    is available in all steps downstream.
    """

    PRIORITY = 0
    CLASS_VARS = {
        "str_const": '"this is a constant"',
        "int_const": 123,
        "obj_const": "[]",
    }

    PARAMETERS = {
        "int_param": {"default": 456},
        "str_param": {"default": "'foobar'"},
    }

    @steps(0, ["all"])
    def step_all(self):
        # make sure class attributes are available in all steps
        # through joins etc
        assert_equals("this is a constant", self.str_const)
        assert_equals(123, self.int_const)
        # obj_const is mutable. Not much that can be done about it
        assert_equals([], self.obj_const)

        assert_equals(456, self.int_param)
        assert_equals("foobar", self.str_param)

        # make sure class variables are not listed as parameters
        from metaflow import current

        assert_equals({"int_param", "str_param"}, set(current.parameter_names))

        try:
            self.int_param = 5
        except AttributeError:
            pass
        else:
            raise Exception("It shouldn't be possible to modify parameters")

        try:
            self.int_const = 122
        except AttributeError:
            pass
        else:
            raise Exception("It shouldn't be possible to modify constants")

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "int_param", 456)
            checker.assert_artifact(step.name, "int_const", 123)
