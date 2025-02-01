from metaflow_test import MetaflowTest, steps


class RuntimeParameterTest(MetaflowTest):
    """
    Test that parameters can be overridden at runtime via the Runner API
    """

    PRIORITY = 1

    PARAMETERS = {
        "bool_param": {"default": False},
        "int_param": {"default": 123},
        "str_param": {"default": "'foobar'"},
        "list_param": {"separator": "','", "default": '"a,b,c"'},
        "json_param": {"default": """'{"a": [1,2,3]}'""", "type": "JSONType"},
    }

    # These all shadow the defaults, except executor_param + test_param.
    RUNTIME_PARAMETERS = {
        "bool_param": False,
        "int_param": 123,
        "str_param": "foobar",
        "list_param": "a,b,c",
        "json_param": {"a": [1, 2, 3]},
    }

    @steps(0, ["all"])
    def step_all(self):
        assert_equals(False, self.bool_param)
        assert_equals(123, self.int_param)
        assert_equals("foobar", self.str_param)
        assert_equals(["a", "b", "c"], self.list_param)
        assert_equals({"a": [1, 2, 3]}, self.json_param)

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "bool_param", False)
            checker.assert_artifact(step.name, "int_param", 123)
            checker.assert_artifact(step.name, "str_param", "foobar")
            checker.assert_artifact(step.name, "list_param", ["a", "b", "c"])
            checker.assert_artifact(step.name, "json_param", {"a": [1, 2, 3]})
