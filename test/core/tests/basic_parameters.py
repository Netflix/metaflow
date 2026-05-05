from metaflow_test import FlowDefinition, steps


class BasicParameter(FlowDefinition):
    PRIORITY = 1
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]
    PARAMETERS = {
        "no_default_param": {"default": None},
        # Note this value is overridden by METAFLOW_RUN_BOOL_PARAM in the tox backend env
        "bool_param": {"default": False},
        "bool_true_param": {"default": True},
        "int_param": {"default": 123},
        "str_param": {"default": "'foobar'"},
        "list_param": {"separator": "','", "default": '"a,b,c"'},
        "json_param": {"default": """'{"a": [1,2,3]}'""", "type": "JSONType"},
    }
    HEADER = """
import os
os.environ['METAFLOW_RUN_NO_DEFAULT_PARAM'] = 'test_str'
os.environ['METAFLOW_RUN_BOOL_PARAM'] = 'False'
"""

    @steps(0, ["all"])
    def step_all(self):
        assert "test_str" == self.no_default_param
        assert False == self.bool_param
        assert True == self.bool_true_param
        assert 123 == self.int_param
        assert "foobar" == self.str_param
        assert ["a", "b", "c"] == self.list_param
        assert {"a": [1, 2, 3]} == self.json_param
        try:
            # parameters should be immutable
            self.int_param = 5
            raise AssertionError("expected AttributeError but none was raised")
        except AttributeError:
            pass

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "no_default_param", "test_str")
            checker.assert_artifact(step.name, "bool_param", False)
            checker.assert_artifact(step.name, "bool_true_param", True)
            checker.assert_artifact(step.name, "int_param", 123)
            checker.assert_artifact(step.name, "str_param", "foobar")
            checker.assert_artifact(step.name, "list_param", ["a", "b", "c"])
            checker.assert_artifact(step.name, "json_param", {"a": [1, 2, 3]})
