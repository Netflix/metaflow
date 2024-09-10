from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class BasicConfigTest(MetaflowTest):
    PRIORITY = 1
    PARAMETERS = {
        "default_from_config": {
            "default": "config_expr('config2').default_param",
            "type": "int",
        },
        "default_from_func": {"default": "param_default", "type": "int"},
    }
    CONFIGS = {
        "config": {"default": "default_config"},
        "silly_config": {"required": True, "parser": "silly_parser"},
        "config2": {},
        "config3": {"default": "config_default"},
    }
    HEADER = """
import json
import os

os.environ['METAFLOW_FLOW_CONFIG'] = json.dumps(
    {
        "config2": {"default_param": 123},
        "silly_config": "baz:amazing"
    }
)

def silly_parser(s):
    k, v = s.split(":")
    return {k: v}

default_config = {
    "value": 42,
    "str_value": "foobar",
    "project_name": "test_config",
    "nested": {"value": 43},
}

def param_default(ctx):
    return ctx.configs.config2.default_param + 1

def config_default(ctx):
    return {"val": 456}

# Test flow-level decorator configurations
@project(name=config_expr("config").project_name)
"""

    # Test step level decorators with configs
    @tag(
        "environment(vars={'normal': config.str_value, 'stringify': config_expr('str(config.value)')})"
    )
    @steps(0, ["all"])
    def step_all(self):
        # Test flow-level decorator configs
        assert_equals(current.project_name, "test_config")

        # Test step-level decorator configs
        assert_equals(os.environ["normal"], "foobar")
        assert_equals(os.environ["stringify"], "42")

        # Test parameters reading configs
        assert_equals(self.default_from_config, 123)
        assert_equals(self.default_from_func, 124)

        # Test configs are accessible as artifacts
        assert_equals(self.config.value, 42)
        assert_equals(self.config["value"], 42)
        assert_equals(self.config.nested.value, 43)
        assert_equals(self.config["nested"]["value"], 43)
        assert_equals(self.config.nested["value"], 43)
        assert_equals(self.config["nested"].value, 43)

        assert_equals(self.silly_config.baz, "amazing")
        assert_equals(self.silly_config["baz"], "amazing")

        assert_equals(self.config3.val, 456)

        try:
            self.config3["val"] = 5
            raise ExpectationFailed(TypeError, "configs should be immutable")
        except TypeError:
            pass

        try:
            self.config3.val = 5
            raise ExpectationFailed(TypeError, "configs should be immutable")
        except TypeError:
            pass

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(
                step.name,
                "config",
                {
                    "value": 42,
                    "str_value": "foobar",
                    "project_name": "test_config",
                    "nested": {"value": 43},
                },
            )
            checker.assert_artifact(step.name, "config2", {"default_param": 123})
            checker.assert_artifact(step.name, "silly_config", {"baz": "amazing"})
