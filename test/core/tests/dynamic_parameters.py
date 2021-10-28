from metaflow_test import MetaflowTest, ExpectationFailed, steps


class DynamicParameterTest(MetaflowTest):
    PRIORITY = 3
    PARAMETERS = {
        "str_param": {"default": "str_func"},
        "json_param": {"default": "json_func", "type": "JSONType"},
        "nondefault_param": {"default": "lambda _: True", "type": "bool"},
    }
    HEADER = """
import os
os.environ['METAFLOW_RUN_NONDEFAULT_PARAM'] = 'False'

def str_func(ctx):
    import os
    from metaflow import current
    assert_equals(current.project_name, 'dynamic_parameters_project')
    assert_equals(ctx.parameter_name, 'str_param')
    assert_equals(ctx.flow_name, 'DynamicParameterTestFlow')
    assert_equals(ctx.user_name, os.environ['METAFLOW_USER'])

    if os.path.exists('str_func.only_once'):
        raise Exception("Dynamic parameter function invoked multiple times!")

    with open('str_func.only_once', 'w') as f:
        f.write('foo')

    return 'does this work?'

def json_func(ctx):
    import json
    return json.dumps({'a': [8]})

@project(name='dynamic_parameters_project')
"""

    @steps(0, ["singleton"], required=True)
    def step_single(self):
        assert_equals(self.str_param, "does this work?")
        assert_equals(self.nondefault_param, False)
        assert_equals(self.json_param, {"a": [8]})

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "nondefault_param", False)
            checker.assert_artifact(step.name, "str_param", "does this work?")
            checker.assert_artifact(step.name, "json_param", {"a": [8]})
