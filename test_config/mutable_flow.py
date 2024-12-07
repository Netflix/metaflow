import json
import os

from metaflow import (
    Config,
    CustomFlowDecorator,
    CustomStepDecorator,
    FlowSpec,
    Parameter,
    config_expr,
    current,
    environment,
    project,
    step,
)

default_config = {
    "parameters": [
        {"name": "param1", "default": "41"},
        {"name": "param2", "default": "42"},
    ],
    "step_add_environment": {"vars": {"STEP_LEVEL": "2"}},
    "step_add_environment_2": {"vars": {"STEP_LEVEL_2": "3"}},
    "flow_add_environment": {"vars": {"FLOW_LEVEL": "4"}},
    "project_name": "config_project",
}


def find_param_in_parameters(parameters, name):
    for param in parameters:
        splits = param.split(" ")
        try:
            idx = splits.index("--" + name)
            return splits[idx + 1]
        except ValueError:
            continue
    return None


def audit(run, parameters, configs, stdout_path):
    # We should only have one run here
    if len(run) != 1:
        raise RuntimeError("Expected only one run; got %d" % len(run))
    run = run[0]

    # Check successful run
    if not run.successful:
        raise RuntimeError("Run was not successful")

    if configs:
        # We should have one config called "config"
        if len(configs) != 1 or not configs.get("config"):
            raise RuntimeError("Expected one config called 'config'")
        config = json.loads(configs["config"])
    else:
        config = default_config

    if len(parameters) > 1:
        expected_tokens = parameters[-1].split()
        if len(expected_tokens) < 8:
            raise RuntimeError("Unexpected parameter list: %s" % str(expected_tokens))
        expected_token = expected_tokens[7]
    else:
        expected_token = ""

    # Check that we have the proper project name
    if f"project:{config['project_name']}" not in run.tags:
        raise RuntimeError("Project name is incorrect.")

    # Check the start step that all values are properly set. We don't need
    # to check end step as it would be a duplicate
    start_task_data = run["start"].task.data

    assert start_task_data.trigger_param == expected_token
    for param in config["parameters"]:
        value = find_param_in_parameters(parameters, param["name"]) or param["default"]
        if not hasattr(start_task_data, param["name"]):
            raise RuntimeError(f"Missing parameter {param['name']}")
        if getattr(start_task_data, param["name"]) != value:
            raise RuntimeError(
                f"Parameter {param['name']} has incorrect value %s versus %s expected"
                % (getattr(start_task_data, param["name"]), value)
            )
    assert (
        start_task_data.flow_level
        == config["flow_add_environment"]["vars"]["FLOW_LEVEL"]
    )
    assert (
        start_task_data.step_level
        == config["step_add_environment"]["vars"]["STEP_LEVEL"]
    )
    assert (
        start_task_data.step_level_2
        == config["step_add_environment_2"]["vars"]["STEP_LEVEL_2"]
    )

    return None


class ModifyFlow(CustomFlowDecorator):
    def evaluate(self, mutable_flow):
        steps = ["start", "end"]
        count = 0
        for name, s in mutable_flow.steps:
            assert name in steps, "Unexpected step name"
            steps.remove(name)
            count += 1
        assert count == 2, "Unexpected number of steps"

        count = 0
        parameters = []
        for name, c in mutable_flow.configs:
            assert name == "config", "Unexpected config name"
            parameters = c["parameters"]
            count += 1
        assert count == 1, "Unexpected number of configs"

        count = 0
        for name, p in mutable_flow.parameters:
            if name == "trigger_param":
                continue
            assert name == parameters[count]["name"], "Unexpected parameter name"
            count += 1

        # Do some actual modification, we are going to update an environment decorator.
        # Note that in this flow, we have an environment decorator which is then
        to_add = mutable_flow.config["flow_add_environment"]["vars"]
        for name, s in mutable_flow.steps:
            if name == "start":
                decos = [deco for deco in s.decorators]
                assert len(decos) == 1, "Unexpected number of decorators"
                assert decos[0].name == "environment", "Unexpected decorator"
                for k, v in to_add.items():
                    decos[0].attributes["vars"][k] = v
            else:
                s.add_decorator(
                    environment, **mutable_flow.config["flow_add_environment"].to_dict()
                )


class ModifyFlowWithArgs(CustomFlowDecorator):
    def init(self, *args, **kwargs):
        self._field_to_check = args[0]

    def evaluate(self, mutable_flow):
        parameters = mutable_flow.config.get(self._field_to_check, [])
        for param in parameters:
            mutable_flow.add_parameter(
                param["name"],
                Parameter(
                    param["name"],
                    type=str,
                    default=param["default"],
                    external_artifact=trigger_name_func,
                ),
                overwrite=True,
            )


class ModifyStep(CustomStepDecorator):
    def evaluate(self, mutable_step):
        mutable_step.remove_decorator("environment")

        for deco in mutable_step.decorators:
            assert deco.name != "environment", "Unexpected decorator"

        mutable_step.add_decorator(
            environment, **mutable_step.flow.config["step_add_environment"].to_dict()
        )


class ModifyStep2(CustomStepDecorator):
    def evaluate(self, mutable_step):
        to_add = mutable_step.flow.config["step_add_environment_2"]["vars"]
        for deco in mutable_step.decorators:
            if deco.name == "environment":
                for k, v in to_add.items():
                    deco.attributes["vars"][k] = v


def trigger_name_func(ctx):
    return [current.project_flow_name + "Trigger"]


@ModifyFlow
@ModifyFlowWithArgs("parameters")
@project(name=config_expr("config.project_name"))
class ConfigMutableFlow(FlowSpec):

    trigger_param = Parameter(
        "trigger_param",
        default="",
        external_trigger=True,
        external_artifact=trigger_name_func,
    )
    config = Config("config", default_value=default_config)

    def _check(self, step_decorators):
        for p in self.config.parameters:
            assert hasattr(self, p["name"]), "Missing parameter"

        assert (
            os.environ.get("SHOULD_NOT_EXIST") is None
        ), "Unexpected environment variable"

        assert (
            os.environ.get("FLOW_LEVEL")
            == self.config.flow_add_environment["vars"]["FLOW_LEVEL"]
        ), "Flow level environment variable not set"
        self.flow_level = os.environ.get("FLOW_LEVEL")

        if step_decorators:
            assert (
                os.environ.get("STEP_LEVEL")
                == self.config.step_add_environment.vars.STEP_LEVEL
            ), "Missing step_level decorator"
            assert (
                os.environ.get("STEP_LEVEL_2")
                == self.config["step_add_environment_2"]["vars"].STEP_LEVEL_2
            ), "Missing step_level_2 decorator"

            self.step_level = os.environ.get("STEP_LEVEL")
            self.step_level_2 = os.environ.get("STEP_LEVEL_2")
        else:
            assert (
                os.environ.get("STEP_LEVEL") is None
            ), "Step level environment variable set"
            assert (
                os.environ.get("STEP_LEVEL_2") is None
            ), "Step level 2 environment variable set"

    @ModifyStep2
    @ModifyStep
    @environment(vars={"SHOULD_NOT_EXIST": "1"})
    @step
    def start(self):
        print("Starting start step...")
        self._check(step_decorators=True)
        print("All checks are good.")
        self.next(self.end)

    @step
    def end(self):
        print("Starting end step...")
        self._check(step_decorators=False)
        print("All checks are good.")


if __name__ == "__main__":
    ConfigMutableFlow()
