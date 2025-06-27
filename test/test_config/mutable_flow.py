import json
import os

from metaflow import (
    Config,
    FlowMutator,
    StepMutator,
    FlowSpec,
    Parameter,
    config_expr,
    current,
    environment,
    project,
    step,
)

from metaflow.decorators import extract_step_decorator_from_decospec

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


class ModifyFlow(FlowMutator):
    def mutate(self, mutable_flow):
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

        to_add = mutable_flow.config["flow_add_environment"]["vars"]
        for name, s in mutable_flow.steps:
            if name == "start":
                decos = [deco for deco in s.decorator_specs]
                assert len(decos) == 3, "Unexpected number of decorators"
                assert decos[0].startswith("environment:"), "Unexpected decorator"
                env_deco, _ = extract_step_decorator_from_decospec(decos[0], {})
                attrs = env_deco.attributes
                for k, v in to_add.items():
                    attrs["vars"][k] = v
                s.remove_decorator(decos[0])
                s.add_decorator(environment, **attrs)
            else:
                s.add_decorator(
                    environment, **mutable_flow.config["flow_add_environment"].to_dict()
                )


class ModifyFlowWithArgs(FlowMutator):
    def init(self):
        super().init()
        self._field_to_check = self.args[0]

    def pre_mutate(self, mutable_flow):
        parameters = mutable_flow.config.get(self._field_to_check, [])
        for param in parameters:
            mutable_flow.add_parameter(
                param["name"],
                Parameter(
                    param["name"],
                    type=str,
                    default=param["default"],
                ),
                overwrite=True,
            )


class ModifyStep(StepMutator):
    def mutate(self, mutable_step):
        for deco in mutable_step.decorator_specs:
            if deco.startswith("environment:"):
                mutable_step.remove_decorator(deco)

        for deco in mutable_step.decorator_specs:
            assert not deco.startswith("environment:"), "Unexpected decorator"

        mutable_step.add_decorator(
            environment, **mutable_step.flow.config["step_add_environment"].to_dict()
        )


class ModifyStep2(StepMutator):
    def mutate(self, mutable_step):
        to_add = mutable_step.flow.config["step_add_environment_2"]["vars"]
        for deco in mutable_step.decorator_specs:
            if deco.startswith("environment:"):
                env_deco, _ = extract_step_decorator_from_decospec(deco, {})
                attrs = env_deco.attributes
                for k, v in to_add.items():
                    attrs["vars"][k] = v
                mutable_step.remove_decorator(deco)
                mutable_step.add_decorator(environment, **attrs)


@ModifyFlow
@ModifyFlowWithArgs("parameters")
@project(name=config_expr("config.project_name"))
class ConfigMutableFlow(FlowSpec):
    trigger_param = Parameter(
        "trigger_param",
        default="",
    )
    config = Config("config", default_value=default_config)

    def _check(self, step_decorators):
        for p in self.config.parameters:
            assert hasattr(self, p["name"]), "Missing parameter"

        assert (
            os.environ.get("SHOULD_NOT_EXIST") is None
        ), "Unexpected environment variable"

        if not step_decorators:
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
