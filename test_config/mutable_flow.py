import os

from metaflow import (
    Config,
    CustomFlowDecorator,
    CustomStepDecorator,
    FlowSpec,
    Parameter,
    config_expr,
    environment,
    project,
    step,
)


def audit(run, parameters, stdout_path):
    # We should only have one run here
    if len(run) != 1:
        raise RuntimeError("Expected only one run; got %d" % len(run))
    run = run[0]

    # Check successful run
    if not run.successful:
        raise RuntimeError("Run was not successful")
    # Check that we have the proper project name
    if "project:config_project" not in run.tags:
        raise RuntimeError("Project name is incorrect.")

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
                Parameter(param["name"], type=str, default=param["default"]),
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


@ModifyFlow
@ModifyFlowWithArgs("parameters")
@project(name=config_expr("config.project_name"))
class ConfigMutableFlow(FlowSpec):

    config = Config(
        "config",
        default_value={
            "parameters": [
                {"name": "param1", "default": "41"},
                {"name": "param2", "default": "42"},
            ],
            "step_add_environment": {"vars": {"STEP_LEVEL": "2"}},
            "step_add_environment_2": {"vars": {"STEP_LEVEL_2": "3"}},
            "flow_add_environment": {"vars": {"FLOW_LEVEL": "4"}},
            "project_name": "config_project",
        },
    )

    def _check(self, step_decorators):
        assert self.param1 == "41", "param1 does not match expected value"
        assert self.param2 == "42", "param2 does not match expected value"
        assert (
            os.environ.get("SHOULD_NOT_EXIST") is None
        ), "Unexpected environment variable"
        assert (
            os.environ.get("FLOW_LEVEL") == "4"
        ), "Flow level environment variable not set"
        if step_decorators:
            assert os.environ.get("STEP_LEVEL") == "2", "Missing step_level decorator"
            assert (
                os.environ.get("STEP_LEVEL_2") == "3"
            ), "Missing step_level_2 decorator"
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
