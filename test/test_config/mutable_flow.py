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
    def init(self, *args, **kwargs):
        self._field_to_check = args[0]

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
