import os

from metaflow import (
    Config,
    FlowMutator,
    StepMutator,
    FlowSpec,
    Parameter,
    config_expr,
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
                deco_info = [deco for deco in s.decorator_specs]
                env_deco = None
                for deco in deco_info:
                    if deco[0] == "environment":
                        env_deco = deco
                        break
                assert env_deco is not None, "Missing environment decorator"
                deco_attrs = env_deco[3]
                for k, v in to_add.items():
                    deco_attrs["vars"][k] = v
                s.add_decorator(
                    environment, deco_kwargs=deco_attrs, duplicates=s.OVERRIDE
                )
            else:
                s.add_decorator(
                    environment,
                    deco_kwargs=mutable_flow.config["flow_add_environment"].to_dict(),
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
    def pre_mutate(self, mutable_step):
        mutable_step.remove_decorator("environment")

        for deco_name, _, _, _ in mutable_step.decorator_specs:
            assert deco_name != "environment", "Unexpected decorator"

        mutable_step.add_decorator(
            environment,
            deco_kwargs=mutable_step.flow.config["step_add_environment"].to_dict(),
        )


class ModifyStep2(StepMutator):
    def pre_mutate(self, mutable_step):
        to_add = mutable_step.flow.config["step_add_environment_2"]["vars"]
        for deco_name, _, _, deco_attrs in mutable_step.decorator_specs:
            if deco_name == "environment":
                for k, v in to_add.items():
                    deco_attrs["vars"][k] = v
                mutable_step.add_decorator(
                    environment,
                    deco_kwargs=deco_attrs,
                    duplicates=mutable_step.OVERRIDE,
                )
                break


@ModifyFlow
@ModifyFlowWithArgs("parameters")
@project(name=config_expr("config.project_name"))
class ConfigMutableFlow(FlowSpec):

    trigger_param = Parameter("trigger_param", default="")
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
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("Starting start step...")
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
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
