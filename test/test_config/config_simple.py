import os

from metaflow import (
    Config,
    FlowSpec,
    Parameter,
    config_expr,
    current,
    environment,
    project,
    step,
)

default_config = {"a": {"b": "41", "project_name": "config_project"}}


def trigger_name_func(ctx):
    return [current.project_flow_name + "Trigger"]


@project(name=config_expr("cfg_default_value.a.project_name"))
class ConfigSimple(FlowSpec):

    trigger_param = Parameter(
        "trigger_param",
        default="",
        external_trigger=True,
        external_artifact=trigger_name_func,
    )
    cfg = Config("cfg", default="config_simple.json")
    cfg_default_value = Config(
        "cfg_default_value",
        default_value=default_config,
    )

    @environment(
        vars={
            "TSTVAL": config_expr("str(cfg.some.value)"),
            "TSTVAL2": cfg_default_value.a.b,
        }
    )
    @step
    def start(self):
        self.config_from_env = os.environ.get("TSTVAL")
        self.config_from_env_2 = os.environ.get("TSTVAL2")
        self.config_val = self.cfg.some.value
        self.config_val_2 = self.cfg_default_value.a.b
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigSimple()
