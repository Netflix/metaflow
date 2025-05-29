import json
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


def audit(run, parameters, configs, stdout_path):
    # We should only have one run here
    if len(run) != 1:
        raise RuntimeError("Expected only one run; got %d" % len(run))
    run = run[0]

    # Check successful run
    if not run.successful:
        raise RuntimeError("Run was not successful")

    if configs and configs.get("cfg_default_value"):
        config = configs["cfg_default_value"]
    else:
        config = default_config

    expected_token = parameters["trigger_param"]

    # Check that we have the proper project name
    if f"project:{config['a']['project_name']}" not in run.tags:
        raise RuntimeError("Project name is incorrect.")

    # Check the value of the artifacts in the end step
    end_task = run["end"].task
    assert end_task.data.trigger_param == expected_token
    if (
        end_task.data.config_val != 5
        or end_task.data.config_val_2 != config["a"]["b"]
        or end_task.data.config_from_env != "5"
        or end_task.data.config_from_env_2 != config["a"]["b"]
        or end_task.data.var1 != "1"
        or end_task.data.var2 != "2"
    ):
        raise RuntimeError("Config values are incorrect.")

    return None


def trigger_name_func(ctx):
    return [current.project_flow_name + "Trigger"]


# Use functions in config_expr
def return_name(cfg):
    return cfg.a.project_name


@project(name=config_expr("return_name(cfg_default_value)"))
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
    env_cfg = Config("env_cfg", default_value={"VAR1": "1", "VAR2": "2"})

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
        self.next(self.mid)

    # Use config_expr as a top level attribute
    @environment(vars=config_expr("env_cfg"))
    @step
    def mid(self):
        self.var1 = os.environ.get("VAR1")
        self.var2 = os.environ.get("VAR2")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigSimple()
