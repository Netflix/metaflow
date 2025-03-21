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
        config = json.loads(configs["cfg_default_value"])
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
    ):
        raise RuntimeError("Config values are incorrect.")

    return None


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
