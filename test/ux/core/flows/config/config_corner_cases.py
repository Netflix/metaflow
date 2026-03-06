import os

from metaflow import (
    Config,
    FlowSpec,
    Parameter,
    config_expr,
    environment,
    project,
    step,
)

default_config = {"a": {"b": "41", "project_name": "config_project"}}

_config_dir = os.path.dirname(__file__)


def return_name(cfg):
    return cfg.a.project_name


@project(name=config_expr("return_name(cfg_default_value)"))
class ConfigSimple(FlowSpec):

    trigger_param = Parameter("trigger_param", default="")
    cfg = Config("cfg", default=os.path.join(_config_dir, "config_simple.json"))
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
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        self.config_from_env = os.environ.get("TSTVAL")
        self.config_from_env_2 = os.environ.get("TSTVAL2")
        self.config_val = self.cfg.some.value
        self.config_val_2 = self.cfg_default_value.a.b
        self.next(self.mid)

    # Use config_expr as a top-level attribute
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
