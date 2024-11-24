import os

from metaflow import Config, FlowSpec, config_expr, environment, project, step


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

    # Check the value of the artifacts in the end step
    end_task = run["end"].task
    if (
        end_task.config_val != 5
        or end_task.config_val_2 != "41"
        or end_task.config_from_env != "5"
        or end_task.config_from_env_2 != "41"
    ):
        raise RuntimeError("Config values are incorrect.")

    return None


@project(name=config_expr("cfg_default_value.a.project_name"))
class ConfigSimple(FlowSpec):

    cfg = Config("cfg", default="config_simple.json")
    cfg_default_value = Config(
        "cfg_default_value",
        default_value={"a": {"b": "41", "project_name": "config_project"}},
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
