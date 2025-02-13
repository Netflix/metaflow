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
    pypi_base,
    req_parser,
    step,
)

default_config = {"project_name": "config_parser"}


def audit(run, parameters, configs, stdout_path):
    # We should only have one run here
    if len(run) != 1:
        raise RuntimeError("Expected only one run; got %d" % len(run))
    run = run[0]

    # Check successful run
    if not run.successful:
        raise RuntimeError("Run was not successful")

    if len(parameters) > 1:
        expected_tokens = parameters[-1].split()
        if len(expected_tokens) < 8:
            raise RuntimeError("Unexpected parameter list: %s" % str(expected_tokens))
        expected_token = expected_tokens[7]
    else:
        expected_token = ""

    # Check that we have the proper project name
    if f"project:{default_config['project_name']}" not in run.tags:
        raise RuntimeError("Project name is incorrect.")

    # Check the value of the artifacts in the end step
    end_task = run["end"].task
    assert end_task.data.trigger_param == expected_token

    if end_task.data.lib_version != "2.5.148":
        raise RuntimeError("Library version is incorrect.")

    # Check we properly parsed the requirements file
    if len(end_task.data.req_config) != 2:
        raise RuntimeError(
            "Requirements file is incorrect -- expected 2 keys, saw %s"
            % str(end_task.data.req_config)
        )
    if end_task.data.req_config["python"] != "3.10.*":
        raise RuntimeError(
            "Requirements file is incorrect -- got python version %s"
            % end_task.data.req_config["python"]
        )

    if end_task.data.req_config["packages"] != {"regex": "2024.11.6"}:
        raise RuntimeError(
            "Requirements file is incorrect -- got packages %s"
            % end_task.data.req_config["packages"]
        )

    return None


def trigger_name_func(ctx):
    return [current.project_flow_name + "Trigger"]


@project(name=config_expr("cfg.project_name"))
@pypi_base(**config_expr("req_config"))
class ConfigParser(FlowSpec):

    trigger_param = Parameter(
        "trigger_param",
        default="",
        external_trigger=True,
        external_artifact=trigger_name_func,
    )
    cfg = Config("cfg", default_value=default_config)

    req_config = Config(
        "req_config", default="config_parser_requirements.txt", parser=req_parser
    )

    @step
    def start(self):
        import regex

        self.lib_version = regex.__version__  # Should be '2.5.148'
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigParser()
