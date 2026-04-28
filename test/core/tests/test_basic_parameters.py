"""Parameters: defaults, env overrides, types, immutability."""

import json

from metaflow import FlowSpec, Parameter, JSONType, step


class BasicParametersFlow(FlowSpec):
    int_param = Parameter("int_param", default=123, type=int)
    bool_default_true = Parameter("bool_default_true", default=True, type=bool)
    # bool override exercised via METAFLOW_RUN_BOOL_PARAM=False (note name).
    bool_param = Parameter("bool_param", default=True, type=bool)
    str_param = Parameter("str_param", default="default_str")
    no_default_param = Parameter("no_default_param", default="defaulted")
    list_param = Parameter("list_param", default="x,y,z", type=str)
    json_param = Parameter("json_param", default='{"k": 1}', type=JSONType)

    @step
    def start(self):
        self.captured_start = {
            "int_param": self.int_param,
            "bool_param": self.bool_param,
            "bool_default_true": self.bool_default_true,
            "str_param": self.str_param,
            "no_default_param": self.no_default_param,
            "list_param_split": self.list_param.split(","),
            "json_param": self.json_param,
        }
        self.next(self.end)

    @step
    def end(self):
        # Parameters should be immutable: writing to them raises.
        try:
            self.int_param = 999
        except (AttributeError, TypeError):
            self.assignment_failed = True
        else:
            self.assignment_failed = False
        self.captured_end = {"int_param": self.int_param}


def test_basic_parameters_defaults(metaflow_runner, executor):
    """Without overrides, defaults should land on every task."""
    result = metaflow_runner(BasicParametersFlow, executor=executor)
    assert result.successful, result.stderr
    captured = result.run()["start"].task.data.captured_start
    assert captured["int_param"] == 123
    assert captured["bool_default_true"] is True
    assert captured["str_param"] == "default_str"
    assert captured["list_param_split"] == ["x", "y", "z"]
    assert captured["json_param"] == {"k": 1}


def test_basic_parameters_env_override(metaflow_runner, executor, monkeypatch):
    """METAFLOW_RUN_<PARAM> env vars override defaults at run time."""
    monkeypatch.setenv("METAFLOW_RUN_BOOL_PARAM", "False")
    monkeypatch.setenv("METAFLOW_RUN_NO_DEFAULT_PARAM", "from_env")
    result = metaflow_runner(
        BasicParametersFlow,
        executor=executor,
        extra_run_options=["--int_param=99"],
    )
    assert result.successful, result.stderr
    captured = result.run()["start"].task.data.captured_start
    assert captured["int_param"] == 99
    assert captured["bool_param"] is False
    assert captured["no_default_param"] == "from_env"


def test_basic_parameters_immutable(metaflow_runner):
    """Parameters cannot be reassigned mid-flow."""
    result = metaflow_runner(BasicParametersFlow, executor="cli")
    assert result.successful, result.stderr
    end_data = result.run()["end"].task.data
    assert end_data.assignment_failed is True
    # The reassignment did not stick.
    assert end_data.captured_end["int_param"] == 123
