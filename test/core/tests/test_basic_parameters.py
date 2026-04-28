"""Parameters (with defaults / from env) propagate to step bodies."""

import os

from metaflow import FlowSpec, Parameter, step


class BasicParametersFlow(FlowSpec):
    int_param = Parameter("int_param", default=123, type=int)
    bool_param = Parameter("bool_param", default=True, type=bool)
    str_param = Parameter("str_param", default="default_str")

    @step
    def start(self):
        self.captured = {
            "int_param": self.int_param,
            "bool_param": self.bool_param,
            "str_param": self.str_param,
        }
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_parameters_defaults(metaflow_runner, executor):
    """Without overrides, defaults should land on every task."""
    result = metaflow_runner(BasicParametersFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    captured = run["start"].task.data.captured
    assert captured["int_param"] == 123
    assert captured["bool_param"] is True
    assert captured["str_param"] == "default_str"


def test_basic_parameters_metaflow_run_env(metaflow_runner, executor, monkeypatch):
    """METAFLOW_RUN_<param> env vars override defaults at run time."""
    monkeypatch.setenv("METAFLOW_RUN_BOOL_PARAM", "False")
    monkeypatch.setenv("METAFLOW_RUN_NO_DEFAULT_PARAM", "test_str")
    result = metaflow_runner(
        BasicParametersFlow, executor=executor, extra_run_options=["--int_param=99"]
    )
    assert result.successful, result.stderr
    captured = result.run()["start"].task.data.captured
    assert captured["int_param"] == 99
    # bool override applied via env var.
    assert captured["bool_param"] is False
