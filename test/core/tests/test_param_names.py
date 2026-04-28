"""Parameter names with mixed case / dashes still resolve correctly."""

from metaflow import FlowSpec, Parameter, step


class ParamNamesFlow(FlowSpec):
    snake_param = Parameter("snake_param", default="snake")
    Capital_Param = Parameter("CapitalParam", default="capital")

    @step
    def start(self):
        self.captured = {
            "snake_param": self.snake_param,
            "Capital_Param": self.Capital_Param,
        }
        self.next(self.end)

    @step
    def end(self):
        pass


def test_param_names(metaflow_runner, executor):
    result = metaflow_runner(ParamNamesFlow, executor=executor)
    assert result.successful, result.stderr
    captured = result.run()["start"].task.data.captured
    assert captured["snake_param"] == "snake"
    assert captured["Capital_Param"] == "capital"
