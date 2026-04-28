"""Parameters with default factories evaluated at run time."""

from metaflow import FlowSpec, Parameter, step


class DynamicParametersFlow(FlowSpec):
    seq_param = Parameter("seq_param", default=lambda ctx: "[0, 1, 2, 3, 4]")
    str_param = Parameter("str_param", default=lambda ctx: "dynamic")

    @step
    def start(self):
        self.captured = (self.seq_param, self.str_param)
        self.next(self.end)

    @step
    def end(self):
        pass


def test_dynamic_parameters(metaflow_runner, executor):
    result = metaflow_runner(DynamicParametersFlow, executor=executor)
    assert result.successful, result.stderr
    seq_param, str_param = result.run()["start"].task.data.captured
    assert str_param == "dynamic"
    # seq_param is type=str, so the list is stringified.
    assert seq_param == "[0, 1, 2, 3, 4]"
