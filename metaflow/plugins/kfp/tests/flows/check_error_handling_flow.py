from metaflow import Parameter, FlowSpec, step, Step


class CheckErrorHandlingFlow(FlowSpec):
    """This flow aims to test metadata of raise_error_flow.py"""

    error_flow_id = Parameter("error-flow-id")

    @step
    def start(self):
        err_step: Step = Step(f"RaiseErrorFlow/{self.error_flow_id}/error_step")
        expected = "This exception is intended to test the integration test!"

        logs = err_step.task.stderr
        assert expected in logs

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CheckErrorHandlingFlow()
