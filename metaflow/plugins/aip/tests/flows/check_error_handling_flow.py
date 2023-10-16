from metaflow import FlowSpec, Parameter, Step, step


class CheckErrorHandlingFlow(FlowSpec):
    """This flow aims to test metadata of raise_error_flow.py"""

    error_flow_id = Parameter("error_flow_id")

    @step
    def start(self):
        err_step: Step = Step(f"RaiseErrorFlow/{self.error_flow_id}/error_step")
        expected_log = "This exception is intended to test the integration test!"

        actual_log = err_step.task.stderr
        print(f"====== Log from RaiseErrorFlow/{self.error_flow_id}/error_step ======")
        print(actual_log)
        print(f"====== Log Ends ======")

        assert expected_log in actual_log, f"expected_log: {expected_log}"
        print(f"Log from RaiseErrorFlow is as expected")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CheckErrorHandlingFlow()
