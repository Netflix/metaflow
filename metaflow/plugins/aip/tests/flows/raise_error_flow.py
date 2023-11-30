from metaflow import FlowSpec, step, exit_handler
from metaflow.plugins.aip import exit_handler_retry


@exit_handler_retry(times=1, minutes_between_retries=0)
def my_exit_handler(
    status: str,
    flow_parameters: dict,
    argo_workflow_run_name: str,
    metaflow_run_id: str,
    argo_ui_url: str,
    retries: int,
) -> None:
    if status == "Succeeded":
        print("Congratulations! The flow succeeded.")
    else:
        print("Oh no! The flow failed.")

    if retries == 0:
        raise Exception("oopsie")


@exit_handler(func=my_exit_handler)
class RaiseErrorFlow(FlowSpec):
    """
    This flow is intended to "test" the Metaflow integration testing framework.
    In the past, the Metaflow integration test has incorrectly reported failing
    fails are passing within the Gitlab (and this Github UI). This flow is supposed
    to fail, and we ensure the integration test detects that.
    """

    @step
    def start(self):
        print("This step should complete successfuly!")
        self.next(self.error_step)

    @step
    def error_step(self):
        raise Exception("This exception is intended to test the integration test!")
        self.next(self.end)

    @step
    def end(self):
        print("This step should not be reachable!")


if __name__ == "__main__":
    RaiseErrorFlow()
