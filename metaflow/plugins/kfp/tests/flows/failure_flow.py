import os
import signal
import time

from metaflow import FlowSpec, step, retry, catch, timeout, current, Step
from metaflow.exception import MetaflowExceptionWrapper


class FailureFlow(FlowSpec):
    retry_log = "Retry count = {retry_count}"

    @retry
    @step
    def start(self):
        self.retry_count = current.retry_count
        print(self.retry_log.format(retry_count=current.retry_count))
        if current.retry_count < 1:
            raise Exception("Let's force one retries!")
        else:
            print("let's succeed")
        self.next(self.no_retry)

    @retry(times=0)
    @step
    def no_retry(self):
        print("Testing logging for retries")
        start_step: Step = Step(f"{current.flow_name}/{current.run_id}/start")
        expected_logs = self.retry_log.format(retry_count=1)
        logs = start_step.task.stdout
        print("\n=== logs for task {task} ===")
        print(logs)
        print("\n=== logs ended ===")
        assert expected_logs in logs

        argo_node_name = os.environ.get("MF_ARGO_NODE_NAME")
        assert not argo_node_name.endswith(")")
        assert self.retry_count == 1
        self.next(self.compute)

    @catch(var="compute_failed")
    @retry(times=0)
    @step
    def compute(self):
        self.x = 1 / 0
        self.next(self.platform_exception)

    @catch(var="platform_exception_failed")
    @retry(times=1)
    @step
    def platform_exception(self):
        assert type(self.compute_failed) == MetaflowExceptionWrapper
        assert "ZeroDivisionError" in str(self.compute_failed)

        # kill this process with the KILL signal
        os.kill(os.getpid(), signal.SIGKILL)
        self.next(self.timeout)

    @catch(print_exception=False, var="timeout_exception")
    @timeout(seconds=3)
    @step
    def timeout(self):
        print(
            f"platform_exception_failed: ",
            type(self.platform_exception_failed),
            self.platform_exception_failed,
        )

        assert type(self.platform_exception_failed) == MetaflowExceptionWrapper
        expected_str = (
            "Task execution kept failing over 2 attempts. "
            "Your code did not raise an exception. "
            "Something in the execution environment caused the failure."
        )
        assert expected_str in str(self.platform_exception_failed)
        for iteration in range(100):
            print("iteration", iteration)
            self.iteration = iteration
            time.sleep(1)
        self.next(self.end)

    @step
    def end(self):
        assert (
            2 <= self.iteration <= 5
        )  # 0 1 2 iterations is 3 seconds, with a few second leeway
        print("timeout_exception", type(self.timeout_exception), self.timeout_exception)
        assert type(self.timeout_exception) == MetaflowExceptionWrapper
        assert "Step timeout timed out after 0 hours, 0 minutes, 3 seconds" in str(
            self.timeout_exception
        )
        print("All done.")


if __name__ == "__main__":
    FailureFlow()
