import os
import signal
import subprocess
import time

from metaflow import FlowSpec, Parameter, Step, catch, current, retry, step, timeout
from metaflow.exception import MetaflowExceptionWrapper


class ResilientFlow(FlowSpec):
    retry_log = "Retry count = {retry_count}"

    @retry
    @step
    def start(self):
        self.start_retry_count = current.retry_count
        print(self.retry_log.format(retry_count=current.retry_count))
        self.download_kubectl()
        if current.retry_count < 1:
            # delete and terminate myself!!
            command = (
                f"./kubectl delete pod {os.environ.get('POD_NAME')} "
                f"--namespace {os.environ.get('POD_NAMESPACE')}"
            )

            print(f"{command=}")
            output = subprocess.check_output(command, shell=True)
            print(str(output))

            # sleep to allow time for k8s to delete the pod
            # Although k8s has always deleted the pod in time,
            # this gives the test extra resilience.
            time.sleep(60 * 5)
        else:
            print("let's succeed")

        self.next(self.user_failure)

    def download_kubectl(self):
        output = subprocess.check_output(
            "curl -LO https://dl.k8s.io/release/$(curl -L -s "
            "https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl",
            shell=True,
        )
        print(str(output))
        subprocess.check_output("chmod u+x ./kubectl", shell=True)

    @retry
    @step
    def user_failure(self):
        if self.start_retry_count < 1:
            raise Exception("start did not retry!")

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
        user_failure_step: Step = Step(
            f"{current.flow_name}/{current.run_id}/user_failure"
        )
        task = user_failure_step.task
        expected_logs = self.retry_log.format(retry_count=task.current_attempt)
        print(f"\n=== logs for attempt {task.current_attempt} ===")
        print(task.stdout)
        print("\n=== logs ended ===")
        assert expected_logs in task.stdout

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
    ResilientFlow()
