import datetime
import subprocess
import time
import uuid
import base64
from typing import Callable

from metaflow import FlowSpec, Parameter, Step, current, step
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
from metaflow.plugins.aip import (
    run_id_to_url,
    run_argo_workflow,
    wait_for_argo_run_completion,
    delete_argo_workflow,
    to_metaflow_run_id,
)
from metaflow.plugins.aip import logger

TEST_TEMPLATE_NAME = "wfdsk-ftf-test"


def generate_base64_uuid():
    """
    Created by ChatGPT
    """
    unique_id = uuid.uuid4()
    unique_id_bytes = unique_id.bytes
    return (
        base64.urlsafe_b64encode(unique_id_bytes)
        .rstrip(b"=")
        .decode("utf-8")
        .replace("_", "-")
    )


def _retry_sleep(func: Callable, count=3, seconds=1, **kwargs):
    for attempt in range(1, count + 1):
        try:
            return func(**kwargs)
        except Exception as e:
            logger.error(f"Function {func.__name__} failed on attempt {attempt}")
            time.sleep(seconds)
            if attempt == count:
                raise e


class FlowTriggeringFlow(FlowSpec):
    # Avoid infinite self trigger
    trigger_enabled: bool = Parameter("trigger_enabled", default=True)
    triggered_by: str = Parameter(name="triggered_by", default=None)

    @step
    def start(self):
        """Upload a downstream pipeline to be triggered"""
        if self.triggered_by:
            logger.info(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:  # Upload pipeline
            self.template_name = (
                f"{TEST_TEMPLATE_NAME}-{generate_base64_uuid()}".lower()
            )
            logger.info(f"Creating workflow: {self.template_name}")
            subprocess.run(
                [
                    "python",
                    __file__,
                    "aip",
                    "create",
                    "--name",
                    self.template_name,
                    "--yaml-only",
                    "--pipeline-path",
                    "/tmp/ftf.yaml",
                    "--kind",
                    "WorkflowTemplate",
                    "--max-run-concurrency",
                    "0",
                ],
                check=True,
            )
            subprocess.run(["cat", "/tmp/ftf.yaml"])
            print(f"{KUBERNETES_NAMESPACE=}")
            subprocess.run(
                [
                    "argo",
                    "template",
                    "-n",
                    KUBERNETES_NAMESPACE,
                    "create",
                    "/tmp/ftf.yaml",
                ],
                check=True,
            )

        self.next(self.end)

    @step
    def end(self):
        """Trigger downstream pipeline and test triggering behaviors"""
        if self.trigger_enabled:
            logger.info("\nTesting run_kubeflow_pipeline")
            run_id: str = run_argo_workflow(
                KUBERNETES_NAMESPACE,
                self.template_name,
                parameters={
                    "trigger_enabled": False,
                    "triggered_by": current.run_id,
                },
            )
            logger.info(f"{run_id=}")
            logger.info(f"{run_id_to_url(run_id, KUBERNETES_NAMESPACE)=}")

            logger.info("Testing timeout exception for wait_for_kfp_run_completion")
            try:
                wait_for_argo_run_completion(
                    run_id, KUBERNETES_NAMESPACE, wait_timeout=0.1
                )
            except TimeoutError:
                logger.error(
                    "Timeout before flow ends throws timeout exception correctly"
                )
            else:
                raise AssertionError("Timeout error not thrown as expected.")

            logger.info("Test wait_for_kfp_run_completion without triggering timeout")
            status: str = wait_for_argo_run_completion(
                run_id, KUBERNETES_NAMESPACE, wait_timeout=datetime.timedelta(minutes=3)
            )

            logger.info(f"Run Status of {run_id}: {status=}")

            metaflow_run_id: str = to_metaflow_run_id(run_id)
            logger.info(f"Test triggered_by is passed correctly")
            metaflow_path = f"{current.flow_name}/{metaflow_run_id}/start"

            _retry_sleep(self.assert_task_triggered_by, metaflow_path=metaflow_path)

            logger.info(f"Deleting {self.template_name=}")
            delete_argo_workflow(KUBERNETES_NAMESPACE, self.template_name)
        else:
            logger.info(f"{self.trigger_enabled=}")

    @staticmethod
    def assert_task_triggered_by(metaflow_path: str):
        logger.info(f"fetching start step {metaflow_path}")
        start_step = Step(metaflow_path)
        logger.info(f"assert {start_step.task.data.triggered_by=} == {current.run_id=}")
        assert start_step.task.data.triggered_by == current.run_id


if __name__ == "__main__":
    FlowTriggeringFlow()
