import datetime
import subprocess
import time
import uuid
import base64
from typing import Callable

from kfp.compiler._k8s_helper import sanitize_k8s_name
from metaflow import FlowSpec, Parameter, Step, current, step
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
from metaflow.plugins.aip import (
    ArgoHelper,
    get_argo_url,
    get_metaflow_run_id,
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
            # for the case where generate_base64_uuid returns a string starting with '-'
            # and template_name == 'wfdsk-ftf-test-he5d4--6rhai0z0wiysuew'
            # where aip _create_workflow_yaml() calls sanitize_k8s_name() which returns
            # 'wfdsk-ftf-test-he5d4-6rhai0z0wiysuew' without the double --

            print(f"{KUBERNETES_NAMESPACE=}")

            self.workflow_template_names = [
                sanitize_k8s_name(
                    f"{TEST_TEMPLATE_NAME}-{generate_base64_uuid()}".lower()
                )
                for _ in range(3)
            ]
            self.triggered_by_tag = "triggerred-by"
            self.index_tag = "template-index"

            for template_name, template_index in enumerate(
                self.workflow_template_names
            ):
                path = f"/tmp/{template_name}.yaml"
                logger.info(f"Creating workflow: {template_name}")
                self.comiple_workflow(
                    template_name,
                    path,
                    extra_args=[
                        "--tag",
                        f"{self.triggered_by_tag}:{current.run_id}" "--tag",
                        f"{self.index}:{template_index}",
                    ],
                )
                subprocess.run(["cat", path])
                self.submit_template(path)
                time.sleep(1)  # Spacing workflow template submission time.

        self.next(self.end)

    @step
    def end(self):
        """Trigger downstream pipeline and test triggering behaviors"""
        if self.trigger_enabled:
            argo_helper = ArgoHelper()

            # ====== Test template filtering ======
            # Test latest template is returned with prefix filter
            assert self.workflow_template_names[-1] == argo_helper.template_get_latest(
                template_prefix=sanitize_k8s_name(TEST_TEMPLATE_NAME.lower()),
                flow_name=current.flow_name,
                filter_func=lambda template: template["metadata"]["labels"][
                    f"metaflow.org/tag_{self.triggered_by_tag}"
                ]
                == current.run_id,
            )

            # Test filter func correctly filters
            assert self.workflow_template_names[1] == argo_helper.template_get_latest(
                template_prefix=sanitize_k8s_name(TEST_TEMPLATE_NAME.lower()),
                flow_name=current.flow_name,
                filter_func=lambda template: template["metadata"]["labels"][
                    f"metaflow.org/tag_{self.triggered_by_tag}"
                ]
                == current.run_id
                and template["metadata"]["labels"][f"metaflow.org/tag_{self.index_tag}"]
                == str(1),
            )

            # ====== Test template triggering ======
            logger.info("\n Testing ArgoHelper.trigger")
            run_id, run_uid = argo_helper.trigger(
                template_name=self.workflow_template_names[0],
                parameters={
                    "trigger_enabled": False,
                    "triggered_by": current.run_id,
                },
            )
            logger.info(f"{run_id=}, {run_uid=}")
            logger.info(f"{get_argo_url(run_id, KUBERNETES_NAMESPACE, run_uid)=}")

            logger.info("Testing timeout exception for wait_for_kfp_run_completion")
            try:
                argo_helper.watch(run_id, wait_timeout=0.1)
            except TimeoutError:
                logger.info(
                    "Timeout before flow ends throws timeout exception correctly"
                )
            else:
                raise AssertionError("Timeout error not thrown as expected.")

            logger.info("Test wait_for_kfp_run_completion without triggering timeout")
            status: str = argo_helper.watch(
                run_id, wait_timeout=datetime.timedelta(minutes=3)
            )

            logger.info(f"Run Status of {run_id}: {status=}")

            metaflow_run_id: str = get_metaflow_run_id(run_uid)
            logger.info(f"Test triggered_by is passed correctly")
            metaflow_path = f"{current.flow_name}/{metaflow_run_id}/start"

            _retry_sleep(self.assert_task_triggered_by, metaflow_path=metaflow_path)

            # ====== Clean up test templates ======
            for template_name in self.workflow_template_names:
                logger.info(f"Deleting {template_name}")
                argo_helper.template_delete(template_name)
        else:
            logger.info(f"{self.trigger_enabled=}")

    @staticmethod
    def assert_task_triggered_by(metaflow_path: str):
        logger.info(f"fetching start step {metaflow_path}")
        start_step = Step(metaflow_path)
        logger.info(f"assert {start_step.task.data.triggered_by=} == {current.run_id=}")
        assert start_step.task.data.triggered_by == current.run_id

    @staticmethod
    def comiple_workflow(template_name, path, extra_args=None):
        extra_args = extra_args or []
        subprocess.run(
            [
                "python",
                __file__,
                "aip",
                "create",
                "--name",
                template_name,
                "--yaml-only",
                "--pipeline-path",
                path,
                "--kind",
                "WorkflowTemplate",
                "--max-run-concurrency",
                "0",
                *extra_args,
            ],
            check=True,
        )

    @staticmethod
    def submit_template(path):
        subprocess.run(
            [
                "argo",
                "template",
                "-n",
                KUBERNETES_NAMESPACE,
                "create",
                path,
            ],
            check=True,
        )


if __name__ == "__main__":
    FlowTriggeringFlow()
