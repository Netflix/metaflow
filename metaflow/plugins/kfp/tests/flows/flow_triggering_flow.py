from metaflow import FlowSpec, Parameter, Step, current, step
from metaflow.metaflow_config import KFP_SDK_NAMESPACE
from metaflow.plugins.kfp import (
    logger,
    run_id_to_url,
    run_kubeflow_pipeline,
    wait_for_kfp_run_completion,
)
from metaflow.plugins.kfp.kfp_utils import (
    _get_kfp_client,
    _upload_pipeline,
    to_metaflow_run_id,
)
import datetime

TEST_PIPELINE_NAME = "metaflow-unit-test-flow-triggering-flow"
logger.handlers.clear()  # Avoid double printint logs TODO (yunw) address logging story


class FlowTriggeringFlow(FlowSpec):
    # Avoid infinite self trigger
    trigger_enabled: bool = Parameter("trigger_enabled", default=False)
    triggered_by: str = Parameter(name="triggered_by", default=None)

    @step
    def start(self):
        """Upload a downstream pipeline to be triggered"""
        if self.triggered_by:
            print(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:  # Upload pipeline
            self.pipeline_id, self.version_id = _upload_pipeline(
                flow_file_path=__file__, pipeline_name=TEST_PIPELINE_NAME
            )
        self.next(self.end)

    @step
    def end(self):
        """Trigger downstream pipeline and test triggering behaviors"""
        if self.trigger_enabled:
            print("\nTesting run_kubeflow_pipeline")
            run_id: str = run_kubeflow_pipeline(
                pipeline_name=TEST_PIPELINE_NAME,
                kubeflow_namespace=KFP_SDK_NAMESPACE,
                triggered_run_name=f"FlowTriggeringFlow triggered by run {current.run_id}",
                kubeflow_experiment_name="default",
                parameters={
                    "triggered_by": current.run_id,
                },
                # Specify version to avoid interaction among multiple test runs
                pipeline_version_id=self.version_id,
            )
            print("Run ID:", run_id)
            print("Run URL:", run_id_to_url(run_id))

            print("\nTesting timeout exception for wait_for_kfp_run_completion")
            try:
                wait_for_kfp_run_completion(run_id=run_id, wait_timeout=0.1)
            except TimeoutError:
                print("Timeout before flow ends throws timeout exception correctly")
            else:
                raise AssertionError("Timeout error not thrown as expected.")

            print("\nTest wait_for_kfp_run_completion without triggering timeout")
            status: str = wait_for_kfp_run_completion(
                run_id=run_id,
                wait_timeout=datetime.timedelta(minutes=3),
            )
            print(f"Run Status of {run_id}:", status)

            print("\nTest parameter is passed correctly")
            metaflow_run_id: str = to_metaflow_run_id(run_id)
            start_step = Step(f"{self.__class__.__name__}/{metaflow_run_id}/start")
            assert start_step.task.data.triggered_by == current.run_id

            print(f"\nDeleting {TEST_PIPELINE_NAME} pipeline version {self.version_id}")
            client = _get_kfp_client()
            client.delete_pipeline_version(self.version_id)


if __name__ == "__main__":
    FlowTriggeringFlow()
