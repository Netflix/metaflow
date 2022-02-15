try:
    import kfp_server_api
    import kfp
except ImportError:
    import os

    print("Installing extra dependencies `zillow-kfp` and `kfp-server-api`")
    os.system(
        "pip install --quiet --disable-pip-version-check --no-cache-dir "
        "-i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/ "
        "zillow-kfp kfp-server-api"
    )
    os.environ["FLOW_TRIGGERING_FLOW_DEP_INSTALLED"] = "true"
    print("Extra dependencies installed")
    import kfp_server_api
    import kfp

from metaflow import FlowSpec, Parameter, current, step
from metaflow.plugins.kfp import (  # noqa
    logger,
    run_id_to_url,
    run_kubeflow_pipeline,
    wait_for_kfp_run_completion,
)
from metaflow.plugins.kfp.kfp_utils import _get_kfp_client, _upload_pipeline  # noqa
from metaflow.mflog import LOG_SOURCES


TEST_PIPELINE_NAME = "metaflow-unit-test-flow-triggering-flow"
logger.handlers.clear()  # Avoid double printint logs  # TODO (yunw) address logging story


class FlowTriggeringFlow(FlowSpec):
    trigger_enabled: bool = Parameter("trigger_enabled", default=False)
    triggered_by: str = Parameter(name="triggered_by", default=None)
    triggered_flow_namespace: str = Parameter(
        name="namespace", default="aip-metaflow-sandbox"
    )

    @step
    def start(self):
        if self.triggered_by:
            print(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:  # Upload pipeline
            self.pipeline_id, self.version_id = _upload_pipeline(
                flow_file_path=__file__, pipeline_name=TEST_PIPELINE_NAME
            )
        self.next(self.test_trigger_and_wait)

    @step
    def test_trigger_and_wait(self):
        if self.trigger_enabled:
            print("\nTesting run_kubeflow_pipeline")
            run_id: str = run_kubeflow_pipeline(
                pipeline_name=TEST_PIPELINE_NAME,
                kubeflow_namespace=self.triggered_flow_namespace,
                triggered_run_name=f"FlowTriggeringFlow triggered by run {current.run_id}",
                kubeflow_experiment_name="default",
                parameters={
                    "triggered_by": current.run_id,
                },
                # Specify version so that multiple instance of tests can be triggered
                pipeline_version_id=self.version_id,
            )
            print("Run ID:", run_id)
            print("Run URL:", run_id_to_url(run_id))

            print("\nTesting timeout exception for wait_for_kfp_run_completion")
            try:
                run = wait_for_kfp_run_completion(run_id=run_id, wait_timeout=10)
            except TimeoutError:
                print("Timeout before flow ends throws timeout exception correctly")
            else:
                raise AssertionError("Timeout error not thrown as expected.")

            print("\nTesting wait_for_kfp_run_completion without triggering timeout")
            status: str = wait_for_kfp_run_completion(run_id=run_id, wait_timeout=180)
            print(f"Run Status of {run_id}:", status)

            print("\nDemo that datastore of downstream job can be accessed")
            from metaflow.datastore.s3 import S3DataStore

            S3DataStore.datastore_root = "s3://aip-example-sandbox/metaflow-prototype"
            s3_datastore = S3DataStore(
                self.__class__.__name__,
                run_id=f"kfp-{run_id}",
                step_name="start",
                task_id="kfp1",
            )
            metadata = s3_datastore.load_metadata("data")
            log_stdout = s3_datastore.load_logs(LOG_SOURCES, "stdout")

            print("\nMetadata: \n", metadata)
            print("\nStdout: \n", log_stdout)

        self.next(self.end)

    @step
    def end(self):
        if self.trigger_enabled:
            print(f"Deleting version {self.version_id}")
            client = _get_kfp_client()
            client.delete_pipeline_version(self.version_id)


if __name__ == "__main__":
    FlowTriggeringFlow()
