"""
Frequently used KFP client interactions, including support for:
- flow triggering flow

Code here exists mainly for user's convenience, to take advantage of Metaflow configs.
They are technically not metaflow code.
"""
import datetime
import json
import logging
import os

import math
import posixpath
import sys
import time
from typing import Callable, Optional, Union

from metaflow.metaflow_config import KFP_RUN_URL_PREFIX, KFP_USER_DOMAIN
from metaflow.plugins.kfp.kfp_constants import KFP_CLI_DEFAULT_RETRY
from metaflow.util import get_username

try:  # Extra required dependency specific to kfp plug-in may not exists
    from kfp import Client as KFPClient
    from kfp_server_api import (
        ApiExperiment,
        ApiPipeline,
        ApiPipelineVersion,
        ApiRun,
        RunServiceApi,
    )
    import kfp_server_api
except ImportError:  # Silence import errors in type hint
    KFPClient = None
    ApiExperiment = None
    ApiPipeline = None
    ApiPipelineVersion = None
    ApiRun = None
    RunServiceApi = None
    kfp_server_api = None


def _get_kfp_logger():
    """Setup logger for KFP plugin

    With expected usage from Jupyter notebook and console in mind,
    INFO level logs need to show up in Jupyter notebook output cell and consoles.
    Therefore some default setting below:
        - Default logging level at logging.DEBUG
        - Default to a StreamHandler pointing to stdout.
    Users retain the ability to alter logger behavior using logging module.
    """
    kfp_logger = logging.getLogger("metaflow.kfp")
    if not kfp_logger.hasHandlers():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        kfp_logger.addHandler(stdout_handler)
    kfp_logger.setLevel(logging.DEBUG)
    return kfp_logger


logger = _get_kfp_logger()


def _get_kfp_client():
    kfp_client_user = get_username()
    if KFP_USER_DOMAIN:
        kfp_client_user += f"@{KFP_USER_DOMAIN}"
    else:  # FIXME(yunw)(AIP-5686): The KFP_USER_DOMAIN value should be available in flow
        kfp_client_user += "@zillowgroup.com"
    return KFPClient(userid=kfp_client_user)


def _retry(func: Callable, **kwargs):
    for attempt in range(1, KFP_CLI_DEFAULT_RETRY + 1):
        try:
            return func(**kwargs)
        except Exception as e:
            logging.exception(f"Function {func.__name__} failed on attempt {attempt}")
            if attempt == KFP_CLI_DEFAULT_RETRY:
                raise e


def _get_kfp_run(run_id: str, client: Optional[KFPClient] = None) -> ApiRun:
    if run_id.startswith("kfp-"):
        run_id = run_id[len("kfp-") :]  # noqa
    client: KFPClient = client or _get_kfp_client()
    return _retry(client.get_run, run_id=run_id).run


def run_id_to_url(run_id: str):
    return posixpath.join(
        KFP_RUN_URL_PREFIX,
        "_/pipeline/#/runs/details",
        run_id,
    )


def to_metaflow_run_id(run_id: str):
    """Return metaflow run id useful for querying metadata, datastore, log etc."""
    return f"kfp-{run_id}" if not run_id.startswith("kfp-") else run_id


def run_kubeflow_pipeline(
    kubeflow_namespace: str,
    pipeline_name: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    triggered_run_name: Optional[str] = None,
    experiment_name: Optional[str] = None,
    pipeline_version_id: Optional[str] = None,
    parameters: Optional[dict] = None,
    wait_timeout: Union[int, float, datetime.timedelta] = 0,
    **kwarg,  # Other parameters for wait function
) -> str:
    """Trigger KFP flow by pipeline id.
    This function imitate behavior of "Create run" button in KFP UI.

    pipeline_id: Pipeline id for which a new run should be triggered.
    experiment_name: Experiment where the triggered run should be placed.
    job_name: Job name of the flow being triggered.

    Return run id of created run. To reconstruct url see run_id_to_url function.

    TODO(yunw)(Ticket todo): Add reference id for chain of runs
    """
    if pipeline_name is None and pipeline_id is None:
        raise ValueError("Exactly one of `pipeline_name` or `pipeline_id` is needed")

    if parameters is None:
        parameters = {}

    client = _get_kfp_client()

    if pipeline_id is None:
        pipeline_id = client.get_pipeline_id(name=pipeline_name)

    if triggered_run_name is None:
        pipeline: ApiPipeline = _retry(client.get_pipeline, pipeline_id=pipeline_id)
        time_format: str = "%Y-%m-%d %H-%M-%S"
        triggered_run_name = (
            f"Triggered {pipeline.name} {datetime.datetime.now().strftime(time_format)}"
        )
    if len(triggered_run_name) > 64:  # Name >64 char causes kfp run time error
        triggered_run_name = triggered_run_name[:63]

    if experiment_name is None:
        pipeline: ApiPipeline = _retry(client.get_pipeline, pipeline_id=pipeline_id)
        experiment_name = f"triggered-{pipeline.name}"
    if len(experiment_name) > 64:  # Name >64 char causes kfp run time error
        experiment_name = experiment_name[0:63]

    experiment: ApiExperiment = _retry(
        client.create_experiment,
        name=experiment_name,
        namespace=kubeflow_namespace,
    )
    pipeline_run: ApiRun = _retry(
        client.run_pipeline,
        experiment_id=experiment.id,
        job_name=triggered_run_name,
        pipeline_id=pipeline_id,
        params={"flow_parameters_json": json.dumps(parameters)},
        version_id=pipeline_version_id,
    )
    logger.info(
        f"Triggered run {triggered_run_name}({pipeline_run.id}) - {run_id_to_url(pipeline_run.id)}"
    )

    if wait_timeout:  # int, float and datetime.timedelta all evaluates to False when 0
        wait_for_kfp_run_completion(
            run_id=pipeline_run.id, wait_timeout=wait_timeout, **kwarg
        )

    return pipeline_run.id


def _is_finished_run(run: ApiRun):
    finished_states = ["succeeded", "failed", "skipped", "error"]
    return run.status and run.status.lower() in finished_states


def _assert_run_success(run: ApiRun):
    if run.status is None:
        # None status usually occurs when run is recently started and has not been scheduled
        # Raise different error, allowing user to catch them.
        raise ValueError(
            f"Run status not available. Run might not have been scheduled."
        )
    else:
        assert (
            run.status.lower() == "succeeded"
        ), f"Run {run.id} finished with non-successful state {run.status}."


def check_kfp_run_status(
    run_id: str,
    assert_success: bool,
) -> str:
    client: KFPClient = _get_kfp_client()
    run: ApiRun = _get_kfp_run(run_id=run_id, client=client)

    if assert_success:
        _assert_run_success(run)

    return run.status


def wait_for_kfp_run_completion(
    run_id: str,
    wait_timeout: Union[int, float, datetime.timedelta] = 0,
    min_check_delay: int = 10,
    max_check_delay: int = 30,
    assert_success: bool = True,
) -> str:
    """Check for KFP run status. Returns status as a string.

    If timeout (in second) is positive this function waits for flow to complete.
    Raise timeout if run is not finished after <timeout> seconds
    - finished or not (bool)
    - success or not (bool)
    - run info (kfp_server_api.ApiRun)

    Status check frequency will be close to min_check_delay for the first 11 minutes,
    and gradually approaches max_check_delay after 23 minutes.

    A close mimic to async is to use _get_kfp_run above.
    Implementation for async is not prioritized until specifically requested.

    TODO(yunw)(AIP-5671): Async version
    """

    def get_delay(secs_since_start, min_delay, max_delay):
        """
        this sigmoid function reaches
        - 0.1 after 11 minutes
        - 0.5 after 15 minutes
        - 1.0 after 23 minutes
        in other words, the the delay is close to min_delay during the first 10 minutes
        """
        sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
        return min_delay + sigmoid * (max_delay - min_delay)

    client: KFPClient = _get_kfp_client()
    run: ApiRun = _get_kfp_run(run_id=run_id, client=client)

    if not _is_finished_run(run) and wait_timeout:
        if isinstance(wait_timeout, datetime.timedelta):
            wait_timeout = wait_timeout.total_seconds()

        # A mimic of kfp.Client.wait_for_run_completion with customized logging
        logger.info(
            f"Waiting for run {run.id} to finish. Timeout: {wait_timeout} second(s)"
        )
        start_time = datetime.datetime.now()
        while not _is_finished_run(run):
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(
                f"Waiting for the run {run.id} to complete... {elapsed_time:.2f}s / {wait_timeout}s"
            )
            if elapsed_time > wait_timeout:
                raise TimeoutError(f"Timeout while waiting for run {run_id} to finish.")
            time.sleep(
                get_delay(
                    elapsed_time, min_delay=min_check_delay, max_delay=max_check_delay
                )
            )
            run = _get_kfp_run(run_id=run_id, client=client)

        if assert_success:
            _assert_run_success(run)

    return run.status


def terminate_run(run_id: str, retry: int = KFP_CLI_DEFAULT_RETRY, **kwargs):
    logging.info(f"Terminating run {run_id}")
    run_service_api = RunServiceApi()
    return _retry(
        run_service_api.terminate_run, max_attempt=retry, run_id=run_id, **kwargs
    )


def _upload_pipeline(flow_file_path: str, pipeline_name: Optional[str] = None):
    """Upload this flow to keep version consistency

    ** NOT OFFICIALLY SUPPORTED FOR USER - FOR TESTING ONLY **
    * User experience is not polished.
    * Test for this function is not complete
    Users are recommended to upload pipeline though paved CICD instead.

    Flow triggering flow only triggers uploaded flows.
    This function is a workaround for testing,
    to ensure downstream pipeline code is updated per test trigger.
    """
    if pipeline_name is None:
        file_base_name = os.path.basename(flow_file_path)
        pipeline_name = os.path.splitext(file_base_name)[0]
    pipeline_name = pipeline_name[: min(63, len(pipeline_name) - 1)]

    print("Uploading downstream pipeline")

    import tempfile

    with tempfile.TemporaryDirectory() as dir_path:
        pipeline_file_path = f"{dir_path}/temp_test_pipeline.yaml"
        print(f"Compiling test flow to local file {pipeline_file_path}...")
        os.system(
            f"python '{flow_file_path}' kfp run --yaml-only --pipeline-path '"
            f"{pipeline_file_path}'"
        )

        print("Uploading pipeline...")
        client = _get_kfp_client()
        try:
            pipeline: ApiPipeline = _retry(
                func=client.upload_pipeline,
                pipeline_package_path=pipeline_file_path,
                pipeline_name=pipeline_name,
            )
            pipeline_id = pipeline.id
            version_id = pipeline.default_version.id
        except kfp_server_api.exceptions.ApiException:
            version: ApiPipelineVersion = _retry(
                func=client.upload_pipeline_version,
                pipeline_package_path=pipeline_file_path,
                pipeline_version_name=f"flow_triggering_flow_{datetime.datetime.now()}",
                pipeline_name=pipeline_name,
            )
            pipeline_id = version.resource_references[0].key.id
            version_id = version.id
        print(f"Uploaded test pipeline {pipeline_id} version {version_id}.")

    return pipeline_id, version_id
