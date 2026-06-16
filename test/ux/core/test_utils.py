import os
import time
from typing import Any, Dict, List, Optional

from metaflow import Deployer, Flow, Run, Runner, namespace
from metaflow.exception import MetaflowNotFound


# Directory containing the test flows, relative to this file
_FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def _resolve_flow_path(flow_name: str) -> str:
    """Resolve a flow path relative to the flows directory."""
    if os.path.isabs(flow_name):
        return flow_name
    return os.path.join(_FLOWS_DIR, flow_name.removeprefix("flows/"))


def prepare_runner_deployer_args(tl_args: Dict[str, Any]) -> Dict[str, Any]:
    """Filter and set defaults for Runner/Deployer top-level arguments."""
    filtered = {k: v for k, v in tl_args.items() if v is not None and v != ""}
    filtered.setdefault("pylint", False)
    return filtered


def disp_test(
    exec_mode: str, decospecs: Any, tag: List[str], scheduler_config: Any
) -> None:
    print(
        f"Test Configuration - Exec mode: {exec_mode}, Decospecs: {decospecs}, "
        f"Tag: {tag}, Scheduler Config: {scheduler_config}"
    )


def _evict_flow_module_cache(flow_path: str) -> None:
    """Remove a flow file from click_api's loaded_modules cache.

    This is needed for test isolation: when a flow uses FlowMutator/config_value,
    the module must be reloaded for each test so that the mutator sees fresh config.
    """
    try:
        from metaflow.runner import click_api

        click_api.loaded_modules.pop(flow_path, None)
    except Exception:
        pass


def run_flow_with_env(
    flow_name: str, runner_args: Optional[Dict[str, Any]] = None, **tl_args
):
    """Run a flow locally using Runner."""
    from metaflow import metaflow_version

    flow_path = _resolve_flow_path(flow_name)
    print(f"Running flow {flow_path} with metaflow: {metaflow_version.get_version()}")

    # Clear the module cache so that FlowMutator/config_value are applied fresh.
    _evict_flow_module_cache(flow_path)

    runner_args = runner_args or {}
    filtered_tl_args = prepare_runner_deployer_args(tl_args)
    print(f"Runner args: {runner_args}, TL args: {filtered_tl_args}")

    with Runner(flow_path, **filtered_tl_args).run(**runner_args) as running:
        return running.run


def deploy_flow_to_scheduler(
    flow_name: str,
    tl_args: Dict[str, Any],
    scheduler_args: Dict[str, Any],
    deploy_args: Dict[str, Any],
    scheduler_type: str,
):
    """Deploy a flow to a scheduler (e.g. step-functions, argo-workflows)."""
    from metaflow import metaflow_version

    flow_path = _resolve_flow_path(flow_name)
    print(
        f"Deploying flow {flow_path} to scheduler {scheduler_type} "
        f"using metaflow: {metaflow_version.get_version()}"
    )

    # Evict the module cache so that config_value / FlowMutator are always
    # applied to a freshly loaded class.  Without this, a previous test that
    # loaded the same flow (with different config) leaves the class in a
    # _configs_processed=True state, causing _process_config_decorators to
    # skip mutation and leaving added parameters (e.g. param3) absent from
    # the WorkflowTemplate.
    _evict_flow_module_cache(flow_path)

    filtered_tl_args = prepare_runner_deployer_args(tl_args)
    deployer = Deployer(flow_file=flow_path, **filtered_tl_args)
    # Normalize scheduler_args: translate the generic 'cluster' key to
    # the scheduler-specific arg, and drop unsupported keys.
    normalized_sched_type = scheduler_type.replace("-", "_")
    norm_sched_args = dict(scheduler_args)
    # Drop 'cluster' — it's the k8s namespace which comes from METAFLOW_KUBERNETES_NAMESPACE
    # in the global config, not passed as a create() argument.
    norm_sched_args.pop("cluster", None)
    deployed_flow = getattr(deployer, normalized_sched_type)(**norm_sched_args).create(
        **deploy_args
    )
    print(f"Deployed workflow {deployed_flow.name}")
    return deployed_flow


def wait_for_runs_by_tags(
    flow_name: str, tags: List[str], timeout: int = 10, polling_interval: int = 60
) -> List[str]:
    """Poll until all runs matching flow_name and tags have finished, then return their pathspecs.

    Blocks until every matching run has a non-None finished_at, or until timeout minutes elapse.
    Note: the number of runs returned depends on how many have started by the time all running
    runs finish — on low-resource infra this may be fewer than expected when concurrent runs
    are involved.
    """
    start_time = time.time()
    namespace(None)
    runs = []
    flow_obj = None

    while time.time() - start_time < timeout * 60:
        if flow_obj is None:
            try:
                flow_obj = Flow(flow_name)
            except MetaflowNotFound:
                print(f"Flow {flow_name} not found, waiting...")
                time.sleep(polling_interval)
                continue

        runs = list(flow_obj.runs(*tags))
        if len(runs) > 0 and all(r.finished_at is not None for r in runs):
            break

        print(f"Found {len(runs)} runs, waiting for completion...")
        time.sleep(polling_interval)

    return [r.pathspec for r in runs]


def verify_single_run(flow_name: str, tags: List[str], timeout: int = 60) -> Run:
    """Verify exactly one run exists for the given flow and tags."""
    run_pathspecs = wait_for_runs_by_tags(flow_name, tags, timeout)

    if len(run_pathspecs) != 1:
        raise RuntimeError(
            f"Expected 1 run for flow {flow_name} with tags {tags}, "
            f"got {len(run_pathspecs)}"
        )

    run = Run(run_pathspecs[0], _namespace_check=False)
    print(f"Found run {run.id} for flow {flow_name}")

    if not run.successful:
        raise RuntimeError(f"Run {run.id} failed")

    return run


def _is_failed_status(status: Optional[str]) -> bool:
    """Return True if the status string indicates a terminal failure (case-insensitive)."""
    return status is not None and status.upper() in ("FAILED", "TIMED_OUT", "ABORTED")


def wait_for_deployed_run(
    deployed_flow,
    timeout: int = 3600,
    run_kwargs: Optional[Dict[str, Any]] = None,
    polling_interval: int = 3,
):
    """Trigger a deployed flow and wait for it to complete."""
    print(f"Deployed flow {deployed_flow.name}")
    run_kwargs = run_kwargs or {}
    triggered_run = deployed_flow.trigger(**run_kwargs)

    start_time = time.time()
    while triggered_run.run is None:
        if time.time() - start_time > timeout:
            raise RuntimeError(f"Run failed to start within {timeout} seconds")
        status = triggered_run.status
        if _is_failed_status(status):
            raise RuntimeError(
                f"Deployed run failed before starting (status: {status})"
            )
        print("Waiting for run to start...")
        time.sleep(polling_interval)

    run = triggered_run.run
    assert run is not None, (
        "triggered_run.run returned None — run_id mismatch between deployer and init block. "
        "Check that pipeline_run_id kwarg is injected by the scheduler."
    )
    print(f"Run {run.id} started")

    while not triggered_run.run.finished:
        if time.time() - start_time > timeout:
            raise RuntimeError(
                f"Run {triggered_run.run.id} failed to complete within {timeout} seconds"
            )
        status = triggered_run.status
        if _is_failed_status(status):
            raise RuntimeError(f"Run {triggered_run.run.id} failed (status: {status})")
        print(f"Waiting for run {triggered_run.run.id} to complete...")
        time.sleep(polling_interval)

    print(f"Run {triggered_run.run.id} completed")
    return triggered_run.run


def wait_for_deployed_run_allow_failure(
    deployed_flow,
    timeout: int = 3600,
    run_kwargs: Optional[Dict[str, Any]] = None,
    polling_interval: int = 3,
):
    """Trigger a deployed flow and wait for it to finish, even if it fails.

    Same as wait_for_deployed_run but does NOT raise on failed status.
    Returns the Run regardless of success/failure so the caller can assert
    on run.successful.
    """
    print(f"Deployed flow {deployed_flow.name}")
    run_kwargs = run_kwargs or {}
    triggered_run = deployed_flow.trigger(**run_kwargs)

    start_time = time.time()
    while triggered_run.run is None:
        if time.time() - start_time > timeout:
            raise RuntimeError(f"Run failed to start within {timeout} seconds")
        print("Waiting for run to start...")
        time.sleep(polling_interval)

    print(f"Run {triggered_run.run.id} started")

    while not triggered_run.run.finished:
        if time.time() - start_time > timeout:
            raise RuntimeError(
                f"Run {triggered_run.run.id} failed to complete within {timeout} seconds"
            )
        print(f"Waiting for run {triggered_run.run.id} to complete...")
        time.sleep(polling_interval)

    print(
        f"Run {triggered_run.run.id} completed (successful={triggered_run.run.successful})"
    )
    return triggered_run.run


def verify_run_provenance(run: Run, decospecs: Any) -> None:
    """Verify the run used the expected datastore and execution environment.

    Checks:
    1. ds-type == "s3": artifacts were stored on the devstack MinIO, not the local
       filesystem. A local ds-type indicates the Metaflow config didn't point at the
       devstack S3 endpoint. This check is skipped when METAFLOW_DEFAULT_DATASTORE is
       "local" (e.g. CI environments that don't have MinIO).
    2. KUBERNETES_SERVICE_HOST was set (for kubernetes decospec backends): proves the
       task actually ran inside a Kubernetes pod and the decospec took effect.
    """
    import os

    start_task = run["start"].task

    ds_type = start_task.metadata_dict.get("ds-type")
    # Only enforce the S3 check when the test environment uses a remote datastore.
    # Local-only CI environments (METAFLOW_DEFAULT_DATASTORE=local) do not have MinIO.
    if os.environ.get("METAFLOW_DEFAULT_DATASTORE", "") != "local":
        assert ds_type == "s3", (
            f"Expected datastore type 's3' (MinIO), got {ds_type!r}. "
            f"Artifacts may be stored locally — check METAFLOW_HOME / METAFLOW_PROFILE."
        )

    if decospecs and any("kubernetes" in str(d) for d in decospecs):
        execution_env = start_task.data.execution_env
        assert execution_env, (
            "Expected task to run on Kubernetes (KUBERNETES_SERVICE_HOST set), "
            "but execution_env is empty. "
            "The kubernetes decospec may not have taken effect."
        )


def send_event(scheduler_type, event_name, payload, scheduler_config):
    """Send a trigger event to the appropriate orchestrator.

    Each orchestrator branch adds its implementation here:
    - Argo Events: POST to webhook (localhost:12000)
    - Step Functions: EventBridge put_events or start_execution
    - Airflow: POST to DAG trigger API

    Raises NotImplementedError if the scheduler_type is not yet implemented.
    Tests that call this catch NotImplementedError and skip gracefully.
    """
    raise NotImplementedError(
        f"send_event not implemented for {scheduler_type}. "
        f"Add implementation on the appropriate devstack branch."
    )


def get_run_pathspecs(flow_name, tags, timeout=10, polling_interval=60):
    """Get pathspecs for runs matching flow_name and tags.

    Convenience wrapper around track_runs_by_tags for use in trigger tests
    where we need to find runs that were triggered asynchronously.
    """
    return track_runs_by_tags(flow_name, tags, timeout, polling_interval)


def execute_test_flow(
    flow_name: str,
    exec_mode: str,
    decospecs: Any,
    tag: List[str],
    scheduler_config: Any,
    test_name: Optional[str] = None,
    run_params: Optional[Dict[str, Any]] = None,
    tl_args_extra: Optional[Dict[str, Any]] = None,
) -> Run:
    """Execute a test flow in runner or deployer mode."""
    if not test_name:
        test_name = flow_name.split("/")[-1].replace(".py", "")
    test_unique_tag = f"test_{test_name}_{exec_mode}"
    combined_tags = tag + [test_unique_tag]

    tl_args: Dict[str, Any] = {"decospecs": decospecs}
    if tl_args_extra:
        tl_args.update(tl_args_extra)

    runner_args = {"tags": combined_tags}
    if run_params:
        runner_args.update(run_params)

    if exec_mode == "deployer":
        sched_type = scheduler_config.scheduler_type
        extra_deploy_args = getattr(scheduler_config, "deploy_args", None) or {}
        deployed_flow = deploy_flow_to_scheduler(
            flow_name=flow_name,
            tl_args=tl_args,
            scheduler_args={"cluster": scheduler_config.cluster},
            deploy_args={"tags": combined_tags, **extra_deploy_args},
            scheduler_type=sched_type,
        )
        run = wait_for_deployed_run(deployed_flow, run_kwargs=run_params)
    else:
        run = run_flow_with_env(flow_name=flow_name, runner_args=runner_args, **tl_args)

    return run
