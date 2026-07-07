"""
Integration tests for localbatch — the local AWS Batch emulator.

Test layout mirrors the spin tests:
  - TestBatchAPI  : validates every Batch REST endpoint using boto3.
                    No Docker required — jobs fail gracefully when unavailable.
  - TestDockerExecution : validates actual container execution.
                    Skipped when Docker is not running.
  - TestMetaflowE2E     : runs a real Metaflow flow via the @batch decorator
                    and verifies artifacts, mirroring the spin test style.
                    Requires Docker.

Run only the API tests (no Docker needed):
    pytest test/unit/localbatch/ -m "not docker"

Run everything:
    pytest test/unit/localbatch/
"""

import time

import pytest
import requests

pytest.importorskip("localbatch", reason="localbatch package not installed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll_job(batch_client, job_id, timeout=30):
    """Poll describe_jobs until the job reaches a terminal state."""
    for _ in range(timeout * 2):
        resp = batch_client.describe_jobs(jobs=[job_id])
        job = resp["jobs"][0]
        if job["status"] in ("SUCCEEDED", "FAILED"):
            return job
        time.sleep(0.5)
    pytest.fail(f"Job {job_id} did not reach terminal state within {timeout}s")


def _register(batch_client, name, command=None):
    """Register a minimal job definition and return its ARN."""
    resp = batch_client.register_job_definition(
        jobDefinitionName=name,
        type="container",
        containerProperties={
            "image": "alpine:latest",
            "command": command or ["echo", "hello"],
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "256"},
            ],
        },
    )
    return resp["jobDefinitionArn"]


# ---------------------------------------------------------------------------
# Batch REST API tests  (no Docker required)
# ---------------------------------------------------------------------------


class TestBatchAPI:
    def test_health(self, localbatch_server):
        resp = requests.get(f"{localbatch_server.base_url}/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_describe_queues_returns_default(self, batch_client):
        resp = batch_client.describe_job_queues()
        queues = resp["jobQueues"]
        assert len(queues) >= 1
        names = [q["jobQueueName"] for q in queues]
        assert "localbatch-default" in names

    def test_default_queue_is_healthy(self, batch_client):
        resp = batch_client.describe_job_queues(jobQueues=["localbatch-default"])
        q = resp["jobQueues"][0]
        assert q["state"] == "ENABLED"
        assert q["status"] == "VALID"
        assert len(q["computeEnvironmentOrder"]) >= 1

    def test_describe_compute_environments(self, batch_client):
        resp = batch_client.describe_compute_environments()
        envs = resp["computeEnvironments"]
        assert len(envs) >= 1
        assert envs[0]["status"] == "VALID"

    def test_register_job_definition(self, batch_client):
        resp = batch_client.register_job_definition(
            jobDefinitionName="api-test-jobdef",
            type="container",
            containerProperties={
                "image": "alpine:latest",
                "command": ["echo", "hello"],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "1"},
                    {"type": "MEMORY", "value": "256"},
                ],
            },
        )
        assert resp["jobDefinitionName"] == "api-test-jobdef"
        assert resp["revision"] == 1
        assert resp["jobDefinitionArn"].startswith("arn:aws:batch:")

    def test_revision_increments_on_re_register(self, batch_client):
        name = "revision-increment-jobdef"
        for expected in (1, 2, 3):
            resp = batch_client.register_job_definition(
                jobDefinitionName=name,
                type="container",
                containerProperties={
                    "image": "alpine:latest",
                    "command": ["echo", str(expected)],
                    "resourceRequirements": [
                        {"type": "VCPU", "value": "1"},
                        {"type": "MEMORY", "value": "256"},
                    ],
                },
            )
            assert resp["revision"] == expected

    def test_describe_job_definitions_by_name(self, batch_client):
        _register(batch_client, "describe-by-name-job")
        resp = batch_client.describe_job_definitions(
            jobDefinitionName="describe-by-name-job", status="ACTIVE"
        )
        defs = resp["jobDefinitions"]
        assert len(defs) >= 1
        assert defs[0]["jobDefinitionName"] == "describe-by-name-job"

    def test_submit_job_returns_id_and_name(self, batch_client):
        arn = _register(batch_client, "submit-test-job")
        resp = batch_client.submit_job(
            jobName="submit-test-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )
        assert resp["jobId"]
        assert resp["jobName"] == "submit-test-run"

    def test_job_reaches_terminal_state(self, batch_client):
        """Without Docker the job fails; with Docker it succeeds — both are valid."""
        arn = _register(batch_client, "terminal-test-job")
        resp = batch_client.submit_job(
            jobName="terminal-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )
        job = _poll_job(batch_client, resp["jobId"])
        assert job["status"] in ("SUCCEEDED", "FAILED")
        assert job["jobName"] == "terminal-run"
        assert job["jobQueue"] == "localbatch-default"

    def test_describe_jobs_returns_correct_shape(self, batch_client):
        arn = _register(batch_client, "describe-shape-job")
        job_id = batch_client.submit_job(
            jobName="describe-shape-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )["jobId"]

        _poll_job(batch_client, job_id)

        resp = batch_client.describe_jobs(jobs=[job_id])
        job = resp["jobs"][0]
        for field in (
            "jobId",
            "jobName",
            "jobQueue",
            "jobDefinition",
            "status",
            "createdAt",
        ):
            assert field in job, f"Missing field: {field}"

    def test_terminate_job_transitions_to_failed(self, batch_client):
        arn = _register(batch_client, "terminate-test-job", command=["sleep", "999"])
        job_id = batch_client.submit_job(
            jobName="terminate-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )["jobId"]

        batch_client.terminate_job(jobId=job_id, reason="cancelled by test")

        job = _poll_job(batch_client, job_id)
        assert job["status"] == "FAILED"
        assert "cancelled by test" in job.get("statusReason", "")

    def test_list_jobs_returns_summary_list(self, batch_client):
        resp = batch_client.list_jobs(jobQueue="localbatch-default", jobStatus="FAILED")
        assert "jobSummaryList" in resp
        for entry in resp["jobSummaryList"]:
            assert "jobId" in entry
            assert "status" in entry

    def test_ecs_metadata_endpoint(self, localbatch_server):
        job_id = "deadbeef-1234-5678-abcd-000000000000"
        resp = requests.get(f"{localbatch_server.base_url}/metadata/{job_id}/task")
        assert resp.status_code == 200
        data = resp.json()
        container = data["Containers"][0]
        opts = container["LogOptions"]
        assert opts["awslogs-group"] == "/localbatch/batch/job"
        assert job_id in opts["awslogs-stream"]
        assert container["LogDriver"] == "awslogs"


# ---------------------------------------------------------------------------
# Docker execution tests
# ---------------------------------------------------------------------------


@pytest.mark.docker
class TestDockerExecution:
    @pytest.fixture(autouse=True)
    def _require_docker(self):
        try:
            import docker

            docker.from_env().ping()
        except Exception:
            pytest.skip("Docker not available")

    def test_successful_container(self, batch_client):
        arn = _register(
            batch_client,
            "docker-success-job",
            command=["sh", "-c", "echo success && exit 0"],
        )
        job_id = batch_client.submit_job(
            jobName="docker-success-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )["jobId"]

        job = _poll_job(batch_client, job_id, timeout=60)
        assert job["status"] == "SUCCEEDED"

    def test_failed_container_reports_exit_code(self, batch_client):
        arn = _register(
            batch_client, "docker-fail-job", command=["sh", "-c", "exit 42"]
        )
        job_id = batch_client.submit_job(
            jobName="docker-fail-run",
            jobQueue="localbatch-default",
            jobDefinition=arn,
        )["jobId"]

        job = _poll_job(batch_client, job_id, timeout=60)
        assert job["status"] == "FAILED"
        assert job["container"]["exitCode"] == 42

    def test_inject_env_is_visible_inside_container(self):
        """
        Spin up a separate localbatch instance with inject_env set and confirm
        the container can see the injected variable.
        """
        import threading

        import boto3
        import uvicorn
        from localbatch.runner import DockerRunner
        from localbatch.server import create_app
        from localbatch.store import Store

        port = 18766
        store = Store(queue_name="inject-queue")
        runner = DockerRunner(
            store,
            host_addr="host.docker.internal",
            port=port,
            inject_env={"LOCALBATCH_CANARY": "canary-value"},
        )
        app = create_app(store, runner)
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        )
        t = threading.Thread(target=server.run, daemon=True)
        t.start()

        for _ in range(100):
            try:
                requests.get(f"http://127.0.0.1:{port}/health", timeout=0.1)
                break
            except Exception:
                time.sleep(0.05)

        client = boto3.client(
            "batch",
            endpoint_url=f"http://127.0.0.1:{port}",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.register_job_definition(
            jobDefinitionName="inject-canary-job",
            type="container",
            containerProperties={
                "image": "alpine:latest",
                "command": [
                    "sh",
                    "-c",
                    'test "$LOCALBATCH_CANARY" = "canary-value"',
                ],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "1"},
                    {"type": "MEMORY", "value": "256"},
                ],
            },
        )
        job_id = client.submit_job(
            jobName="inject-canary-run",
            jobQueue="inject-queue",
            jobDefinition="inject-canary-job:1",
        )["jobId"]

        job = _poll_job(client, job_id, timeout=60)
        assert (
            job["status"] == "SUCCEEDED"
        ), "LOCALBATCH_CANARY was not visible or had wrong value inside container"

        server.should_exit = True
        t.join(timeout=5)


# ---------------------------------------------------------------------------
# Metaflow end-to-end test  (mirrors spin test style)
# ---------------------------------------------------------------------------


_NEEDS_CORE_BATCH_PARAMS = pytest.mark.xfail(
    reason="requires npow/core-deployer-changes: BATCH_CLIENT_PARAMS must be added "
    "to metaflow_config.py so METAFLOW_BATCH_CLIENT_PARAMS env var is recognized",
    strict=False,
)


@pytest.mark.docker
class TestMetaflowE2E:
    """
    Runs a real Metaflow flow against localbatch and verifies that artifacts
    produced inside the @batch step are correctly persisted and readable
    from the client side — the same contract the spin tests enforce.
    """

    @pytest.fixture(autouse=True)
    def _require_docker(self):
        try:
            import docker

            docker.from_env().ping()
        except Exception:
            pytest.skip("Docker not available")

    @_NEEDS_CORE_BATCH_PARAMS
    def test_batch_step_artifacts_are_persisted(self, simple_batch_run):
        """
        The @batch step writes message='hello from localbatch' and value=42.
        Both must be readable via the Metaflow client after the run finishes.
        """
        task = simple_batch_run["start"].task
        assert task["message"].data == "hello from localbatch"
        assert task["value"].data == 42

    @_NEEDS_CORE_BATCH_PARAMS
    def test_run_succeeds(self, simple_batch_run):
        assert simple_batch_run.successful

    @_NEEDS_CORE_BATCH_PARAMS
    def test_all_steps_have_tasks(self, simple_batch_run):
        step_names = {step.id for step in simple_batch_run.steps()}
        assert {"start", "end"} <= step_names
