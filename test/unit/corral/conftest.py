"""
Fixtures for the corral integration tests.

The corral server is started once per session as an in-process uvicorn
thread, so no external daemon is required.  Tests that need Docker are
marked with @pytest.mark.docker and skipped automatically when Docker
is unavailable.

Custom marks
------------
docker : test requires a running Docker daemon.
"""


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "docker: mark test as requiring a running Docker daemon",
    )

import os
import threading
import time

import pytest
import requests

CORRAL_PORT = 18765
FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


# ---------------------------------------------------------------------------
# Availability probes
# ---------------------------------------------------------------------------


def _corral_available():
    try:
        from corral.server import create_app  # noqa: F401

        return True
    except ImportError:
        return False


def _docker_available():
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Custom markers / skip helpers
# ---------------------------------------------------------------------------

requires_corral = pytest.mark.skipif(
    not _corral_available(), reason="corral package not installed (pip install ~/code/corral)"
)
requires_docker = pytest.mark.skipif(
    not _docker_available(), reason="Docker daemon not reachable"
)


# ---------------------------------------------------------------------------
# Server fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def corral_server():
    """
    Start an in-process corral server and yield a small info object.
    The server is torn down after the test session.
    """
    if not _corral_available():
        pytest.skip("corral package not installed")

    import uvicorn
    from corral.runner import DockerRunner
    from corral.server import create_app
    from corral.store import Store

    store = Store(queue_name="corral-default")
    runner = DockerRunner(store, host_addr="host.docker.internal", port=CORRAL_PORT)
    app = create_app(store, runner)

    config = uvicorn.Config(app, host="127.0.0.1", port=CORRAL_PORT, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait up to 5 s for the server to accept connections.
    for _ in range(100):
        try:
            requests.get(f"http://127.0.0.1:{CORRAL_PORT}/health", timeout=0.1)
            break
        except Exception:
            time.sleep(0.05)
    else:
        pytest.fail("corral server did not start in time")

    class _Info:
        port = CORRAL_PORT
        base_url = f"http://127.0.0.1:{CORRAL_PORT}"

    yield _Info()

    server.should_exit = True
    thread.join(timeout=5)


# ---------------------------------------------------------------------------
# boto3 client fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def batch_client(corral_server):
    """Return a boto3 Batch client pointed at the local corral server."""
    import boto3

    return boto3.client(
        "batch",
        endpoint_url=corral_server.base_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


# ---------------------------------------------------------------------------
# Flow run fixture  (mirrors the spin conftest pattern)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def simple_batch_run(corral_server):
    """
    Run SimpleBatchFlow end-to-end against the local corral server.
    Skipped when Docker is unavailable because the @batch step executes
    inside a container.
    """
    if not _docker_available():
        pytest.skip("Docker not available — cannot run @batch step")

    from metaflow import Runner

    flow_path = os.path.join(FLOWS_DIR, "simple_batch_flow.py")

    env = {
        "METAFLOW_BATCH_JOB_QUEUE": "corral-default",
        "METAFLOW_BATCH_CLIENT_PARAMS": f'{{"endpoint_url":"{corral_server.base_url}"}}',
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
    }

    with Runner(flow_path, cwd=FLOWS_DIR, environment="local", env=env).run() as running:
        return running.run
