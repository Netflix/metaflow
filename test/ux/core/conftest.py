"""
Devstack-specific test configuration.

Sets the Metaflow profile to the local devstack config and configures
boto3 service-endpoint env vars so all AWS clients (S3/MinIO, Batch/localbatch,
SFN, DDB) hit their local emulators.

We use service-specific boto3 env vars instead of METAFLOW_S3_ENDPOINT_URL
because METAFLOW_S3_ENDPOINT_URL would be injected into k8s pods via
kubernetes.py, overriding the correct internal MinIO URL that k8s pods receive
from the minio-secret (minio.default.svc.cluster.local:9000).

Env vars are set in pytest_configure (before collection) so that metaflow's
module-level config is loaded with the correct profile on first import.
"""

import os

# Path to the devtools directory (devtools/.devtools/)
_DEVTOOLS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "devtools", ".devtools")
)


def _set_devstack_env():
    """Set all devstack environment variables, respecting any user overrides."""
    # Point Metaflow at the devstack config (devtools/.devtools/config_local.json)
    os.environ.setdefault("METAFLOW_HOME", _DEVTOOLS_DIR)
    os.environ.setdefault("METAFLOW_PROFILE", "local")

    # Set MinIO credentials explicitly so they take precedence over any real
    # ~/.aws/credentials file the developer might have.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "rootuser")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "rootpass123")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    # Service-specific boto3 endpoint overrides for the devstack.
    # These are picked up automatically by ALL boto3 clients (regardless of
    # whether they pass explicit client_params) without leaking into k8s pods.
    os.environ.setdefault("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    os.environ.setdefault("AWS_ENDPOINT_URL_BATCH", "http://localhost:8000")
    os.environ.setdefault("AWS_ENDPOINT_URL_SFN", "http://localhost:8082")
    os.environ.setdefault("AWS_ENDPOINT_URL_DYNAMODB", "http://localhost:8765")
    # EventBridge stub: handles the schedule() call from the SFN deployer.
    # The stub returns ResourceNotFoundException for DisableRule (ignored by
    # EventBridgeClient._disable) so that deploying unscheduled flows works.
    os.environ.setdefault("AWS_ENDPOINT_URL_EVENTBRIDGE", "http://localhost:7777")


def _setup_gcs_emulator():
    """Configure the GCS client factory to use anonymous credentials.

    When STORAGE_EMULATOR_HOST is set, the google-cloud-storage Client
    automatically uses anonymous credentials and routes requests to the
    emulator -- but only when no explicit credentials are passed.
    Metaflow's default GCP client provider calls google.auth.default()
    first, which fails without real GCP credentials. We monkey-patch
    the factory to return a plain Client() that auto-detects the emulator.
    """
    if not os.environ.get("STORAGE_EMULATOR_HOST"):
        return

    try:
        from google.cloud import storage
        from metaflow.plugins.gcp import gs_storage_client_factory as factory

        _emulator_client = storage.Client()

        def _get_emulator_client():
            return _emulator_client

        factory.get_gs_storage_client = _get_emulator_client
    except ImportError:
        pass


def pytest_configure(config):
    """
    Called early by pytest (before collection) so env vars are set before
    metaflow is imported at module level by the test files.
    """
    _set_devstack_env()
    _setup_gcs_emulator()
