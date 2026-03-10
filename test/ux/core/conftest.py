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


def _set_azurite_env():
    """Set Azurite (Azure emulator) env vars when using the azure datastore.

    The well-known Azurite account key is used as AZURE_STORAGE_KEY so that
    Metaflow's AzureDefaultClientProvider can authenticate without
    DefaultAzureCredential (which doesn't support Azurite).
    """
    azurite_account = "devstoreaccount1"
    azurite_key = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
        "/K1SZFPTOtr/KBHBeksoGMGw=="
    )
    os.environ.setdefault("AZURE_STORAGE_KEY", azurite_key)
    os.environ.setdefault(
        "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT",
        "http://127.0.0.1:10000/" + azurite_account,
    )
    os.environ.setdefault(
        "METAFLOW_DATASTORE_SYSROOT_AZURE",
        "az://metaflow-test/metaflow",
    )
    conn_str = (
        "DefaultEndpointsProtocol=http"
        ";AccountName="
        + azurite_account
        + ";AccountKey="
        + azurite_key
        + ";BlobEndpoint=http://127.0.0.1:10000/"
        + azurite_account
        + ";"
    )
    os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", conn_str)


def pytest_configure(config):
    """
    Called early by pytest (before collection) so env vars are set before
    metaflow is imported at module level by the test files.
    """
    _set_devstack_env()

    # If the azure datastore is requested (via env var or --only-backend
    # azure-local), set Azurite credentials.
    if os.environ.get("METAFLOW_DEFAULT_DATASTORE") == "azure":
        _set_azurite_env()
