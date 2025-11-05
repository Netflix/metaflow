import pytest
from metaflow import current
from .. import S3ROOT

# S3ROOT variants for testing both with and without trailing slash
S3ROOT_VARIANTS = [S3ROOT.rstrip("/"), S3ROOT if S3ROOT.endswith("/") else S3ROOT + "/"]


@pytest.fixture(params=S3ROOT_VARIANTS, ids=["no_slash", "with_slash"])
def s3root(request):
    """
    Fixture that provides S3ROOT with and without trailing slash.

    This ensures tests work correctly regardless of whether the s3root
    has a trailing slash or not.
    """
    return request.param


@pytest.fixture(autouse=True)
def reset_current_env():
    """
    Fixture to ensure the metaflow current environment is clean between tests.

    This prevents test pollution when tests manipulate the global current state.
    The fixture runs automatically for all tests in this directory.
    """
    # Setup: Save the current state (if any)
    saved_state = getattr(current, "_flow", None)

    yield

    # Teardown: Reset current to initial state
    # Clear any environment that was set during the test
    if hasattr(current, "_flow"):
        current._flow = saved_state
