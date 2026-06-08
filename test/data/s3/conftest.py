import pytest
from metaflow import current
from .. import S3ROOT

# S3ROOT variants for testing both with and without trailing slash
# Handle case where S3ROOT is None (for unit tests that don't need S3 access)
if S3ROOT:
    S3ROOT_VARIANTS = [
        S3ROOT.rstrip("/"),
        S3ROOT if S3ROOT.endswith("/") else S3ROOT + "/",
    ]
else:
    S3ROOT_VARIANTS = [None]


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
    """
    # Setup: Capture internal state
    saved_state = {
        attr: getattr(current, attr, None)
        for attr in dir(current)
        if attr.startswith("_") and not attr.startswith("__")
    }

    yield

    # Teardown: Restore original attributes and clean up new ones
    current_attrs = [
        attr
        for attr in dir(current)
        if attr.startswith("_") and not attr.startswith("__")
    ]

    for attr in current_attrs:
        try:
            if attr not in saved_state:
                # Remove attributes created during the test
                delattr(current, attr)
            else:
                # Restore original values
                setattr(current, attr, saved_state[attr])
        except (AttributeError, TypeError):
            # Some internal attributes in 'current' may be read-only or immutable
            pass
