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
    # Setup: Save all private attributes that might be set by current._set_env
    saved_state = {
        attr: getattr(current, attr, None)
        for attr in dir(current)
        if attr.startswith("_") and not attr.startswith("__")
    }

    yield

    # Teardown: Clear all current environment attributes
    # First, remove any new attributes that were added
    for attr in dir(current):
        if (
            attr.startswith("_")
            and not attr.startswith("__")
            and attr not in saved_state
        ):
            try:
                delattr(current, attr)
            except AttributeError:
                pass  # Some attributes may be read-only

    # Then restore original values
    for attr, value in saved_state.items():
        try:
            if value is None:
                # Remove attribute if it didn't exist before
                if hasattr(current, attr):
                    delattr(current, attr)
            else:
                setattr(current, attr, value)
        except AttributeError:
            pass  # Some attributes may be read-only
