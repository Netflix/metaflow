"""
Tests for reserved parameter names (Issue #1753).

Verifies that formerly-reserved names are now free to use as Parameter names,
while still-reserved names continue to raise MetaflowException.
"""
import pytest

from metaflow.exception import MetaflowException
from metaflow.parameters import Parameter


def _make_parameter(name, **kwargs):
    """Helper: create a Parameter and call init() to trigger reservation check."""
    p = Parameter(name, default="test", **kwargs)
    # init() is where the reservation check happens; pass ignore_errors=True
    # to skip unrelated validation (e.g. type resolution against a real flow).
    p.init(ignore_errors=True)
    return p


# ---------------------------------------------------------------------------
# Names that WERE reserved and are now FREE
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name",
    [
        "tags",
        "decospecs",
        "run_id_file",
        "run-id-file",
        "max_num_splits",
        "max-num-splits",
        "max_workers",
        "max-workers",
        "max_log_size",
        "max-log-size",
        "user_namespace",
        "user-namespace",
        "run_id",
        "run-id",
        "task_id",
        "task-id",
        "runner_attribute_file",
        "runner-attribute-file",
    ],
)
def test_formerly_reserved_names_are_now_allowed(name):
    """Formerly-reserved names should no longer raise MetaflowException."""
    # Should not raise
    _make_parameter(name)


# ---------------------------------------------------------------------------
# Names that REMAIN reserved
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name",
    [
        "params",
        "with",
        "obj",
        "tag",
        "namespace",
    ],
)
def test_still_reserved_names_raise(name):
    """Still-reserved names must continue to raise MetaflowException."""
    with pytest.raises(MetaflowException, match="reserved"):
        _make_parameter(name)
