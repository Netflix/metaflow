import pytest

from metaflow.plugins.argo.argo_workflows_cli import (
    sanitize_for_argo,
    sanitize_for_argo_v2,
)


def test_sanitize_for_argo():
    # cover collisions for legacy sanitize. This is an existing 'bug' we want to keep for legacy name resolution
    assert sanitize_for_argo("Test@+_123") == sanitize_for_argo("test123")

    # should not sanitize a valid string
    assert sanitize_for_argo("valid-name-123") == "valid-name-123"


def test_sanitize_for_argo_v2():
    a = sanitize_for_argo_v2("Test@+_123")
    b = sanitize_for_argo_v2("test123")
    assert a != b

    # check that sanitized length is correct (same as input)
    assert len(a) == len("Test@+_123")

    assert sanitize_for_argo_v2("Test") != sanitize_for_argo_v2("test")

    # should not sanitize a valid string
    assert sanitize_for_argo_v2("valid-string-123") == "valid-string-123"
