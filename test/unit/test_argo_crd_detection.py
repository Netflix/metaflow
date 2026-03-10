import json

import pytest

from metaflow.plugins.argo.argo_client import ArgoClientException, _check_crd_exists


class MockApiException:
    """Mimics kubernetes.client.rest.ApiException."""

    def __init__(self, status, body=None):
        self.status = status
        self.body = body


def test_crd_missing_generic_404():
    """A generic 404 (no specific resource name) should raise a CRD error."""
    body = json.dumps(
        {
            "kind": "Status",
            "message": "the server could not find the requested resource",
            "reason": "NotFound",
        }
    )
    exc = MockApiException(404, body)
    with pytest.raises(ArgoClientException, match="CRDs do not appear to be installed"):
        _check_crd_exists(None, exc, "workflowtemplates")


def test_named_resource_not_found():
    """A 404 for a specific named resource should NOT raise the CRD error."""
    body = json.dumps(
        {
            "kind": "Status",
            "message": 'workflowtemplates "my-workflow" not found',
            "reason": "NotFound",
        }
    )
    exc = MockApiException(404, body)
    # Should not raise — this is a normal "resource not found" 404
    _check_crd_exists(None, exc, "workflowtemplates")


def test_non_404_ignored():
    """Non-404 errors should be ignored by _check_crd_exists."""
    body = json.dumps({"message": "forbidden"})
    exc = MockApiException(403, body)
    # Should not raise
    _check_crd_exists(None, exc, "workflowtemplates")


def test_crd_missing_empty_body():
    """A 404 with no body should not crash."""
    exc = MockApiException(404, None)
    # Should not raise (can't determine if CRD is missing without body)
    _check_crd_exists(None, exc, "workflowtemplates")
