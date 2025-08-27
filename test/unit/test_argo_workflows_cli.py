import pytest

from metaflow.plugins.argo.argo_workflows_cli import sanitize_for_argo


@pytest.mark.parametrize(
    "name, expected",
    [
        ("a-valid-name", "a-valid-name"),
        ("removing---@+_characters@_+", "removing---characters"),
        ("numb3rs-4r3-0k-123", "numb3rs-4r3-0k-123"),
        ("proj3ct.br4nch.flow_name", "proj3ct.br4nch.flowname"),
        (
            "---1breaking1---.--2subdomain2--.-3rules3-",
            "1breaking1.2subdomain2.3rules3",
        ),  # should not break RFC 1123 subdomain requirements
    ],
)
def test_sanitize_for_argo(name, expected):
    sanitized = sanitize_for_argo(name)
    assert sanitized == expected
