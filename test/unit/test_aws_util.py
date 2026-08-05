import contextlib
import pytest

from metaflow.plugins.aws.aws_utils import validate_aws_tag


@pytest.mark.parametrize(
    "key, value, should_raise",
    [
        ("test", "value", False),
        ("test-with@chars+ - = ._/", "value@with.chars-+ - = ._/", False),
        ("a" * 128, "ok", False),  # <=128 char key should work
        ("a" * 129, "ok", True),  # >128 char key should fail
        ("ok", "a" * 256, False),  # <=256 char value should work
        ("ok", "a" * 257, True),  # >256 char value should fail
        ("aWs:not-allowed", "ok", True),  # 'aws:' prefix should not be allowed as key
        ("ok", "AWS:not-allowed", True),  # 'aws:' prefix should not be allowed as value
        ("ok-aws:", "middleaWs:not-allowed", False),  # 'aws:' substring itself is fine
    ],
    ids=[
        "simple-valid",
        "allowed-special-chars",
        "key-max-length-128",
        "key-length-exceeded-129",
        "value-max-length-256",
        "value-length-exceeded-257",
        "aws-prefix-key-rejected",
        "aws-prefix-value-rejected",
        "aws-substring-allowed",
    ],
)
def test_aws_tag_validation_rules(key, value, should_raise):
    """Verify AWS tag validation enforces character sets, length limits, and prefix restrictions."""
    expectation = pytest.raises(Exception) if should_raise else contextlib.nullcontext()

    with expectation:
        validate_aws_tag(key, value)
