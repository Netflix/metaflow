import pytest

from metaflow.plugins.aws.aws_utils import validate_aws_tag


@pytest.mark.parametrize(
    "key, value, should_raise",
    [
        ("test", "value", False),
        ("test-with@chars+ - = ._/", "value@with.chars-+ - = ._/", False),
        (
            "a" * 128,
            "ok",
            False,
        ),  # <=128 char key should work.
        ("a" * 129, "ok", True),  # >128 char key should fail.
        (
            "ok",
            "a" * 256,
            False,
        ),  # <=256 char value should work.
        ("ok", "a" * 257, True),  # >256 char value should fail.
        ("aWs:not-allowed", "ok", True),  # 'aws:' prefix should not be allowed as key
        ("ok", "AWS:not-allowed", True),  # 'aws:' prefix should not be allowed as value
        (
            "ok-aws:",
            "middleaWs:not-allowed",
            False,
        ),  # 'aws:' itself is not a restricted pattern
    ],
)
def test_validate_aws_tag(key, value, should_raise):
    did_raise = False
    try:
        validate_aws_tag(key, value)
    except Exception as e:
        did_raise = True

    assert did_raise == should_raise
