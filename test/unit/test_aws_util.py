import pytest

from metaflow.plugins.aws.aws_utils import validate_aws_tag


@pytest.mark.parametrize(
    "key, value, should_raise",
    [
        ("test", "value", False),
        ("test-with@chars+ - = ._/", "value@with.chars-+ - = ._/", False),
        (
            "not-too-long-key-value-" + "a" * 105,
            "ok",
            False,
        ),  # <=128 char key should work.
        ("too-long-key-value-" + "a" * 110, "ok", True),  # >128 char key should fail.
        (
            "ok",
            "not-too-long-value-" + "a" * 237,
            False,
        ),  # <=256 char value should work.
        ("ok", "too-long-value-" + "a" * 242, True),  # >256 char value should fail.
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
