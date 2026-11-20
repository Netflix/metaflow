import pytest

from metaflow.exception import MetaflowException
from metaflow.plugins.aws.aws_utils import parse_aws_tag, validate_aws_tag


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


@pytest.mark.parametrize(
    "key, value, expected_prefix",
    [
        ("#not-permitted", "ok", "Key *#not-permitted* is not permitted."),
        ("ok", "#not-permitted", "Value *#not-permitted* is not permitted."),
    ],
)
def test_validate_aws_tag_not_permitted_message(key, value, expected_prefix):
    with pytest.raises(MetaflowException) as exc_info:
        validate_aws_tag(key, value)

    assert str(exc_info.value).startswith(expected_prefix)


@pytest.mark.parametrize(
    "tag_str, expected_key, expected_val",
    [
        ("team=ml", "team", "ml"),
        ("query=env=prod", "query", "env=prod"),
        ("k=a=b=c", "k", "a=b=c"),
    ],
)
def test_parse_aws_tag(tag_str, expected_key, expected_val):
    key, val = parse_aws_tag(tag_str)
    assert key == expected_key
    assert val == expected_val


