import pytest

from metaflow.exception import MetaflowException
from metaflow.plugins.aws.aws_utils import parse_s3_full_path, validate_aws_tag


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
    "s3_uri, expected",
    [
        ("s3://bucket/key", ("bucket", "key")),
        ("s3://bucket/a/b/c.txt", ("bucket", "a/b/c.txt")),
        ("s3://bucket/dir/", ("bucket", "dir")),
        ("s3://bucket//dir//", ("bucket", "dir")),
        ("s3://bucket/", ("bucket", None)),
        ("s3://bucket", ("bucket", None)),
        ("S3://bucket/key", ("bucket", "key")),
        ("s3://bucket/a key/b", ("bucket", "a key/b")),
        # "?" and "#" are valid in an S3 key, so they must not cut the key off.
        ("s3://bucket/key?version=1", ("bucket", "key?version=1")),
        ("s3://bucket/run#1/data", ("bucket", "run#1/data")),
        ("s3://bucket/key;v=2", ("bucket", "key;v=2")),
        ("s3://bucket/a?b#c;d", ("bucket", "a?b#c;d")),
    ],
)
def test_parse_s3_full_path(s3_uri, expected):
    assert parse_s3_full_path(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri", ["gs://bucket/key", "http://bucket/key", "bucket/key"]
)
def test_parse_s3_full_path_requires_s3_scheme(s3_uri):
    with pytest.raises(AssertionError):
        parse_s3_full_path(s3_uri)
