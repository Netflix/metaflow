import pytest
from metaflow.exception import MetaflowTaggingError
from metaflow.tagging_util import (
    validate_tag,
    validate_tags,
    MAX_TAG_SIZE,
    MAX_USER_TAG_SET_SIZE,
)


def test_validate_tag_valid():
    validate_tag("valid_tag")
    validate_tag("a" * MAX_TAG_SIZE)
    validate_tag(b"valid_bytes_tag")


@pytest.mark.parametrize("invalid_tag, expected_type", [
    (123, "<class 'int'>"),
    (None, "<class 'NoneType'>"),
    (12.34, "<class 'float'>"),
    ([], "<class 'list'>"),
    ({}, "<class 'dict'>"),
])
def test_validate_tag_non_string_type_error_message(invalid_tag, expected_type):
    with pytest.raises(MetaflowTaggingError) as exc_info:
        validate_tag(invalid_tag)

    err = exc_info.value
    err_str = str(err)
    assert f"Tags must be some kind of string (bytes or unicode), got {expected_type}" in err_str
    assert err.line_no is None


def test_validate_tag_empty():
    with pytest.raises(MetaflowTaggingError, match="Tags must not be empty string"):
        validate_tag("")


def test_validate_tag_too_long():
    with pytest.raises(MetaflowTaggingError, match="Tag is too long"):
        validate_tag("a" * (MAX_TAG_SIZE + 1))


def test_validate_tags_set_size():
    valid_tags = [f"tag_{i}" for i in range(MAX_USER_TAG_SET_SIZE)]
    validate_tags(valid_tags)

    too_many_tags = [f"tag_{i}" for i in range(MAX_USER_TAG_SET_SIZE + 1)]
    with pytest.raises(MetaflowTaggingError, match="Cannot increase size of tag set beyond"):
        validate_tags(too_many_tags)
