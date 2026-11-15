import pytest
from metaflow.exception import MetaflowTaggingError
from metaflow.tagging_util import (
    is_utf8_decodable,
    is_utf8_encodable,
    validate_tag,
    validate_tags,
    MAX_TAG_SIZE,
    MAX_USER_TAG_SET_SIZE,
)


def test_is_utf8_encodable():
    assert is_utf8_encodable("valid string") is True
    assert is_utf8_encodable("🚀") is True
    assert is_utf8_encodable("") is True
    # Lone surrogate cannot be encoded to UTF-8
    assert is_utf8_encodable("\ud800") is False


def test_is_utf8_decodable():
    assert is_utf8_decodable(b"valid bytes") is True
    assert is_utf8_decodable("🚀".encode("utf-8")) is True
    assert is_utf8_decodable(b"") is True
    # Invalid UTF-8 start byte
    assert is_utf8_decodable(b"\x80") is False
    assert is_utf8_decodable(b"\xff\xfe") is False


def test_validate_tag_valid():
    validate_tag("valid_tag")
    validate_tag("a" * MAX_TAG_SIZE)
    validate_tag(b"valid_bytes_tag")
    validate_tag(b"a" * MAX_TAG_SIZE)


@pytest.mark.parametrize(
    "invalid_tag, expected_type",
    [
        (123, "<class 'int'>"),
        (None, "<class 'NoneType'>"),
        (12.34, "<class 'float'>"),
        ([], "<class 'list'>"),
        ({}, "<class 'dict'>"),
        (True, "<class 'bool'>"),
        (set(), "<class 'set'>"),
        (object(), "<class 'object'>"),
    ],
)
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
    with pytest.raises(MetaflowTaggingError, match="Tags must not be empty string"):
        validate_tag(b"")


def test_validate_tag_too_long():
    with pytest.raises(MetaflowTaggingError, match=f"Tag is too long {MAX_TAG_SIZE + 1} > {MAX_TAG_SIZE}"):
        validate_tag("a" * (MAX_TAG_SIZE + 1))
    with pytest.raises(MetaflowTaggingError, match=f"Tag is too long {MAX_TAG_SIZE + 1} > {MAX_TAG_SIZE}"):
        validate_tag(b"a" * (MAX_TAG_SIZE + 1))


def test_validate_tag_unencodable_unicode():
    with pytest.raises(MetaflowTaggingError, match="Tags must be UTF-8 encodable"):
        validate_tag("\ud800")


def test_validate_tag_undecodable_bytes():
    with pytest.raises(MetaflowTaggingError, match="Tags must be UTF-8 decodable"):
        validate_tag(b"\x80invalid")


def test_validate_tags_valid():
    validate_tags(["tag1", "tag2", "tag3"])
    # Duplicate tags are deduplicated into a set
    validate_tags(["tag1", "tag1", "tag2"])
    # Accepts tuples and sets
    validate_tags(("tag1", "tag2"))
    validate_tags({"tag1", "tag2"})


def test_validate_tags_set_size_limit():
    valid_tags = [f"tag_{i}" for i in range(MAX_USER_TAG_SET_SIZE)]
    validate_tags(valid_tags)

    too_many_tags = [f"tag_{i}" for i in range(MAX_USER_TAG_SET_SIZE + 1)]
    with pytest.raises(
        MetaflowTaggingError,
        match=f"Cannot increase size of tag set beyond {MAX_USER_TAG_SET_SIZE}",
    ):
        validate_tags(too_many_tags)


def test_validate_tags_remediation():
    # If existing tags is already larger than MAX_USER_TAG_SET_SIZE,
    # mutating or maintaining the tag set without increasing its size is permitted.
    oversized_existing = [f"tag_{i}" for i in range(60)]
    same_size_new = [f"new_tag_{i}" for i in range(60)]
    smaller_new = [f"new_tag_{i}" for i in range(55)]
    larger_new = [f"new_tag_{i}" for i in range(61)]

    # Maintaining or reducing size does not raise
    validate_tags(same_size_new, existing_tags=oversized_existing)
    validate_tags(smaller_new, existing_tags=oversized_existing)

    # Increasing size raises error
    with pytest.raises(
        MetaflowTaggingError,
        match=f"Cannot increase size of tag set beyond {MAX_USER_TAG_SET_SIZE}",
    ):
        validate_tags(larger_new, existing_tags=oversized_existing)


def test_validate_tags_invalid_element():
    with pytest.raises(MetaflowTaggingError) as exc_info:
        validate_tags(["valid_tag", 12345])

    err_str = str(exc_info.value)
    assert "Tags must be some kind of string (bytes or unicode), got <class 'int'>" in err_str
