import pytest

from metaflow.exception import MetaflowTaggingError
from metaflow.tagging_util import (
    MAX_TAG_SIZE,
    MAX_USER_TAG_SET_SIZE,
    is_utf8_decodable,
    is_utf8_encodable,
    validate_tag,
    validate_tags,
)


def test_is_utf8_encodable_with_ascii_string():
    assert is_utf8_encodable("hello") is True


def test_is_utf8_encodable_with_unicode_string():
    assert is_utf8_encodable("hello \u00e9\u00e0\u00fc") is True


def test_is_utf8_encodable_with_empty_string():
    assert is_utf8_encodable("") is True


def test_is_utf8_encodable_with_non_string():
    with pytest.raises(AttributeError):
        is_utf8_encodable(123)


@pytest.mark.parametrize(
    "input_bytes",
    [
        b"hello",
        b"valid utf-8: \xc3\xa9",
        b"",
    ],
)
def test_is_utf8_decodable_with_valid_bytes(input_bytes):
    assert is_utf8_decodable(input_bytes) is True


def test_is_utf8_decodable_with_invalid_bytes():
    assert is_utf8_decodable(b"\xff\xfe") is False


def test_is_utf8_decodable_with_non_bytes():
    with pytest.raises(AttributeError):
        is_utf8_decodable("string")


class TestValidateTag:
    def test_valid_unicode_tag(self):
        validate_tag("valid-tag")

    def test_valid_bytes_tag(self):
        validate_tag(b"valid-bytes-tag")

    def test_valid_unicode_with_utf8_chars(self):
        validate_tag("caf\u00e9")

    def test_rejects_empty_string(self):
        with pytest.raises(MetaflowTaggingError, match="must not be empty"):
            validate_tag("")

    def test_rejects_empty_bytes(self):
        with pytest.raises(MetaflowTaggingError, match="must not be empty"):
            validate_tag(b"")

    def test_rejects_tag_too_long(self):
        long_tag = "a" * (MAX_TAG_SIZE + 1)
        with pytest.raises(MetaflowTaggingError, match="Tag is too long"):
            validate_tag(long_tag)

    def test_rejects_tag_too_long_bytes(self):
        long_tag = b"a" * (MAX_TAG_SIZE + 1)
        with pytest.raises(MetaflowTaggingError, match="Tag is too long"):
            validate_tag(long_tag)

    def test_rejects_non_string_type(self):
        with pytest.raises(MetaflowTaggingError, match="must be some kind of string"):
            validate_tag(123)

    def test_rejects_none_type(self):
        with pytest.raises(MetaflowTaggingError, match="must be some kind of string"):
            validate_tag(None)

    def test_rejects_list_type(self):
        with pytest.raises(MetaflowTaggingError, match="must be some kind of string"):
            validate_tag(["tag"])

    def test_tag_at_max_length_is_valid(self):
        validate_tag("a" * MAX_TAG_SIZE)


class TestValidateTags:
    def test_valid_single_tag(self):
        validate_tags(["valid-tag"])

    def test_valid_multiple_tags(self):
        validate_tags(["tag1", "tag2", "tag3"])

    def test_valid_empty_list(self):
        validate_tags([])

    def test_deduplicates_tags(self):
        validate_tags(["tag", "tag", "tag"])

    def test_validates_each_tag(self):
        with pytest.raises(MetaflowTaggingError, match="must not be empty"):
            validate_tags(["valid", "", "also-valid"])

    def test_rejects_too_many_tags(self):
        too_many = ["tag-%d" % i for i in range(MAX_USER_TAG_SET_SIZE + 1)]
        with pytest.raises(MetaflowTaggingError, match="Cannot increase size"):
            validate_tags(too_many)

    def test_allows_remediation_when_existing_tags_provided(self):
        large_set = ["tag-%d" % i for i in range(MAX_USER_TAG_SET_SIZE + 1)]
        validate_tags(large_set, existing_tags=large_set)

    def test_rejects_increase_even_with_existing(self):
        existing = ["tag-%d" % i for i in range(MAX_USER_TAG_SET_SIZE + 1)]
        larger = existing + ["new-tag"]
        with pytest.raises(MetaflowTaggingError, match="Cannot increase size"):
            validate_tags(larger, existing_tags=existing)

    def test_valid_bytes_tags(self):
        validate_tags([b"tag1", b"tag2"])

    def test_valid_mixed_unicode_and_bytes(self):
        validate_tags(["unicode-tag", b"bytes-tag"])
