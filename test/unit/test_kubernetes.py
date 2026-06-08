import pytest

from metaflow.plugins.kubernetes.kube_utils import (
    KubernetesException,
    validate_kube_labels,
    parse_kube_keyvalue_list,
)


# ---------------------------------------------------------------------------
# validate_kube_labels
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "labels",
    [
        None,
        {"label": "value"},
        {"label1": "val1", "label2": "val2"},
        {"label1": "val1", "label2": None},
        {"label": "a"},
        {"label": ""},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )  # 63 characters (max valid length)
        },
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234-_.890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )
        },
    ],
    ids=[
        "none",
        "single_label",
        "multiple_labels",
        "none_value",
        "single_char",
        "empty_string",
        "max_length_63_chars",
        "max_length_with_allowed_special_chars",
    ],
)
def test_validate_kube_labels_accepts_valid_inputs(labels):
    assert validate_kube_labels(labels)


@pytest.mark.parametrize(
    "labels",
    [
        {"label": "a-"},
        {"label": ".a"},
        {"label": "test()"},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234"
            )  # 64 characters (exceeds max length)
        },
        {"label": "(){}??"},
        {"valid": "test", "invalid": "bißchen"},
    ],
    ids=[
        "ends_with_hyphen",
        "starts_with_dot",
        "invalid_chars_parentheses",
        "exceeds_max_length_64_chars",
        "only_invalid_chars",
        "invalid_unicode_chars",
    ],
)
def test_validate_kube_labels_rejects_invalid_inputs(labels):
    """Fail if label contains invalid characters or is too long"""
    with pytest.raises(KubernetesException):
        validate_kube_labels(labels)


# ---------------------------------------------------------------------------
# parse_kube_keyvalue_list
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "items,requires_both,expected",
    [
        (["key=value"], True, {"key": "value"}),
        (["key=value"], False, {"key": "value"}),
        (["key"], False, {"key": None}),
        (["key=value", "key2=value2"], True, {"key": "value", "key2": "value2"}),
    ],
    ids=[
        "single_kv_requires_both",
        "single_kv_optional_both",
        "key_only_optional_both",
        "multiple_kv_requires_both",
    ],
)
def test_parse_kube_keyvalue_list_success(items, requires_both, expected):
    ret = parse_kube_keyvalue_list(items, requires_both)
    assert ret == expected


@pytest.mark.parametrize(
    "items,requires_both",
    [
        (["key=value", "key=value2"], True),
        (["key"], True),
    ],
    ids=[
        "duplicate_keys_not_allowed",
        "missing_value_when_requires_both",
    ],
)
def test_parse_kube_keyvalue_list_raises_exception(items, requires_both):
    with pytest.raises(KubernetesException):
        parse_kube_keyvalue_list(items, requires_both)
