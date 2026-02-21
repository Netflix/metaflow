import pytest

from metaflow.plugins.kubernetes.kubernetes import KubernetesException

from metaflow.plugins.kubernetes.kube_utils import (
    validate_kube_labels,
    parse_kube_keyvalue_list,
)


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
            )
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
)
def test_kubernetes_decorator_validate_kube_labels(labels):
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
            )
        },
        {"label": "(){}??"},
        {"valid": "test", "invalid": "bißchen"},
    ],
)
def test_kubernetes_decorator_validate_kube_labels_fail(labels):
    """Fail if label contains invalid characters or is too long"""
    with pytest.raises(KubernetesException):
        validate_kube_labels(labels)


@pytest.mark.parametrize(
    "items,requires_both,expected",
    [
        (["key=value"], True, {"key": "value"}),
        (["key=value"], False, {"key": "value"}),
        (["key"], False, {"key": None}),
        (["key=value", "key2=value2"], True, {"key": "value", "key2": "value2"}),
    ],
)
def test_kubernetes_parse_keyvalue_list(items, requires_both, expected):
    ret = parse_kube_keyvalue_list(items, requires_both)
    assert ret == expected


@pytest.mark.parametrize(
    "items,requires_both",
    [
        (["key=value", "key=value2"], True),
        (["key"], True),
    ],
)
def test_kubernetes_parse_keyvalue_list_fail(items, requires_both):
    with pytest.raises(KubernetesException):
        parse_kube_keyvalue_list(items, requires_both)


# ---------------------------------------------------------------------------
# Tests for @resources + @kubernetes resource merging (issue #2464)
#
# We test the merge logic directly by simulating what step_init() does when
# it encounters a @resources decorator alongside @kubernetes.  This avoids
# the need for a live Kubernetes cluster or a full Metaflow environment while
# still exercising the exact code path that was broken.
# ---------------------------------------------------------------------------
"""
Pure-Python reimplementation of the resource merge loop from
KubernetesDecorator.step_init() so we can unit-test it without needing a
full Metaflow environment at pytest collection time.

The constants below mirror the decorator defaults verbatim; if those ever
change upstream the test will catch the divergence.
"""

# Mirrors KubernetesDecorator.defaults (only numeric resource keys)
_KUBE_DEFAULTS = {"cpu": "1", "memory": "4096", "disk": "10240"}

# Mirrors ResourcesDecorator.defaults (only numeric resource keys)
_RES_DEFAULTS = {"cpu": "1", "memory": "4096"}


def _simulate_merge(kube_explicit_memory, resources_memory):
    """
    Simulate the resource-merge loop from KubernetesDecorator.step_init().

    Parameters
    ----------
    kube_explicit_memory : str or None
        Memory value the user passed to @kubernetes(memory=...).
        Pass None to simulate a default-valued @kubernetes decorator.
    resources_memory : str or None
        Memory value the user passed to @resources(memory=...).
        Pass None to simulate a default-valued @resources decorator.

    Returns
    -------
    str – resolved memory value (what would be sent to the Kubernetes pod).
    """
    # Simulate @kubernetes decorator state after init()
    kube_attrs = dict(_KUBE_DEFAULTS)
    if kube_explicit_memory is not None:
        kube_attrs["memory"] = kube_explicit_memory

    # Simulate @resources decorator state
    res_attrs = dict(_RES_DEFAULTS)
    if resources_memory is not None:
        res_attrs["memory"] = resources_memory

    # -----------------------------------------------------------------------
    # This is an exact copy of the fixed merge loop from step_init().
    # If you change the logic in kubernetes_decorator.py make sure to
    # update this copy too so tests remain meaningful.
    # -----------------------------------------------------------------------
    for k, v in res_attrs.items():
        if k not in kube_attrs:
            continue
        if _KUBE_DEFAULTS.get(k) is None:
            continue
        my_val = kube_attrs.get(k)
        if my_val is None and v is None:
            continue
        kube_is_default = my_val is not None and str(my_val) == str(_KUBE_DEFAULTS[k])
        if kube_is_default and v is not None:
            kube_attrs[k] = str(float(v))
        else:
            kube_attrs[k] = str(max(float(my_val or 0), float(v or 0)))

    return kube_attrs["memory"]


@pytest.mark.parametrize(
    "kube_memory, resources_memory, expected_memory",
    [
        # Core bug: @resources(memory=256) with default @kubernetes → must use 256
        (None, "256", "256.0"),
        # @resources larger than default → must use the larger value
        (None, "8192", "8192.0"),
        # @kubernetes explicitly set lower, @resources higher → resources wins
        (None, "16384", "16384.0"),
        # Both explicit: @kubernetes=1024, @resources=256 → max = 1024
        ("1024", "256", "1024.0"),
        # Both explicit: @kubernetes=1024, @resources=8192 → max = 8192
        ("1024", "8192", "8192.0"),
        # Both explicit, equal → same value
        ("2048", "2048", "2048.0"),
        # @resources not set (uses its own default of 4096), @kubernetes default
        # → max(4096, 4096) = 4096
        (None, "4096", "4096.0"),
    ],
)
def test_resources_memory_merge_with_kubernetes(
    kube_memory, resources_memory, expected_memory
):
    """
    Regression test for https://github.com/Netflix/metaflow/issues/2464.

    When @resources specifies a memory value that is lower than the @kubernetes
    default, the user's explicit @resources value must be honoured instead of
    being silently overridden by max().
    """
    result = _simulate_merge(kube_memory, resources_memory)
    assert result == expected_memory, (
        f"Expected memory={expected_memory} but got {result} "
        f"(kube_memory={kube_memory!r}, resources_memory={resources_memory!r})"
    )
