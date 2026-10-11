import os
import sys

# Windows compatibility: Metaflow plugins may attempt to import fcntl on module load.
# Provide an isolated, temporary shim during import and immediately clean up so test
# collection never leaks an incomplete module into subsequent tests.
if sys.platform == "win32" and "fcntl" not in sys.modules:
    import types

    _orig_fcntl = sys.modules.get("fcntl")
    _had_nonblock = hasattr(os, "O_NONBLOCK")
    sys.modules["fcntl"] = types.ModuleType("fcntl")
    sys.modules["fcntl"].F_SETFL = 0
    if not _had_nonblock:
        os.O_NONBLOCK = 0
    try:
        from metaflow import FlowSpec, step
    finally:
        if _orig_fcntl is None:
            sys.modules.pop("fcntl", None)
        else:
            sys.modules["fcntl"] = _orig_fcntl
        if not _had_nonblock and hasattr(os, "O_NONBLOCK"):
            delattr(os, "O_NONBLOCK")
else:
    from metaflow import FlowSpec, step

import pytest


class SampleFlow(FlowSpec):
    getter_run_count = 0

    @property
    def broken_prop(self):
        SampleFlow.getter_run_count += 1
        raise AttributeError("original error inside property")

    @property
    def nested_missing_attr_prop(self):
        obj = {}
        return obj.missing_attr

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_flowspec_getattr_property_attribute_error_runs_once():
    SampleFlow.getter_run_count = 0
    flow = SampleFlow(use_cli=False)
    with pytest.raises(AttributeError) as exc_info:
        _ = flow.broken_prop
    assert str(exc_info.value) == "original error inside property"
    assert SampleFlow.getter_run_count == 1, (
        f"Property getter must run exactly once, ran {SampleFlow.getter_run_count} times"
    )


def test_flowspec_getattr_nested_property_attribute_error():
    flow = SampleFlow(use_cli=False)
    with pytest.raises(AttributeError) as exc_info:
        _ = flow.nested_missing_attr_prop
    assert "missing_attr" in str(exc_info.value)


def test_flowspec_getattr_missing_attribute_error():
    flow = SampleFlow(use_cli=False)
    with pytest.raises(AttributeError) as exc_info:
        _ = flow.non_existent_attr
    assert "Flow SampleFlow has no attribute 'non_existent_attr'" in str(exc_info.value)
