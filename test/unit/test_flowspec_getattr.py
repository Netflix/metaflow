import os
import sys
import types

# Windows compatibility shim for unit test execution in non-POSIX environments
if sys.platform == "win32" and "fcntl" not in sys.modules:
    _fake_fcntl = types.ModuleType("fcntl")
    _fake_fcntl.F_SETFL = 0
    sys.modules["fcntl"] = _fake_fcntl
    if not hasattr(os, "O_NONBLOCK"):
        os.O_NONBLOCK = 0

import pytest
from metaflow import FlowSpec, step


class SampleFlow(FlowSpec):
    @property
    def broken_prop(self):
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


def test_flowspec_getattr_property_attribute_error():
    flow = SampleFlow(use_cli=False)
    with pytest.raises(AttributeError) as exc_info:
        _ = flow.broken_prop
    assert str(exc_info.value) == "original error inside property"


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
