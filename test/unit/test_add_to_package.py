"""Tests for _add_addl_files in MetaflowPackage.

Exercises the add_to_package hooks on FlowDecorators, FlowMutators,
StepDecorators, and StepMutators, as well as deduplication and error handling.
"""

import os
import tempfile
from types import ModuleType
from unittest.mock import MagicMock, call

import pytest

from metaflow.package import (
    MetaflowPackage,
    NonUniqueFileNameToFilePathMappingException,
)
from metaflow.packaging_sys import ContentType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_step(decorators=None, config_decorators=None):
    step = MagicMock()
    step.decorators = decorators or []
    step.config_decorators = config_decorators or []
    return step


def _make_flow(steps, flow_decorators=None, flow_mutators=None):
    flow = MagicMock()
    # The flow may be iterated multiple times (step decos + step mutators),
    # so return a fresh iterator each time.
    flow.__iter__ = lambda self: iter(steps)
    flow._flow_decorators = flow_decorators or {}
    flow._flow_mutators = flow_mutators or []
    return flow


def _make_environment(tuples=None):
    env = MagicMock()
    env.add_to_package.return_value = tuples or []
    return env


def _make_mfcontent():
    return MagicMock()


def _make_deco(tuples):
    """Create a mock decorator-like object with an add_to_package method."""
    deco = MagicMock()
    deco.add_to_package.return_value = tuples
    return deco


def _call_add_addl_files(flow, environment, mfcontent):
    """Call _add_addl_files on a bare MetaflowPackage instance."""
    pkg = object.__new__(MetaflowPackage)
    pkg._flow = flow
    pkg._environment = environment
    pkg._mfcontent = mfcontent
    pkg._add_addl_files()
    return mfcontent


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_flow_decorator_add_to_package():
    """Flow decorator's add_to_package files are added."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        deco = _make_deco([(f.name, "flow_deco_file.py", ContentType.CODE_CONTENT)])
        flow = _make_flow(
            steps=[_make_step()],
            flow_decorators={"my_deco": [deco]},
        )
        mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        mc.add_code_file.assert_called_once_with(
            os.path.realpath(f.name), "flow_deco_file.py"
        )


def test_flow_mutator_add_to_package_module():
    """Flow mutator's add_to_package with MODULE_CONTENT calls add_module."""
    import json

    mutator = _make_deco([(json, None, ContentType.MODULE_CONTENT)])
    flow = _make_flow(steps=[_make_step()], flow_mutators=[mutator])
    mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
    mc.add_module.assert_called_once_with(json)


def test_step_mutator_deduplicated_across_steps():
    """Same StepMutator instance on two steps: add_to_package called once."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        mutator = _make_deco([(f.name, "shared.py", ContentType.CODE_CONTENT)])
        step1 = _make_step(config_decorators=[mutator])
        step2 = _make_step(config_decorators=[mutator])
        flow = _make_flow(steps=[step1, step2])
        mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        assert mutator.add_to_package.call_count == 1
        mc.add_code_file.assert_called_once()


def test_step_mutator_distinct_instances():
    """Two different StepMutator instances: both called."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f1, tempfile.NamedTemporaryFile(
        suffix=".py"
    ) as f2:
        m1 = _make_deco([(f1.name, "file1.py", ContentType.CODE_CONTENT)])
        m2 = _make_deco([(f2.name, "file2.py", ContentType.CODE_CONTENT)])
        step = _make_step(config_decorators=[m1, m2])
        flow = _make_flow(steps=[step])
        mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        assert m1.add_to_package.call_count == 1
        assert m2.add_to_package.call_count == 1
        assert mc.add_code_file.call_count == 2


def test_legacy_two_tuple_defaults_to_code_content():
    """A 2-tuple (file_path, arcname) is treated as CODE_CONTENT."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        deco = _make_deco([(f.name, "legacy.py")])
        step = _make_step(decorators=[deco])
        flow = _make_flow(steps=[step])
        mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        mc.add_code_file.assert_called_once_with(os.path.realpath(f.name), "legacy.py")


def test_non_unique_filename_raises():
    """Different file paths for the same arcname raises an exception."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f1, tempfile.NamedTemporaryFile(
        suffix=".py"
    ) as f2:
        d1 = _make_deco([(f1.name, "same_name.py", ContentType.CODE_CONTENT)])
        d2 = _make_deco([(f2.name, "same_name.py", ContentType.CODE_CONTENT)])
        step = _make_step(decorators=[d1, d2])
        flow = _make_flow(steps=[step])
        with pytest.raises(NonUniqueFileNameToFilePathMappingException):
            _call_add_addl_files(flow, _make_environment(), _make_mfcontent())


def test_module_content_deduplicated():
    """Same module returned by two decorators: add_module called once."""
    import json

    d1 = _make_deco([(json, None, ContentType.MODULE_CONTENT)])
    d2 = _make_deco([(json, None, ContentType.MODULE_CONTENT)])
    step = _make_step(decorators=[d1, d2])
    flow = _make_flow(steps=[step])
    mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
    mc.add_module.assert_called_once_with(json)


def test_other_content_type():
    """OTHER_CONTENT files are passed to add_other_file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as f:
        deco = _make_deco([(f.name, "config.yaml", ContentType.OTHER_CONTENT)])
        step = _make_step(decorators=[deco])
        flow = _make_flow(steps=[step])
        mc = _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        mc.add_other_file.assert_called_once_with(
            os.path.realpath(f.name), "config.yaml"
        )


def test_ordering_flow_decorators_before_step_decorators():
    """Flow decorators are processed before step decorators.

    We verify the flow decorator's add_to_package is called before the step
    decorator's by checking call order on a shared mock recorder.
    """
    call_order = []

    def make_recording_deco(label, tuples):
        deco = MagicMock()

        def record():
            call_order.append(label)
            return tuples

        deco.add_to_package = record
        return deco

    with tempfile.NamedTemporaryFile(suffix=".py") as f1, tempfile.NamedTemporaryFile(
        suffix=".py"
    ) as f2:
        flow_deco = make_recording_deco(
            "flow_deco", [(f1.name, "flow_file.py", ContentType.CODE_CONTENT)]
        )
        step_deco = make_recording_deco(
            "step_deco", [(f2.name, "step_file.py", ContentType.CODE_CONTENT)]
        )
        step = _make_step(decorators=[step_deco])
        flow = _make_flow(
            steps=[step],
            flow_decorators={"fd": [flow_deco]},
        )
        _call_add_addl_files(flow, _make_environment(), _make_mfcontent())
        assert call_order == ["flow_deco", "step_deco"]
