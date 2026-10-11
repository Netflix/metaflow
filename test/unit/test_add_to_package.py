"""Tests for _add_addl_files in MetaflowPackage.

Exercises the add_to_package hooks on FlowDecorators, FlowMutators,
StepDecorators, and StepMutators, as well as deduplication and error handling.
"""

import os
import sys
import tempfile
from types import ModuleType
from unittest import mock
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


def _build_pkg(flow, environment, mfcontent):
    """Build a bare MetaflowPackage instance with minimal state."""
    pkg = object.__new__(MetaflowPackage)
    pkg._flow = flow
    pkg._environment = environment
    pkg._mfcontent = mfcontent
    pkg._user_content_from_addl = {}
    return pkg


def _call_add_addl_files(flow, environment, mfcontent):
    """Call _add_addl_files on a bare MetaflowPackage instance."""
    pkg = _build_pkg(flow, environment, mfcontent)
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


def test_user_content_recorded():
    """USER_CONTENT tuples are recorded in _user_content_from_addl (not sent
    to _mfcontent which handles code/other files only)."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        deco = _make_deco([(f.name, "extra.py", ContentType.USER_CONTENT)])
        flow = _make_flow(
            steps=[_make_step()],
            flow_decorators={"my_deco": [deco]},
        )
        mfcontent = _make_mfcontent()
        pkg = _build_pkg(flow, _make_environment(), mfcontent)
        pkg._add_addl_files()

        # Not routed to _mfcontent — USER_CONTENT is packaged alongside user code.
        mfcontent.add_code_file.assert_not_called()
        mfcontent.add_other_file.assert_not_called()
        mfcontent.add_module.assert_not_called()

        assert pkg._user_content_from_addl == {"extra.py": os.path.realpath(f.name)}


def test_user_content_duplicate_same_path_dedup():
    """Same USER_CONTENT arcname with the same path from two decorators: dedup."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        d1 = _make_deco([(f.name, "shared.py", ContentType.USER_CONTENT)])
        d2 = _make_deco([(f.name, "shared.py", ContentType.USER_CONTENT)])
        flow = _make_flow(
            steps=[_make_step()],
            flow_decorators={"fd": [d1, d2]},
        )
        pkg = _build_pkg(flow, _make_environment(), _make_mfcontent())
        pkg._add_addl_files()
        assert pkg._user_content_from_addl == {"shared.py": os.path.realpath(f.name)}


def test_user_content_duplicate_different_path_raises():
    """Same USER_CONTENT arcname with different paths raises the usual exception."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f1, tempfile.NamedTemporaryFile(
        suffix=".py"
    ) as f2:
        d1 = _make_deco([(f1.name, "shared.py", ContentType.USER_CONTENT)])
        d2 = _make_deco([(f2.name, "shared.py", ContentType.USER_CONTENT)])
        flow = _make_flow(
            steps=[_make_step()],
            flow_decorators={"fd": [d1, d2]},
        )
        with pytest.raises(NonUniqueFileNameToFilePathMappingException):
            pkg = _build_pkg(flow, _make_environment(), _make_mfcontent())
            pkg._add_addl_files()


# ---------------------------------------------------------------------------
# _user_code_tuples — merge of USER_CONTENT with the flow-dir walker (DEF-010)
# ---------------------------------------------------------------------------


def _build_pkg_for_user_tuples(tmpdir, user_content_from_addl=None):
    """Build a minimal MetaflowPackage with just enough state for
    _user_code_tuples(): a flow dir, filter, exclude list, and the
    dict of USER_CONTENT files produced by add_to_package."""
    pkg = object.__new__(MetaflowPackage)
    pkg._user_code_filter = lambda _name: True
    pkg._exclude_tl_dirs = []
    pkg._user_content_from_addl = user_content_from_addl or {}
    pkg._user_flow_dir = None
    return pkg


def _fake_flow_dir(tmpdir, *relpaths):
    """Create `flow.py` plus the given relative paths in tmpdir. Returns the
    absolute flow.py path."""
    flow_file = os.path.join(tmpdir, "flow.py")
    with open(flow_file, "w") as f:
        f.write("# flow\n")
    for rel in relpaths:
        p = os.path.join(tmpdir, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write("x\n")
    return flow_file


def test_user_code_tuples_emits_addl_user_content_not_in_walker():
    """A USER_CONTENT file outside the walker's output gets emitted."""
    with tempfile.TemporaryDirectory() as flow_dir, tempfile.NamedTemporaryFile(
        suffix=".cfg", delete=False
    ) as external:
        try:
            _fake_flow_dir(flow_dir, "code.py")
            pkg = _build_pkg_for_user_tuples(
                flow_dir,
                user_content_from_addl={"extra.cfg": external.name},
            )
            with mock.patch.object(
                sys, "argv", [os.path.join(flow_dir, "flow.py")]
            ), mock.patch("metaflow.R.use_r", return_value=False):
                tuples = list(pkg._user_code_tuples())
            by_arc = {arc: path for path, arc in tuples}
            # walker picked up code.py and flow.py from the flow dir
            assert "code.py" in by_arc
            assert "flow.py" in by_arc
            # external USER_CONTENT was emitted as well
            assert by_arc["extra.cfg"] == external.name
        finally:
            os.unlink(external.name)


def test_user_code_tuples_skips_addl_when_walker_already_has_it():
    """USER_CONTENT with same arcname as a walker-yielded file is dropped."""
    with tempfile.TemporaryDirectory() as flow_dir:
        _fake_flow_dir(flow_dir, "code.py")
        walker_path = os.path.join(flow_dir, "code.py")
        # A *different* absolute path but same arcname. The walker wins.
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as shadow:
            try:
                pkg = _build_pkg_for_user_tuples(
                    flow_dir,
                    user_content_from_addl={"code.py": shadow.name},
                )
                with mock.patch.object(
                    sys, "argv", [os.path.join(flow_dir, "flow.py")]
                ), mock.patch("metaflow.R.use_r", return_value=False):
                    tuples = list(pkg._user_code_tuples())
                # code.py appears exactly once, with the walker's path
                code_py = [t for t in tuples if t[1] == "code.py"]
                assert len(code_py) == 1
                assert code_py[0][0] == walker_path
                # shadow is never emitted
                assert not any(t[0] == shadow.name for t in tuples)
            finally:
                os.unlink(shadow.name)


def test_user_code_tuples_respects_user_code_filter():
    """USER_CONTENT bypasses the suffix/user filter applied to the walker.

    The walker's filter excludes .yaml, but a USER_CONTENT tuple with a .yaml
    arcname must still be emitted — this is a primary reason USER_CONTENT
    exists.
    """
    with tempfile.TemporaryDirectory() as flow_dir:
        _fake_flow_dir(flow_dir, "conf.yaml")
        yaml_path = os.path.join(flow_dir, "conf.yaml")
        pkg = _build_pkg_for_user_tuples(
            flow_dir,
            user_content_from_addl={"conf.yaml": yaml_path},
        )
        # Restrict walker to .py only — .yaml should not come from the walker.
        pkg._user_code_filter = lambda fname: fname.lower().endswith(".py")
        with mock.patch.object(
            sys, "argv", [os.path.join(flow_dir, "flow.py")]
        ), mock.patch("metaflow.R.use_r", return_value=False):
            tuples = list(pkg._user_code_tuples())
        by_arc = {arc: path for path, arc in tuples}
        assert "conf.yaml" in by_arc
        assert by_arc["conf.yaml"] == yaml_path


# ---------------------------------------------------------------------------
# Integration: _add_addl_files + _user_code_tuples together (DEF-011)
# ---------------------------------------------------------------------------


def test_integration_add_addl_then_user_code_tuples_dedupes_by_arcname():
    """End-to-end: decorator emits USER_CONTENT for a file already in the
    flow dir; the final user tuples contain it only once with the walker's
    path.
    """
    with tempfile.TemporaryDirectory() as flow_dir:
        _fake_flow_dir(flow_dir, "code.py")
        walker_path = os.path.join(flow_dir, "code.py")

        deco = _make_deco([(walker_path, "code.py", ContentType.USER_CONTENT)])
        flow = _make_flow(
            steps=[_make_step()],
            flow_decorators={"fd": [deco]},
        )
        pkg = _build_pkg(flow, _make_environment(), _make_mfcontent())
        pkg._user_code_filter = lambda _: True
        pkg._exclude_tl_dirs = []
        pkg._user_flow_dir = None

        # Phase 1: populate _user_content_from_addl from add_to_package hooks.
        pkg._add_addl_files()
        assert pkg._user_content_from_addl == {"code.py": os.path.realpath(walker_path)}

        # Phase 2: walk the flow dir and merge addl USER_CONTENT.
        with mock.patch.object(
            sys, "argv", [os.path.join(flow_dir, "flow.py")]
        ), mock.patch("metaflow.R.use_r", return_value=False):
            tuples = list(pkg._user_code_tuples())

        # code.py is present exactly once, walker's copy wins (by arcname dedup).
        code_py = [t for t in tuples if t[1] == "code.py"]
        assert len(code_py) == 1


def test_integration_add_addl_contributes_file_outside_flow_dir():
    """End-to-end: decorator emits USER_CONTENT for a file that is NOT in the
    flow dir; it ends up in the user tuples via the merge path.
    """
    with tempfile.TemporaryDirectory() as flow_dir, tempfile.NamedTemporaryFile(
        suffix=".cfg", delete=False
    ) as external:
        try:
            _fake_flow_dir(flow_dir)

            deco = _make_deco(
                [(external.name, "external.cfg", ContentType.USER_CONTENT)]
            )
            flow = _make_flow(
                steps=[_make_step()],
                flow_decorators={"fd": [deco]},
            )
            pkg = _build_pkg(flow, _make_environment(), _make_mfcontent())
            pkg._user_code_filter = lambda _: True
            pkg._exclude_tl_dirs = []
            pkg._user_flow_dir = None

            pkg._add_addl_files()
            with mock.patch.object(
                sys, "argv", [os.path.join(flow_dir, "flow.py")]
            ), mock.patch("metaflow.R.use_r", return_value=False):
                tuples = list(pkg._user_code_tuples())
            by_arc = {arc: path for path, arc in tuples}
            assert by_arc["external.cfg"] == os.path.realpath(external.name)
        finally:
            os.unlink(external.name)
