"""Tests for _add_addl_files in MetaflowPackage.

Exercises the add_to_package hooks on FlowDecorators, FlowMutators,
StepDecorators, and StepMutators, as well as deduplication and error handling.
"""

import os
import sys
import pytest

from metaflow.package import (
    MetaflowPackage,
    NonUniqueFileNameToFilePathMappingException,
)
from metaflow.packaging_sys import ContentType


# ---------------------------------------------------------------------------
# Fixtures & Factories
# ---------------------------------------------------------------------------


@pytest.fixture
def make_step(mocker):
    """Factory fixture to create a mocked flow step."""

    def _make(decorators=None, config_decorators=None):
        step = mocker.MagicMock()
        step.decorators = decorators or []
        step.config_decorators = config_decorators or []
        return step

    return _make


@pytest.fixture
def make_flow(mocker):
    """Factory fixture to create a mocked flow with steps."""

    def _make(steps, flow_decorators=None, flow_mutators=None):
        flow = mocker.MagicMock()
        # The flow may be iterated multiple times, so return a fresh iterator
        flow.__iter__ = lambda self: iter(steps)
        flow._flow_decorators = flow_decorators or {}
        flow._flow_mutators = flow_mutators or []
        return flow

    return _make


@pytest.fixture
def make_environment(mocker):
    """Factory fixture to create a mocked environment."""

    def _make(tuples=None):
        env = mocker.MagicMock()
        env.add_to_package.return_value = tuples or []
        return env

    return _make


@pytest.fixture
def make_deco(mocker):
    """Factory fixture to create a mocked decorator with add_to_package."""

    def _make(tuples):
        deco = mocker.MagicMock()
        deco.add_to_package.return_value = tuples
        return deco

    return _make


@pytest.fixture
def mfcontent(mocker):
    """Fixture to provide a fresh mfcontent mock per test."""
    return mocker.MagicMock()


@pytest.fixture
def build_pkg(mocker):
    """Factory fixture to build a minimal MetaflowPackage instance."""

    def _build(
        flow=None,
        environment=None,
        mfcontent_mock=None,
        user_content=None,
        flow_dir=None,
    ):
        pkg = object.__new__(MetaflowPackage)
        pkg._flow = flow
        pkg._environment = environment
        pkg._mfcontent = mfcontent_mock
        pkg._user_content_from_addl = user_content or {}

        # State necessary for _user_code_tuples
        pkg._user_code_filter = lambda _name: True
        pkg._exclude_tl_dirs = []
        pkg._user_flow_dir = str(flow_dir) if flow_dir else None
        return pkg

    return _build


@pytest.fixture
def setup_flow_dir(tmp_path):
    """Creates a fake flow directory structure for walker testing."""

    def _setup(*relpaths):
        flow_file = tmp_path / "flow.py"
        flow_file.write_text("# flow\n")

        for rel in relpaths:
            p = tmp_path / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("x\n")

        return flow_file, tmp_path

    return _setup


# ---------------------------------------------------------------------------
# Tests: _add_addl_files hooks & logic
# ---------------------------------------------------------------------------


def test_flow_decorator_files_are_added_to_code_content(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Flow decorator's add_to_package files are correctly routed to add_code_file."""
    target_file = tmp_path / "flow_deco_file.py"
    target_file.touch()

    deco = make_deco(
        [(str(target_file), "flow_deco_file.py", ContentType.CODE_CONTENT)]
    )
    flow = make_flow(steps=[make_step()], flow_decorators={"my_deco": [deco]})

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    mfcontent.add_code_file.assert_called_once_with(
        os.path.realpath(target_file), "flow_deco_file.py"
    )


def test_flow_mutator_module_content_calls_add_module(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg
):
    """Flow mutator's add_to_package with MODULE_CONTENT calls add_module."""
    import json

    mutator = make_deco([(json, None, ContentType.MODULE_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_mutators=[mutator])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    mfcontent.add_module.assert_called_once_with(json)


def test_step_mutator_deduplicates_same_instance_across_steps(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Same StepMutator instance on two steps: add_to_package called once."""
    target_file = tmp_path / "shared.py"
    target_file.touch()

    mutator = make_deco([(str(target_file), "shared.py", ContentType.CODE_CONTENT)])
    step1 = make_step(config_decorators=[mutator])
    step2 = make_step(config_decorators=[mutator])
    flow = make_flow(steps=[step1, step2])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    assert mutator.add_to_package.call_count == 1
    mfcontent.add_code_file.assert_called_once()


def test_step_mutator_adds_multiple_distinct_instances(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Two different StepMutator instances: both are called and added."""
    file1, file2 = tmp_path / "file1.py", tmp_path / "file2.py"
    file1.touch()
    file2.touch()

    m1 = make_deco([(str(file1), "file1.py", ContentType.CODE_CONTENT)])
    m2 = make_deco([(str(file2), "file2.py", ContentType.CODE_CONTENT)])
    step = make_step(config_decorators=[m1, m2])
    flow = make_flow(steps=[step])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    assert m1.add_to_package.call_count == 1
    assert m2.add_to_package.call_count == 1
    assert mfcontent.add_code_file.call_count == 2


def test_legacy_two_tuple_defaults_to_code_content(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """A 2-tuple (file_path, arcname) is gracefully treated as CODE_CONTENT."""
    legacy_file = tmp_path / "legacy.py"
    legacy_file.touch()

    deco = make_deco([(str(legacy_file), "legacy.py")])
    step = make_step(decorators=[deco])
    flow = make_flow(steps=[step])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    mfcontent.add_code_file.assert_called_once_with(
        os.path.realpath(legacy_file), "legacy.py"
    )


def test_non_unique_filename_to_arcname_raises_exception(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Different file paths targeting the same arcname raise a NonUniqueFileNameToFilePathMappingException."""
    file1, file2 = tmp_path / "f1.py", tmp_path / "f2.py"
    file1.touch()
    file2.touch()

    d1 = make_deco([(str(file1), "same_name.py", ContentType.CODE_CONTENT)])
    d2 = make_deco([(str(file2), "same_name.py", ContentType.CODE_CONTENT)])
    step = make_step(decorators=[d1, d2])
    flow = make_flow(steps=[step])

    pkg = build_pkg(flow, make_environment(), mfcontent)

    with pytest.raises(NonUniqueFileNameToFilePathMappingException):
        pkg._add_addl_files()


def test_module_content_deduplicates_same_module(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg
):
    """Same module returned by two decorators triggers add_module only once."""
    import json

    d1 = make_deco([(json, None, ContentType.MODULE_CONTENT)])
    d2 = make_deco([(json, None, ContentType.MODULE_CONTENT)])
    step = make_step(decorators=[d1, d2])
    flow = make_flow(steps=[step])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    mfcontent.add_module.assert_called_once_with(json)


def test_other_content_type_routes_to_add_other_file(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """OTHER_CONTENT files are correctly passed to add_other_file."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.touch()

    deco = make_deco([(str(yaml_file), "config.yaml", ContentType.OTHER_CONTENT)])
    step = make_step(decorators=[deco])
    flow = make_flow(steps=[step])

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    mfcontent.add_other_file.assert_called_once_with(
        os.path.realpath(yaml_file), "config.yaml"
    )


def test_flow_decorators_execute_before_step_decorators(
    mocker, make_step, make_flow, make_environment, mfcontent, build_pkg, tmp_path
):
    """Flow decorators must be processed before step decorators."""
    call_order = []

    def make_recording_deco(label, tuples):
        deco = mocker.MagicMock()

        def record():
            call_order.append(label)
            return tuples

        deco.add_to_package = record
        return deco

    f1, f2 = tmp_path / "f1.py", tmp_path / "f2.py"
    f1.touch()
    f2.touch()

    flow_deco = make_recording_deco(
        "flow_deco", [(str(f1), "flow_file.py", ContentType.CODE_CONTENT)]
    )
    step_deco = make_recording_deco(
        "step_deco", [(str(f2), "step_file.py", ContentType.CODE_CONTENT)]
    )

    step = make_step(decorators=[step_deco])
    flow = make_flow(steps=[step], flow_decorators={"fd": [flow_deco]})

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    assert call_order == ["flow_deco", "step_deco"]


def test_user_content_is_recorded_but_not_added_to_mfcontent(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """USER_CONTENT tuples are tracked internally, not sent to _mfcontent."""
    extra_file = tmp_path / "extra.py"
    extra_file.touch()

    deco = make_deco([(str(extra_file), "extra.py", ContentType.USER_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_decorators={"my_deco": [deco]})

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    # Verify USER_CONTENT is not routed directly to _mfcontent
    mfcontent.add_code_file.assert_not_called()
    mfcontent.add_other_file.assert_not_called()
    mfcontent.add_module.assert_not_called()

    assert pkg._user_content_from_addl == {"extra.py": os.path.realpath(extra_file)}


def test_user_content_duplicates_with_same_path_are_deduplicated(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Same USER_CONTENT arcname matching the same file path is safely deduplicated."""
    shared_file = tmp_path / "shared.py"
    shared_file.touch()

    d1 = make_deco([(str(shared_file), "shared.py", ContentType.USER_CONTENT)])
    d2 = make_deco([(str(shared_file), "shared.py", ContentType.USER_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_decorators={"fd": [d1, d2]})

    pkg = build_pkg(flow, make_environment(), mfcontent)
    pkg._add_addl_files()

    assert pkg._user_content_from_addl == {"shared.py": os.path.realpath(shared_file)}


def test_user_content_duplicate_arcnames_with_different_paths_raises(
    make_step, make_flow, make_environment, make_deco, mfcontent, build_pkg, tmp_path
):
    """Same USER_CONTENT arcname with different backing paths raises an exception."""
    f1, f2 = tmp_path / "f1.py", tmp_path / "f2.py"
    f1.touch()
    f2.touch()

    d1 = make_deco([(str(f1), "shared.py", ContentType.USER_CONTENT)])
    d2 = make_deco([(str(f2), "shared.py", ContentType.USER_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_decorators={"fd": [d1, d2]})

    pkg = build_pkg(flow, make_environment(), mfcontent)

    with pytest.raises(NonUniqueFileNameToFilePathMappingException):
        pkg._add_addl_files()


# ---------------------------------------------------------------------------
# Tests: _user_code_tuples merge logic
# ---------------------------------------------------------------------------


def test_user_code_tuples_emits_addl_user_content_not_in_walker(
    mocker, build_pkg, setup_flow_dir, tmp_path
):
    """A USER_CONTENT file outside the walker's normal output gets emitted properly."""
    external_file = tmp_path / "external" / "extra.cfg"
    external_file.parent.mkdir()
    external_file.touch()

    flow_file, flow_dir = setup_flow_dir("code.py")

    pkg = build_pkg(user_content={"extra.cfg": str(external_file)}, flow_dir=flow_dir)

    mocker.patch.object(sys, "argv", [str(flow_file)])
    mocker.patch("metaflow.R.use_r", return_value=False)

    tuples = list(pkg._user_code_tuples())
    by_arc = {arc: path for path, arc in tuples}

    assert "code.py" in by_arc
    assert "flow.py" in by_arc
    assert by_arc["extra.cfg"] == str(external_file)


def test_user_code_tuples_skips_addl_when_walker_already_has_it(
    mocker, build_pkg, setup_flow_dir, tmp_path
):
    """USER_CONTENT with the same arcname as a file yielded by the walker drops the duplicate."""
    shadow_file = tmp_path / "shadow" / "code.py"
    shadow_file.parent.mkdir()
    shadow_file.touch()

    flow_file, flow_dir = setup_flow_dir("code.py")
    walker_path = str(flow_dir / "code.py")

    pkg = build_pkg(user_content={"code.py": str(shadow_file)}, flow_dir=flow_dir)

    mocker.patch.object(sys, "argv", [str(flow_file)])
    mocker.patch("metaflow.R.use_r", return_value=False)

    tuples = list(pkg._user_code_tuples())

    code_py = [t for t in tuples if t[1] == "code.py"]
    assert len(code_py) == 1
    assert code_py[0][0] == walker_path
    assert not any(t[0] == str(shadow_file) for t in tuples)


def test_user_code_tuples_respects_user_code_filter(mocker, build_pkg, setup_flow_dir):
    """USER_CONTENT bypasses the suffix/user filter applied to the standard walker."""
    flow_file, flow_dir = setup_flow_dir("conf.yaml")
    yaml_path = str(flow_dir / "conf.yaml")

    pkg = build_pkg(user_content={"conf.yaml": yaml_path}, flow_dir=flow_dir)
    # Restrict walker strictly to .py files
    pkg._user_code_filter = lambda fname: fname.lower().endswith(".py")

    mocker.patch.object(sys, "argv", [str(flow_file)])
    mocker.patch("metaflow.R.use_r", return_value=False)

    tuples = list(pkg._user_code_tuples())
    by_arc = {arc: path for path, arc in tuples}

    assert "conf.yaml" in by_arc
    assert by_arc["conf.yaml"] == yaml_path


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


def test_integration_add_addl_then_user_code_tuples_dedupes_by_arcname(
    mocker,
    make_step,
    make_flow,
    make_environment,
    make_deco,
    mfcontent,
    build_pkg,
    setup_flow_dir,
):
    """End-to-end: Decorator emits USER_CONTENT for an existing flow file; deduplicated correctly."""
    flow_file, flow_dir = setup_flow_dir("code.py")
    walker_path = str(flow_dir / "code.py")

    deco = make_deco([(walker_path, "code.py", ContentType.USER_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_decorators={"fd": [deco]})

    pkg = build_pkg(flow, make_environment(), mfcontent, flow_dir=flow_dir)

    # Phase 1: Populate _user_content_from_addl
    pkg._add_addl_files()
    assert pkg._user_content_from_addl == {"code.py": os.path.realpath(walker_path)}

    # Phase 2: Walk the flow dir and merge
    mocker.patch.object(sys, "argv", [str(flow_file)])
    mocker.patch("metaflow.R.use_r", return_value=False)

    tuples = list(pkg._user_code_tuples())

    # code.py is present exactly once, walker's copy wins
    code_py = [t for t in tuples if t[1] == "code.py"]
    assert len(code_py) == 1


def test_integration_add_addl_contributes_file_outside_flow_dir(
    mocker,
    make_step,
    make_flow,
    make_environment,
    make_deco,
    mfcontent,
    build_pkg,
    setup_flow_dir,
    tmp_path,
):
    """End-to-end: Decorator emits USER_CONTENT for a file outside flow dir; merged successfully."""
    external_file = tmp_path / "external" / "external.cfg"
    external_file.parent.mkdir()
    external_file.touch()

    flow_file, flow_dir = setup_flow_dir()

    deco = make_deco([(str(external_file), "external.cfg", ContentType.USER_CONTENT)])
    flow = make_flow(steps=[make_step()], flow_decorators={"fd": [deco]})

    pkg = build_pkg(flow, make_environment(), mfcontent, flow_dir=flow_dir)

    pkg._add_addl_files()

    mocker.patch.object(sys, "argv", [str(flow_file)])
    mocker.patch("metaflow.R.use_r", return_value=False)

    tuples = list(pkg._user_code_tuples())
    by_arc = {arc: path for path, arc in tuples}

    assert by_arc["external.cfg"] == os.path.realpath(external_file)
