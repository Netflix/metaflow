import importlib
import os
import shutil
import sys
from unittest import mock

import pytest

from metaflow import package_sources
from metaflow.exception import MetaflowException
from metaflow.packaging_sys import ContentType
from metaflow.plugins.package_sources import package_sources as package_sources_plugin


def _write(path, content=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _mutator(sources, flow_file, **kwargs):
    mutator = package_sources.__new__(package_sources)
    mutator._flow_cls = mock.Mock()
    mutator.init(sources, **kwargs)
    return mutator, mock.patch(
        "metaflow.plugins.package_sources.inspect.getfile",
        return_value=os.fspath(flow_file),
    )


def test_package_sources_is_available_at_top_level():
    assert package_sources is package_sources_plugin


def test_packages_sibling_source_relative_to_flow_file(tmp_path):
    flow_file = tmp_path / "flows" / "flow.py"
    source = tmp_path / "shared"
    _write(flow_file)
    _write(source / "__init__.py")
    _write(source / "helper.py")
    _write(source / "data.json")

    mutator, getfile = _mutator("../shared", flow_file)
    with getfile:
        results = list(mutator.add_to_package())

    assert {result[1] for result in results} == {
        os.path.join("shared", "__init__.py"),
        os.path.join("shared", "helper.py"),
    }
    assert all(result[2] == ContentType.USER_CONTENT for result in results)


def test_src_layout_package_is_importable_after_packaging(tmp_path, monkeypatch):
    flow_file = tmp_path / "flows" / "flow.py"
    source = tmp_path / "src" / "forecasting"
    package_root = tmp_path / "package"
    _write(flow_file)
    _write(source / "__init__.py", "VALUE = 'packaged'\n")

    mutator, getfile = _mutator("../src/forecasting", flow_file)
    with getfile:
        results = list(mutator.add_to_package())

    for file_path, archive_path, _ in results:
        destination = package_root / archive_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file_path, destination)

    monkeypatch.syspath_prepend(os.fspath(package_root))
    sys.modules.pop("forecasting", None)
    try:
        module = importlib.import_module("forecasting")
        assert module.VALUE == "packaged"
    finally:
        sys.modules.pop("forecasting", None)


def test_supports_multiple_sources_arcnames_and_suffixes(tmp_path):
    flow_file = tmp_path / "flows" / "flow.py"
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write(flow_file)
    _write(first / "helper.py")
    _write(first / "config.json")
    _write(second / "model.py")

    mutator, getfile = _mutator(
        [("../first", "vendor/first"), "../second"],
        flow_file,
        suffixes=[".py", ".json"],
    )
    with getfile:
        results = list(mutator.add_to_package())

    assert {result[1] for result in results} == {
        os.path.join("vendor", "first", "helper.py"),
        os.path.join("vendor", "first", "config.json"),
        os.path.join("second", "model.py"),
    }


@pytest.mark.parametrize("arcname", [".", "../outside", "nested/../outside", "/tmp"])
def test_rejects_unsafe_arcnames(arcname):
    with pytest.raises(MetaflowException, match="relative path"):
        package_sources._validate_arcname(arcname)


def test_rejects_arcname_with_multiple_sources(tmp_path):
    with pytest.raises(MetaflowException, match="single package source"):
        _mutator(["../first", "../second"], tmp_path / "flow.py", arcname="pkg")


def test_missing_source_raises(tmp_path):
    flow_file = tmp_path / "flows" / "flow.py"
    _write(flow_file)
    mutator, getfile = _mutator("../missing", flow_file)

    with getfile, pytest.raises(MetaflowException, match="source does not exist"):
        list(mutator.add_to_package())
