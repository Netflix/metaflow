import os
import tempfile

from metaflow.packaging_sys.utils import walk


def _make_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("")


def test_walk_includes_files_when_hidden_dir_is_ancestor_of_root():
    """Regression: hidden ancestor dirs must not exclude user files."""
    with tempfile.TemporaryDirectory() as base:
        root = os.path.join(base, ".hidden_parent", "project", "flows")
        os.makedirs(root)
        _make_file(os.path.join(root, "hello_flow.py"))

        results = {rel for _, rel in walk(root, exclude_hidden=True)}
        assert any(
            "hello_flow.py" in r for r in results
        ), f"Expected hello_flow.py in walk results, got: {results}"


def test_walk_excludes_known_tool_dirs_under_root():
    """Well-known tool directories under root (.git, __pycache__, etc.) must be excluded."""
    with tempfile.TemporaryDirectory() as base:
        root = os.path.join(base, "project")
        os.makedirs(root)
        _make_file(os.path.join(root, "visible.py"))
        _make_file(os.path.join(root, ".git", "config"))
        _make_file(os.path.join(root, "__pycache__", "flow.cpython-312.pyc"))
        _make_file(os.path.join(root, ".tox", "py312", "bin", "activate"))

        results = {rel for _, rel in walk(root, exclude_hidden=True)}
        assert any("visible.py" in r for r in results)
        assert not any(
            "config" in r for r in results
        ), f".git/config should be excluded, got: {results}"
        assert not any(
            ".pyc" in r for r in results
        ), f"__pycache__ files should be excluded, got: {results}"
        assert not any(
            "activate" in r for r in results
        ), f".tox files should be excluded, got: {results}"


def test_walk_includes_dot_dirs_not_in_skip_list():
    """Dot-directories not in the skip list (e.g. .buildtool) must be included.

    Regression test for https://github.com/Netflix/metaflow/issues/1788.

    Build systems like Pants/Bazel/Buck commonly sandbox execution under
    directories like .buildtool, .pants.d, or .cache. Metaflow previously
    excluded any path component starting with '.', which silently dropped
    user code living under such directories. Only well-known tool directories
    (VCS dirs, caches listed in DIRS_TO_SKIP) should be excluded.
    """
    with tempfile.TemporaryDirectory() as base:
        root = os.path.join(base, "project")
        os.makedirs(root)
        _make_file(os.path.join(root, "flow.py"))
        _make_file(os.path.join(root, ".buildtool", "helper.py"))
        _make_file(os.path.join(root, ".pants.d", "runs", "flow.py"))

        results = {rel for _, rel in walk(root, exclude_hidden=True)}
        assert any("flow.py" in r for r in results)
        assert any(
            "helper.py" in r for r in results
        ), f".buildtool/helper.py should be included, got: {results}"
        assert any(
            os.path.join(".pants.d", "runs", "flow.py") in r for r in results
        ), f".pants.d/runs/flow.py should be included, got: {results}"
