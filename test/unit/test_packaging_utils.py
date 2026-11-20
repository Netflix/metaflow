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


def test_walk_excludes_hidden_dirs_under_root():
    """Hidden directories *under* root should still be excluded."""
    with tempfile.TemporaryDirectory() as base:
        root = os.path.join(base, ".hidden_parent", "project")
        os.makedirs(root)
        _make_file(os.path.join(root, "visible.py"))
        _make_file(os.path.join(root, ".secret", "hidden.py"))

        results = {rel for _, rel in walk(root, exclude_hidden=True)}
        assert any("visible.py" in r for r in results)
        assert not any(
            "hidden.py" in r for r in results
        ), f"hidden.py should be excluded, got: {results}"


def test_walk_exclude_tl_dirs_only_excludes_at_top_level():
    """Ensure exclude_tl_dirs only excludes directories at the top level, not at nested depths."""
    with tempfile.TemporaryDirectory() as base:
        root = os.path.join(base, "project")
        os.makedirs(root)
        _make_file(os.path.join(root, "data", "big.csv"))
        _make_file(os.path.join(root, "mypkg", "data", "loader.py"))
        _make_file(os.path.join(root, "mypkg", "__init__.py"))

        results = {rel.replace("\\", "/") for _, rel in walk(root, exclude_tl_dirs=["data"])}
        assert not any(
            "big.csv" in r for r in results
        ), f"top-level data/big.csv should be excluded, got: {results}"
        assert any(
            "loader.py" in r for r in results
        ), f"nested mypkg/data/loader.py should be included, got: {results}"
        assert any(
            "__init__.py" in r for r in results
        ), f"mypkg/__init__.py should be included, got: {results}"
