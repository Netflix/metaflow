"""
Tests for metaflow.packaging_sys.utils.walk

Verifies that the walk() function correctly handles hidden directories,
particularly when the walk root itself is nested under a hidden ancestor
directory (GitHub issue #2791).
"""

import os
import tempfile
import pytest

from metaflow.packaging_sys.utils import walk


@pytest.fixture
def tree_under_hidden_ancestor(tmp_path):
    """
    Creates a directory tree where the project root sits under a hidden
    ancestor directory:

        <tmp>/.hidden_ancestor/project/
            hello.py
            sub/
                util.py
            .secret/
                internal.py
    """
    hidden = tmp_path / ".hidden_ancestor" / "project"
    hidden.mkdir(parents=True)

    (hidden / "hello.py").write_text("print('hi')")
    (hidden / "sub").mkdir()
    (hidden / "sub" / "util.py").write_text("x = 1")
    (hidden / ".secret").mkdir()
    (hidden / ".secret" / "internal.py").write_text("secret = True")

    return str(hidden)


@pytest.fixture
def normal_tree(tmp_path):
    """
    Creates a normal directory tree (no hidden ancestors):

        <tmp>/project/
            main.py
            lib/
                helper.py
            .hidden_sub/
                private.py
    """
    root = tmp_path / "project"
    root.mkdir()

    (root / "main.py").write_text("print('main')")
    (root / "lib").mkdir()
    (root / "lib" / "helper.py").write_text("h = 1")
    (root / ".hidden_sub").mkdir()
    (root / ".hidden_sub" / "private.py").write_text("p = 1")

    return str(root)


class TestWalkHiddenAncestorBug:
    """Regression tests for GitHub issue #2791."""

    def test_files_found_under_hidden_ancestor(self, tree_under_hidden_ancestor):
        """The main bug: walk() must find files even when root is inside a hidden ancestor."""
        root = tree_under_hidden_ancestor + "/"
        results = [relpath for _, relpath in walk(root)]
        assert "hello.py" in results
        assert os.path.join("sub", "util.py") in results

    def test_hidden_subdir_excluded_under_hidden_ancestor(self, tree_under_hidden_ancestor):
        """Hidden subdirs *under* root must still be excluded, even when root is inside a hidden ancestor."""
        root = tree_under_hidden_ancestor + "/"
        results = [relpath for _, relpath in walk(root)]
        hidden_file = os.path.join(".secret", "internal.py")
        assert hidden_file not in results


class TestWalkHiddenSubdirectory:
    """Tests for excluding hidden subdirectories inside the project root."""

    def test_hidden_subdir_excluded_by_default(self, normal_tree):
        """Hidden subdirectories under root should be excluded when exclude_hidden=True (default)."""
        root = normal_tree + "/"
        results = [relpath for _, relpath in walk(root)]
        assert "main.py" in results
        assert os.path.join("lib", "helper.py") in results
        hidden_file = os.path.join(".hidden_sub", "private.py")
        assert hidden_file not in results

    def test_hidden_subdir_included_when_flag_off(self, normal_tree):
        """When exclude_hidden=False, hidden subdirectories should be included."""
        root = normal_tree + "/"
        results = [relpath for _, relpath in walk(root, exclude_hidden=False)]
        assert "main.py" in results
        hidden_file = os.path.join(".hidden_sub", "private.py")
        assert hidden_file in results


class TestWalkExcludeTopLevelDirs:
    """Tests for the exclude_tl_dirs parameter."""

    def test_exclude_top_level_dirs(self, normal_tree):
        """Top-level directories in exclude_tl_dirs should be skipped."""
        root = normal_tree + "/"
        results = [relpath for _, relpath in walk(root, exclude_tl_dirs=["lib"])]
        assert "main.py" in results
        assert os.path.join("lib", "helper.py") not in results


class TestWalkFileFilter:
    """Tests for the file_filter parameter."""

    def test_file_filter_applied(self, normal_tree):
        """Only files passing the filter should be yielded."""
        root = normal_tree + "/"
        results = [
            relpath
            for _, relpath in walk(root, file_filter=lambda f: f == "main.py")
        ]
        assert "main.py" in results
        assert os.path.join("lib", "helper.py") not in results
