"""Tests for the ``package_suffixes`` example FlowMutator."""

import os
import pytest

from metaflow.packaging_sys import ContentType
from metaflow.plugins.package_suffixes_mutator import package_suffixes


# ---------------------------------------------------------------------------
# Fixtures & Factories
# ---------------------------------------------------------------------------


@pytest.fixture
def setup_flow_dir(tmp_path):
    """Fixture to write a minimal flow.py and a few sibling files.
    Returns the tuple (flow_file_path, flow_directory_path).
    """
    flow_file = tmp_path / "flow.py"
    flow_file.write_text("# dummy flow\n")

    (tmp_path / "config.yaml").write_text("a: 1\n")
    (tmp_path / "data.json").write_text("{}\n")

    sub_dir = tmp_path / "sub"
    sub_dir.mkdir()
    (sub_dir / "nested.yaml").write_text("b: 2\n")

    (tmp_path / "ignored.txt").write_text("ignored\n")

    return flow_file, tmp_path


@pytest.fixture
def make_mutator(mocker):
    """Factory fixture to initialize the package_suffixes mutator with mock dependencies."""

    def _build(suffixes):
        m = package_suffixes.__new__(package_suffixes)
        # We only need _flow_cls for inspect.getfile(); mock that directly instead
        # of constructing a real class.
        m._flow_cls = mocker.Mock()
        m.init(suffixes)
        return m

    return _build


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_suffixes, expected",
    [
        ([".yaml", "json"], (".yaml", ".json")),
        (".yaml,json, .txt", (".yaml", ".json", ".txt")),
    ],
    ids=["list_format", "string_format"],
)
def test_init_normalizes_suffixes(make_mutator, input_suffixes, expected):
    """Test that init correctly parses and normalizes list and string suffix inputs."""
    m = make_mutator(input_suffixes)
    assert m._suffixes == expected


def test_add_to_package_yields_matching_files(mocker, setup_flow_dir, make_mutator):
    """Test that add_to_package recursively finds files matching the configured suffixes."""
    flow_file, flow_dir = setup_flow_dir
    m = make_mutator([".yaml", ".json"])

    mocker.patch("inspect.getfile", return_value=str(flow_file))
    results = list(m.add_to_package())

    # All tuples are USER_CONTENT
    assert all(t[2] == ContentType.USER_CONTENT for t in results)

    arcnames = {t[1] for t in results}

    # walk() yields arcnames relative to the flow directory (no flow dir
    # basename prefix), matching the convention used by _user_code_tuples.
    assert "config.yaml" in arcnames
    assert "data.json" in arcnames
    assert os.path.join("sub", "nested.yaml") in arcnames

    # Non-matching files are not included.
    assert "ignored.txt" not in arcnames
    # flow.py is a .py file — not part of the configured extra suffixes.
    assert "flow.py" not in arcnames


def test_add_to_package_yields_nothing_when_suffixes_empty(
    mocker, setup_flow_dir, make_mutator
):
    """Test that an empty suffix list results in no files being yielded."""
    flow_file, flow_dir = setup_flow_dir
    m = make_mutator([])

    mocker.patch("inspect.getfile", return_value=str(flow_file))

    assert list(m.add_to_package()) == []
