"""Unit coverage for card datastore root resolution (issue #2139).

``CardDatastore.get_storage_root`` decides where local/spin cards are written and
read. These tests pin the resolution order the fix guarantees:

  1. an explicit ``METAFLOW_CARD_LOCALROOT`` wins;
  2. otherwise cards sit under the resolved datastore root (``<root>/mf.cards``),
     so they stay next to the flow artifacts;
  3. otherwise fall back to the legacy nearest-parent ``.metaflow`` lookup, then
     to the current working directory.

Cloud backends (s3/azure/gs) must be unaffected. The end-to-end write/read
round-trip lives in ``test/core/test_card_local_storage.py``.
"""

import os

import pytest

from metaflow.metaflow_config import (
    CARD_SUFFIX,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SPIN_LOCAL_DIR,
)
from metaflow.plugins.cards import card_datastore
from metaflow.plugins.cards.card_datastore import CardDatastore


@pytest.fixture
def no_explicit_card_root(monkeypatch):
    monkeypatch.setattr(card_datastore, "CARD_LOCALROOT", None)


def test_local_card_root_honors_explicit_config(monkeypatch, tmp_path):
    # Priority 1: an explicit card root must win even when a datastore root is
    # also supplied. (Before the fix this returned None and was silently ignored.)
    explicit_card_root = tmp_path / "cards"
    datastore_root = tmp_path / "shared" / DATASTORE_LOCAL_DIR
    monkeypatch.setattr(card_datastore, "CARD_LOCALROOT", str(explicit_card_root))

    assert CardDatastore.get_storage_root(
        "local", datastore_root=str(datastore_root)
    ) == str(explicit_card_root)


def test_local_card_root_uses_resolved_datastore_root(
    no_explicit_card_root, monkeypatch, tmp_path
):
    # Priority 2 (the #2139 fix): with no explicit card root, cards follow the
    # resolved datastore root instead of the current working directory. The cwd
    # must NOT gain a `.metaflow` directory.
    work_dir = tmp_path / "work"
    datastore_root = tmp_path / "shared" / DATASTORE_LOCAL_DIR
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    assert CardDatastore.get_storage_root(
        "local", datastore_root=str(datastore_root)
    ) == os.path.join(str(datastore_root), CARD_SUFFIX)
    assert not (work_dir / DATASTORE_LOCAL_DIR).exists()


def test_local_card_root_falls_back_to_nearest_metaflow_directory(
    no_explicit_card_root, monkeypatch, tmp_path
):
    # Priority 3, legacy behavior: with neither an explicit card root nor a
    # supplied datastore root, walk upward to the nearest existing `.metaflow`.
    ancestor = tmp_path / "ancestor"
    work_dir = ancestor / "project" / "subdir"
    metaflow_dir = ancestor / DATASTORE_LOCAL_DIR
    work_dir.mkdir(parents=True)
    metaflow_dir.mkdir()
    monkeypatch.chdir(work_dir)

    assert CardDatastore.get_storage_root("local") == os.path.join(
        str(metaflow_dir.resolve()), CARD_SUFFIX
    )


def test_local_card_root_treats_empty_datastore_root_as_missing(
    no_explicit_card_root, monkeypatch, tmp_path
):
    # Keep compatibility with older/empty `ds-root` metadata: an empty string is
    # not a valid configured root, so fall through to the legacy lookup.
    ancestor = tmp_path / "ancestor"
    work_dir = ancestor / "project" / "subdir"
    metaflow_dir = ancestor / DATASTORE_LOCAL_DIR
    work_dir.mkdir(parents=True)
    metaflow_dir.mkdir()
    monkeypatch.chdir(work_dir)

    assert CardDatastore.get_storage_root("local", datastore_root="") == os.path.join(
        str(metaflow_dir.resolve()), CARD_SUFFIX
    )


def test_local_card_root_falls_back_to_cwd_when_no_root_exists(
    no_explicit_card_root, monkeypatch, tmp_path
):
    # Legacy last resort: no config, no supplied root, and no ancestor
    # `.metaflow` -> use `<cwd>/.metaflow/mf.cards`.
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    assert CardDatastore.get_storage_root("local") == os.path.join(
        str((work_dir / DATASTORE_LOCAL_DIR).resolve()), CARD_SUFFIX
    )


def test_spin_card_root_uses_resolved_datastore_root(no_explicit_card_root, tmp_path):
    # spin shares this code path with local; guard against fixing local while
    # regressing spin. spin uses `.metaflow_spin` as its datastore dir.
    datastore_root = tmp_path / DATASTORE_SPIN_LOCAL_DIR

    assert CardDatastore.get_storage_root(
        "spin", datastore_root=str(datastore_root)
    ) == os.path.join(str(datastore_root), CARD_SUFFIX)


@pytest.mark.parametrize(
    "storage_type,root_attr",
    [
        ("s3", "CARD_S3ROOT"),
        ("azure", "CARD_AZUREROOT"),
        ("gs", "CARD_GSROOT"),
    ],
)
def test_cloud_card_roots_are_unchanged(
    monkeypatch, tmp_path, storage_type, root_attr
):
    # The fix is scoped to local/spin. Cloud backends must keep returning their
    # configured root and ignore any supplied datastore_root.
    configured_root = "cloud://bucket/%s" % storage_type
    monkeypatch.setattr(card_datastore, root_attr, configured_root)

    assert (
        CardDatastore.get_storage_root(
            storage_type, datastore_root=str(tmp_path / "ignored")
        )
        == configured_root
    )
