import io
import os
import tarfile

import pytest

from metaflow.util import tar_safe_extract


def _make_tar(entries):
    """Build an in-memory tar. entries: list of (name, kind, payload)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, kind, payload in entries:
            info = tarfile.TarInfo(name)
            if kind == "file":
                data = payload.encode()
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
            elif kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = payload
                tar.addfile(info)
    buf.seek(0)
    return tarfile.open(fileobj=buf, mode="r")


@pytest.fixture(params=["filter", "fallback"])
def mode(request, monkeypatch):
    # Exercise both the extraction-filter path and the pre-filter fallback.
    if request.param == "fallback":
        # Python 3.14+ resolves extractall()'s default filter to
        # tarfile.data_filter, so restore the pre-3.14 default (no filtering)
        # before removing it to model an interpreter without filters.
        if hasattr(tarfile, "fully_trusted_filter"):
            monkeypatch.setattr(
                tarfile.TarFile,
                "extraction_filter",
                staticmethod(tarfile.fully_trusted_filter),
            )
        monkeypatch.delattr(tarfile, "data_filter", raising=False)
    elif not hasattr(tarfile, "data_filter"):
        pytest.skip("tarfile extraction filters not available")
    return request.param


def test_extracts_regular_files(tmp_path, mode):
    dest = tmp_path / "out"
    dest.mkdir()
    with _make_tar([("a/b.json", "file", "{}")]) as tar:
        tar_safe_extract(tar, str(dest))
    assert (dest / "a" / "b.json").read_text() == "{}"


def test_rejects_sibling_directory_with_shared_prefix(tmp_path, mode):
    # "<tmp>/foobar" shares a string prefix with "<tmp>/foo" but is outside it.
    # A commonprefix-based check accepted this.
    dest = tmp_path / "foo"
    dest.mkdir()
    with _make_tar([("../foobar/evil.txt", "file", "x")]) as tar:
        with pytest.raises(Exception):
            tar_safe_extract(tar, str(dest))
    assert not (tmp_path / "foobar" / "evil.txt").exists()


def test_rejects_symlink_escaping_destination(tmp_path, mode):
    dest = tmp_path / "out"
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    entries = [
        ("link", "symlink", str(outside)),
        ("link/evil.txt", "file", "x"),
    ]
    with _make_tar(entries) as tar:
        with pytest.raises(Exception):
            tar_safe_extract(tar, str(dest))
    assert not (outside / "evil.txt").exists()


def test_rejects_parent_traversal(tmp_path, mode):
    dest = tmp_path / "out"
    dest.mkdir()
    with _make_tar([("../evil.txt", "file", "x")]) as tar:
        with pytest.raises(Exception):
            tar_safe_extract(tar, str(dest))
    assert not (tmp_path / "evil.txt").exists()
