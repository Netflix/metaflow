import io
import os
import tarfile

import pytest

from metaflow.packaging_sys import tar_backend
from metaflow.packaging_sys.tar_backend import TarPackagingBackend
from metaflow.packaging_sys.tar_backend import _extractall_preserving_validated_members


def _archive_with(member):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        archive.addfile(member, io.BytesIO(b"payload"))
    data.seek(0)
    return data


def test_extract_members_rejects_parent_path_traversal(tmp_path):
    member = tarfile.TarInfo("../escaped.txt")
    member.size = len(b"payload")

    with TarPackagingBackend.cls_open(_archive_with(member)) as archive:
        with pytest.raises(tarfile.ExtractError):
            TarPackagingBackend.cls_extract_members(archive, dest_dir=str(tmp_path))

    assert not (tmp_path.parent / "escaped.txt").exists()


def test_extract_members_rejects_symlink_escape(tmp_path):
    member = tarfile.TarInfo("link")
    member.type = tarfile.SYMTYPE
    member.linkname = os.path.join("..", "escaped.txt")

    with TarPackagingBackend.cls_open(_archive_with(member)) as archive:
        with pytest.raises(tarfile.ExtractError):
            TarPackagingBackend.cls_extract_members(archive, dest_dir=str(tmp_path))

    assert not (tmp_path / "link").exists()


def test_extract_members_rejects_hardlink_escape(tmp_path):
    member = tarfile.TarInfo("link")
    member.type = tarfile.LNKTYPE
    member.linkname = os.path.join("..", "escaped.txt")

    with TarPackagingBackend.cls_open(_archive_with(member)) as archive:
        with pytest.raises(tarfile.ExtractError):
            TarPackagingBackend.cls_extract_members(archive, dest_dir=str(tmp_path))

    assert not (tmp_path / "link").exists()


class _RecordingArchive:
    def __init__(self):
        self.extractall_kwargs = None

    def extractall(self, **kwargs):
        self.extractall_kwargs = kwargs


def test_extractall_uses_tar_filter_when_supported(monkeypatch, tmp_path):
    archive = _RecordingArchive()
    member = tarfile.TarInfo("safe.txt")
    monkeypatch.setattr(tar_backend, "_EXTRACTALL_SUPPORTS_FILTER", True)

    _extractall_preserving_validated_members(archive, str(tmp_path), [member])

    assert archive.extractall_kwargs == {
        "path": str(tmp_path),
        "members": [member],
        "filter": "tar",
    }


def test_extractall_omits_filter_when_unsupported(monkeypatch, tmp_path):
    archive = _RecordingArchive()
    member = tarfile.TarInfo("safe.txt")
    monkeypatch.setattr(tar_backend, "_EXTRACTALL_SUPPORTS_FILTER", False)

    _extractall_preserving_validated_members(archive, str(tmp_path), [member])

    assert archive.extractall_kwargs == {
        "path": str(tmp_path),
        "members": [member],
    }
