import io
import os
import tarfile

import pytest

from metaflow.packaging_sys import tar_backend
from metaflow.packaging_sys.tar_backend import TarPackagingBackend
from metaflow.packaging_sys.tar_backend import _ensure_within_directory
from metaflow.packaging_sys.tar_backend import _extractall_preserving_validated_members
from metaflow.packaging_sys.tar_backend import _member_target_path
from metaflow.packaging_sys.tar_backend import _validate_member


def _archive_with(member):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        archive.addfile(member, io.BytesIO(b"payload"))
    data.seek(0)
    return data


def _archive_with_members(*members):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        for member in members:
            archive.addfile(member, io.BytesIO(b"payload"))
    data.seek(0)
    return data


def test_extract_members_accepts_explicit_safe_member_list(tmp_path):
    included = tarfile.TarInfo("included.txt")
    included.size = len(b"payload")
    skipped = tarfile.TarInfo("../skipped.txt")
    skipped.size = len(b"payload")

    with TarPackagingBackend.cls_open(
        _archive_with_members(included, skipped)
    ) as archive:
        member = archive.getmember("included.txt")
        TarPackagingBackend.cls_extract_members(
            archive, members=(member,), dest_dir=str(tmp_path)
        )

    assert (tmp_path / "included.txt").read_bytes() == b"payload"
    assert not (tmp_path.parent / "skipped.txt").exists()


def test_member_target_path_resolves_under_destination(tmp_path):
    assert _member_target_path(str(tmp_path), "nested/safe.txt") == os.path.abspath(
        os.path.join(str(tmp_path), "nested/safe.txt")
    )


def test_directory_check_accepts_inside_path(tmp_path):
    _ensure_within_directory(str(tmp_path), str(tmp_path / "safe.txt"))


def test_validate_member_accepts_safe_relative_symlink(tmp_path):
    member = tarfile.TarInfo("nested/link")
    member.type = tarfile.SYMTYPE
    member.linkname = "target.txt"

    _validate_member(str(tmp_path), member)


def test_validate_member_accepts_safe_absolute_symlink(tmp_path):
    member = tarfile.TarInfo("nested/link")
    member.type = tarfile.SYMTYPE
    member.linkname = os.path.abspath(os.path.join(str(tmp_path), "target.txt"))

    _validate_member(str(tmp_path), member)


def test_validate_member_accepts_safe_hardlink(tmp_path):
    member = tarfile.TarInfo("nested/link")
    member.type = tarfile.LNKTYPE
    member.linkname = "target.txt"

    _validate_member(str(tmp_path), member)


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


def test_extract_members_rejects_absolute_symlink_escape(tmp_path):
    member = tarfile.TarInfo("link")
    member.type = tarfile.SYMTYPE
    member.linkname = os.path.abspath(os.path.join(str(tmp_path), "..", "escaped.txt"))

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


def test_directory_check_rejects_commonpath_errors(monkeypatch, tmp_path):
    def raise_value_error(paths):
        raise ValueError("different drives")

    monkeypatch.setattr(tar_backend.os.path, "commonpath", raise_value_error)

    with pytest.raises(tarfile.ExtractError):
        _ensure_within_directory(str(tmp_path), str(tmp_path / "safe.txt"))


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
