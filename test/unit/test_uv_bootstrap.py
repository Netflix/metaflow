import inspect
import io
import tarfile

import pytest

from metaflow.plugins.uv import bootstrap as uv_bootstrap


def _make_uv_tar(member_name: str, data: bytes = b"uv-binary") -> tuple[bytes, bytes]:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name=member_name)
        info.size = len(data)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(data))
    return buf.getvalue(), data


def test_extract_uv_falls_back_when_filter_is_unsupported(monkeypatch, tmp_path):
    tar_bytes, data = _make_uv_tar("some/dir/uv")
    original_extractall = tarfile.TarFile.extractall

    def raising_extractall(self, path, *args, **kwargs):
        if "filter" in kwargs:
            raise TypeError("unexpected keyword argument 'filter'")
        return original_extractall(self, path, *args, **kwargs)

    monkeypatch.setattr(tarfile.TarFile, "extractall", raising_extractall)

    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        uv_bootstrap._extract_uv(tar, str(tmp_path))

    assert (tmp_path / "uv").read_bytes() == data


def test_extract_uv_fallback_sanitizes_member_path(monkeypatch, tmp_path):
    tar_bytes, data = _make_uv_tar("../../uv")
    original_extractall = tarfile.TarFile.extractall

    def raising_extractall(self, path, *args, **kwargs):
        if "filter" in kwargs:
            raise TypeError("unexpected keyword argument 'filter'")
        return original_extractall(self, path, *args, **kwargs)

    monkeypatch.setattr(tarfile.TarFile, "extractall", raising_extractall)

    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        uv_bootstrap._extract_uv(tar, str(tmp_path))

    assert (tmp_path / "uv").read_bytes() == data


def test_extract_uv_uses_filter_when_available(tmp_path):
    if "filter" not in inspect.signature(tarfile.TarFile.extractall).parameters:
        pytest.skip("tarfile.extractall(filter=...) not supported on this Python")

    tar_bytes, data = _make_uv_tar("nested/uv")
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        uv_bootstrap._extract_uv(tar, str(tmp_path))

    assert (tmp_path / "uv").read_bytes() == data

