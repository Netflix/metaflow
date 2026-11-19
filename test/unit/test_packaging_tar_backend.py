"""Anti-Potemkin reproduction suite for Netflix/metaflow#3435 (CWE-22 Tar Slip regression).

Verifies that:
1. `TarPackagingBackend.cls_extract_members` rejects `../` parent-directory traversal
   members instead of writing files outside `dest_dir`.
2. `TarPackagingBackend.cls_extract_members` and `metaflow.util.tar_safe_extract` reject
   sibling-prefix traversal (`../pkg_evil/escaped.py` when `dest_dir` is `.../pkg`).
3. `MetaflowPackage.cls_extract_into` rejects crafted `.tgz` code packages end-to-end.
4. Valid nested Metaflow code packages and filtered `members` subsets extract intact.
"""

from io import BytesIO
import json
import tarfile
from typing import Dict

import pytest

from metaflow.package import MetaflowPackage
from metaflow.packaging_sys import ContentType
from metaflow.packaging_sys.tar_backend import TarPackagingBackend
from metaflow.util import tar_safe_extract


def _build_tgz_bytes(entries: Dict[str, bytes]) -> BytesIO:
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for arcname, payload in entries.items():
            info = tarfile.TarInfo(name=arcname)
            info.size = len(payload)
            tf.addfile(info, BytesIO(payload))
    buf.seek(0)
    return buf


def test_tar_backend_blocks_parent_directory_traversal(tmp_path):
    dest_dir = tmp_path / "extract_target"
    dest_dir.mkdir()
    escaped = tmp_path / "pwned_parent.py"

    archive_buf = _build_tgz_bytes(
        {
            "flow.py": b"print('safe')\n",
            "../pwned_parent.py": b"raise RuntimeError('escaped')\n",
        }
    )

    with TarPackagingBackend.cls_open(archive_buf) as archive:
        with pytest.raises(Exception, match="Attempted path traversal in TAR file"):
            TarPackagingBackend.cls_extract_members(
                archive, members=archive.getmembers(), dest_dir=str(dest_dir)
            )

    assert not escaped.exists(), f"Tar Slip wrote outside dest_dir: {escaped}"


def test_tar_backend_blocks_absolute_path_member(tmp_path):
    dest_dir = tmp_path / "extract_target"
    dest_dir.mkdir()
    escaped = tmp_path / "abs_escape.txt"

    archive_buf = _build_tgz_bytes({str(escaped): b"abs-escape"})

    with TarPackagingBackend.cls_open(archive_buf) as archive:
        with pytest.raises(Exception, match="Attempted path traversal in TAR file"):
            TarPackagingBackend.cls_extract_members(
                archive, members=None, dest_dir=str(dest_dir)
            )

    assert (
        not escaped.exists()
    ), f"Absolute tar member wrote outside dest_dir: {escaped}"


def test_tar_safe_extract_blocks_sibling_prefix_traversal(tmp_path):
    dest_dir = tmp_path / "pkg"
    sibling_dir = tmp_path / "pkg_evil"
    dest_dir.mkdir()
    escaped = sibling_dir / "payload.py"

    archive_buf = _build_tgz_bytes(
        {
            "ok.py": b"VALUE = 1\n",
            "../pkg_evil/payload.py": b"ESCAPED = True\n",
        }
    )

    with tarfile.open(fileobj=archive_buf, mode="r:gz") as tf:
        with pytest.raises(Exception, match="Attempted path traversal in TAR file"):
            tar_safe_extract(tf, path=str(dest_dir))

    assert (
        not escaped.exists()
    ), f"commonprefix sibling-directory bypass wrote outside dest_dir: {escaped}"


def test_metaflow_package_cls_extract_into_blocks_tar_slip_end_to_end(tmp_path):
    dest_dir = tmp_path / "client_code_extract"
    dest_dir.mkdir()
    escaped = tmp_path / "multi_tenant_escape.py"

    pkg_metadata = json.dumps({"archive_format": "tgz", "mfcontent_version": 0})
    archive_buf = _build_tgz_bytes(
        {
            "INFO": json.dumps({"mfcontent_version": 0}).encode("utf-8"),
            "flow.py": b"class MyFlow: pass\n",
            "../multi_tenant_escape.py": b"pwn = 1\n",
        }
    )

    with pytest.raises(Exception, match="Attempted path traversal in TAR file"):
        MetaflowPackage.cls_extract_into(
            pkg_metadata,
            archive_buf,
            dest_dir=str(dest_dir),
            content_types=ContentType.ALL_CONTENT.value,
        )

    assert not escaped.exists(), f"cls_extract_into wrote outside dest_dir: {escaped}"


def test_valid_nested_package_extracts_cleanly(tmp_path):
    dest_dir = tmp_path / "valid_extract"
    dest_dir.mkdir()

    archive_buf = _build_tgz_bytes(
        {
            "flow.py": b"FLOW = True\n",
            "shared/helpers/math_util.py": b"def add(a, b):\n    return a + b\n",
        }
    )

    with TarPackagingBackend.cls_open(archive_buf) as archive:
        TarPackagingBackend.cls_extract_members(
            archive, members=archive.getmembers(), dest_dir=str(dest_dir)
        )

    assert (dest_dir / "flow.py").read_bytes() == b"FLOW = True\n"
    assert (
        dest_dir / "shared" / "helpers" / "math_util.py"
    ).read_text() == "def add(a, b):\n    return a + b\n"


def test_tar_safe_extract_blocks_symlink_escape(tmp_path):
    dest_dir = tmp_path / "pkg"
    outside_dir = tmp_path / "outside"
    dest_dir.mkdir()
    outside_dir.mkdir()
    escaped = outside_dir / "payload.txt"

    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        sym = tarfile.TarInfo(name="link")
        sym.type = tarfile.SYMTYPE
        sym.linkname = "../outside"
        tf.addfile(sym)

        payload = b"escaped via symlink\n"
        reg = tarfile.TarInfo(name="link/payload.txt")
        reg.size = len(payload)
        tf.addfile(reg, BytesIO(payload))
    buf.seek(0)

    with TarPackagingBackend.cls_open(buf) as archive:
        with pytest.raises(Exception, match="Attempted path traversal in TAR file"):
            TarPackagingBackend.cls_extract_members(
                archive, members=archive.getmembers(), dest_dir=str(dest_dir)
            )

    assert not escaped.exists(), f"Symlink Tar Slip wrote outside dest_dir: {escaped}"
