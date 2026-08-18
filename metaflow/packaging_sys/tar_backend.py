import inspect
import os
import tarfile

from io import BytesIO
from typing import Any, IO, List, Optional, Union

from .backend import PackagingBackend


_EXTRACTALL_SUPPORTS_FILTER = (
    "filter" in inspect.signature(tarfile.TarFile.extractall).parameters
)


def _member_target_path(dest_dir: str, member_name: str) -> str:
    return os.path.abspath(os.path.join(dest_dir, member_name))


def _has_parent_reference(path: str) -> bool:
    return ".." in path.replace("\\", "/").split("/")


def _ensure_within_directory(dest_dir: str, target_path: str) -> None:
    abs_dest = os.path.abspath(dest_dir)
    try:
        is_within_dest = os.path.commonpath([abs_dest, target_path]) == abs_dest
    except ValueError:
        is_within_dest = False
    if not is_within_dest:
        raise tarfile.ExtractError("Attempted path traversal in TAR file")


def _validate_member(dest_dir: str, member: tarfile.TarInfo) -> None:
    if _has_parent_reference(member.name):
        raise tarfile.ExtractError("Attempted path traversal in TAR file")

    target_path = _member_target_path(dest_dir, member.name)
    _ensure_within_directory(dest_dir, target_path)

    if member.issym():
        if _has_parent_reference(member.linkname):
            raise tarfile.ExtractError("Attempted path traversal in TAR file")
        if os.path.isabs(member.linkname):
            link_target = os.path.abspath(member.linkname)
        else:
            link_target = os.path.abspath(
                os.path.join(os.path.dirname(target_path), member.linkname)
            )
        _ensure_within_directory(dest_dir, link_target)
    elif member.islnk():
        if _has_parent_reference(member.linkname):
            raise tarfile.ExtractError("Attempted path traversal in TAR file")
        link_target = os.path.abspath(os.path.join(dest_dir, member.linkname))
        _ensure_within_directory(dest_dir, link_target)


def _extractall_preserving_validated_members(
    archive: tarfile.TarFile, dest_dir: str, members: List[Any]
) -> None:
    kwargs = {}
    if _EXTRACTALL_SUPPORTS_FILTER:
        kwargs["filter"] = "tar"
    archive.extractall(path=dest_dir, members=members, **kwargs)


class TarPackagingBackend(PackagingBackend):
    type = "tgz"

    @classmethod
    def get_extract_commands(cls, archive_name: str, dest_dir: str) -> List[str]:
        return [
            f"TAR_OPTIONS='--warning=no-timestamp' tar -xzf {archive_name} -C {dest_dir}"
        ]

    def __init__(self):
        super().__init__()
        self._buf = None

    def create(self):
        self._buf = BytesIO()
        self._archive = tarfile.open(
            fileobj=self._buf, mode="w:gz", compresslevel=3, dereference=True
        )
        return self

    def add_file(self, filename: str, arcname: Optional[str] = None):
        info = self._archive.gettarinfo(filename, arcname)
        # Setting this default to Dec 3, 2019
        info.mtime = 1575360000
        with open(filename, mode="rb") as f:
            self._archive.addfile(info, f)

    def add_data(self, data: BytesIO, arcname: str):
        info = tarfile.TarInfo(arcname)
        data.seek(0)
        info.size = len(data.getvalue())
        # Setting this default to Dec 3, 2019
        info.mtime = 1575360000
        self._archive.addfile(info, data)

    def close(self):
        if self._archive:
            self._archive.close()

    def get_blob(self) -> Optional[Union[bytes, bytearray]]:
        if self._buf:
            blob = bytearray(self._buf.getvalue())
            blob[4:8] = [0] * 4  # Reset 4 bytes from offset 4 to account for ts
            return blob
        return None

    @classmethod
    def cls_open(cls, content: IO[bytes]) -> tarfile.TarFile:
        return tarfile.open(fileobj=content, mode="r:gz")

    @classmethod
    def cls_member_name(cls, member: Union[tarfile.TarInfo, str]) -> str:
        """
        Returns the name of the member as a string.
        """
        return member.name if isinstance(member, tarfile.TarInfo) else member

    @classmethod
    def cls_has_member(cls, archive: tarfile.TarFile, name: str) -> bool:
        try:
            archive.getmember(name)
            return True
        except KeyError:
            return False

    @classmethod
    def cls_get_member(cls, archive: tarfile.TarFile, name: str) -> Optional[bytes]:
        try:
            member = archive.getmember(name)
            return archive.extractfile(member).read()
        except KeyError:
            return None

    @classmethod
    def cls_extract_members(
        cls,
        archive: tarfile.TarFile,
        members: Optional[List[Any]] = None,
        dest_dir: str = ".",
    ) -> None:
        members = archive.getmembers() if members is None else list(members)
        for member in members:
            _validate_member(dest_dir, member)
        _extractall_preserving_validated_members(archive, dest_dir, members)

    @classmethod
    def cls_list_members(
        cls, archive: tarfile.TarFile
    ) -> Optional[List[tarfile.TarInfo]]:
        return archive.getmembers() or None

    @classmethod
    def cls_list_names(cls, archive: tarfile.TarFile) -> Optional[List[str]]:
        return archive.getnames() or None
