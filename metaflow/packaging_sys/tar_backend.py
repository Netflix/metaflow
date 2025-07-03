import tarfile

from io import BytesIO
from typing import IO, List, Optional, Union

from .backend import PackagingBackend


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
        members: Optional[List[str]] = None,
        dest_dir: str = ".",
    ) -> None:
        archive.extractall(path=dest_dir, members=members)

    @classmethod
    def cls_list_members(cls, archive: tarfile.TarFile) -> Optional[List[str]]:
        return archive.getnames() or None
