from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, IO, List, Optional, Union


class PackagingBackend(ABC):
    _mappings = {}
    type = "none"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.type in cls._mappings:
            raise ValueError(f"PackagingBackend {cls.type} already exists")
        cls._mappings[cls.type] = cls

    @classmethod
    def get_backend(cls, name: str) -> "PackagingBackend":
        if name not in cls._mappings:
            raise ValueError(f"PackagingBackend {name} not found")
        return cls._mappings[name]

    @classmethod
    def backend_type(cls) -> str:
        return cls.type

    @classmethod
    @abstractmethod
    def get_extract_commands(cls, archive_name: str, dest_dir: str) -> List[str]:
        pass

    def __init__(self):
        self._archive = None

    @abstractmethod
    def create(self) -> "PackagingBackend":
        pass

    @abstractmethod
    def add_file(self, filename: str, arcname: Optional[str] = None):
        pass

    @abstractmethod
    def add_data(self, data: BytesIO, arcname: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_blob(self) -> Optional[Union[bytes, bytearray]]:
        pass

    @classmethod
    @abstractmethod
    def cls_open(cls, content: IO[bytes]) -> Any:
        """Open the archive from the given content."""
        pass

    @classmethod
    @abstractmethod
    def cls_has_member(cls, archive: Any, name: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def cls_get_member(cls, archive: Any, name: str) -> Optional[bytes]:
        pass

    @classmethod
    @abstractmethod
    def cls_extract_members(
        cls,
        archive: Any,
        members: Optional[List[str]] = None,
        dest_dir: str = ".",
    ) -> None:
        pass

    @classmethod
    @abstractmethod
    def cls_list_members(cls, archive: Any) -> Optional[List[str]]:
        pass

    def has_member(self, name: str) -> bool:
        if self._archive:
            return self.cls_has_member(self._archive, name)
        raise ValueError("Cannot check for member in an uncreated archive")

    def get_member(self, name: str) -> Optional[bytes]:
        if self._archive:
            return self.cls_get_member(self._archive, name)
        raise ValueError("Cannot get member from an uncreated archive")

    def extract_members(
        self, members: Optional[List[str]] = None, dest_dir: str = "."
    ) -> None:
        if self._archive:
            self.cls_extract_members(self._archive, members, dest_dir)
        else:
            raise ValueError("Cannot extract from an uncreated archive")

    def list_members(self) -> Optional[List[str]]:
        if self._archive:
            return self.cls_list_members(self._archive)
        raise ValueError("Cannot list members from an uncreated archive")

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
