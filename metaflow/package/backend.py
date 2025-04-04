from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, List, Optional, Union


class PackagingBackend(ABC):

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
    def cls_has_member(cls, archive: Any, name: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def cls_extract_member(cls, archive: Any, name: str) -> Optional[bytes]:
        pass

    @classmethod
    @abstractmethod
    def cls_list_members(cls, archive: Any) -> Optional[List[str]]:
        pass

    def has_member(self, name: str) -> bool:
        if self._archive:
            return self.cls_has_member(self._archive, name)
        return False

    def extract_member(self, name: str) -> Optional[bytes]:
        if self._archive:
            return self.cls_extract_member(self._archive, name)
        return None

    def list_members(self) -> Optional[List[str]]:
        if self._archive:
            return self.cls_list_members(self._archive)
        return None

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
