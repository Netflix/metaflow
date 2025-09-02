import json
import os

from enum import IntEnum
from types import ModuleType
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Type,
    Union,
)

from metaflow.packaging_sys.distribution_support import PackagedDistributionFinder


from .backend import PackagingBackend
from .tar_backend import TarPackagingBackend

from ..util import get_metaflow_root

MFCONTENT_MARKER = ".mf_install"

if TYPE_CHECKING:
    import metaflow.extension_support.metadata


class ContentType(IntEnum):
    USER_CONTENT = (
        0x1  # File being added is user code (ie: the directory with the flow file)
    )
    CODE_CONTENT = (
        0x2  # File being added is non-user code (libraries, metaflow itself, ...)
    )
    MODULE_CONTENT = 0x4  # File being added is a python module
    OTHER_CONTENT = 0x8  # File being added is a non-python file

    ALL_CONTENT = USER_CONTENT | CODE_CONTENT | MODULE_CONTENT | OTHER_CONTENT


class MetaflowCodeContent:
    """
    Base class for all Metaflow code packages (non user code).

    A Metaflow code package, at a minimum, contains:
      - a special INFO file (containing a bunch of metadata about the Metaflow environment)
      - a special CONFIG file (containing user configurations for the flow)

    Declare all other MetaflowCodeContent subclasses (versions) here to handle just the functions
    that are not implemented here. In a *separate* file, declare any other
    function for that specific version.

    NOTE: This file must remain as dependency-free as possible as it is loaded *very*
    early on. This is why you must decleare a *separate* class implementing what you want
    the Metaflow code package (non user) to do.
    """

    _cached_mfcontent_info = {}

    _mappings = {}

    @classmethod
    def get_info(cls) -> Optional[Dict[str, Any]]:
        """
        Get the content of the special INFO file on the local filesystem after
        the code package has been expanded.

        Returns
        -------
        Optional[Dict[str, Any]]
            The content of the INFO file -- None if there is no such file.
        """
        mfcontent_info = cls._extract_mfcontent_info()
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_info_impl(mfcontent_info)

    @classmethod
    def get_config(cls) -> Optional[Dict[str, Any]]:
        """
        Get the content of the special CONFIG file on the local filesystem after
        the code package has been expanded.

        Returns
        -------
        Optional[Dict[str, Any]]
            The content of the CONFIG file -- None if there is no such file.
        """
        mfcontent_info = cls._extract_mfcontent_info()
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_config_impl(mfcontent_info)

    @classmethod
    def get_filename(cls, filename: str, content_type: ContentType) -> Optional[str]:
        """
        Get the path to a file extracted from the archive. The filename is the filename
        passed in when creating the archive and content_type is the type of the content.

        This function will return the local path where the file can be found after
        the package has been extracted.

        Parameters
        ----------
        filename: str
            The name of the file on the filesystem.
        content_type: ContentType

        Returns
        -------
        str
            The path to the file on the local filesystem or None if not found.
        """
        mfcontent_info = cls._extract_mfcontent_info()
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_filename_impl(mfcontent_info, filename, content_type)

    @classmethod
    def get_env_vars_for_packaged_metaflow(cls, dest_dir: str) -> Dict[str, str]:
        """
        Get the environment variables that are needed to run Metaflow when it is
        packaged. This is typically used to set the PYTHONPATH to include the
        directory where the Metaflow code package has been extracted.

        Returns
        -------
        Dict[str, str]
            The environment variables that are needed to run Metaflow when it is
            packaged it present.
        """
        mfcontent_info = cls._extract_mfcontent_info(dest_dir)
        if mfcontent_info is None:
            # No MFCONTENT_MARKER file found -- this is not a packaged Metaflow code
            # package so no environment variables to set.
            return {}
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        v = handling_cls.get_post_extract_env_vars_impl(dest_dir)
        v["METAFLOW_EXTRACTED_ROOT:"] = dest_dir
        return v

    @classmethod
    def get_archive_info(
        cls,
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the content of the special INFO file in the archive.

        Returns
        -------
        Optional[Dict[str, Any]]
            The content of the INFO file -- None if there is no such file.
        """
        mfcontent_info = cls._extract_archive_mfcontent_info(archive, packaging_backend)
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_archive_info_impl(
            mfcontent_info, archive, packaging_backend
        )

    @classmethod
    def get_archive_config(
        cls,
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the content of the special CONFIG file in the archive.

        Returns
        -------
        Optional[Dict[str, Any]]
            The content of the CONFIG file -- None if there is no such file.
        """
        mfcontent_info = cls._extract_archive_mfcontent_info(archive, packaging_backend)
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_archive_config_impl(
            mfcontent_info, archive, packaging_backend
        )

    @classmethod
    def get_archive_filename(
        cls,
        archive: Any,
        filename: str,
        content_type: ContentType,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[str]:
        """
        Get the filename of the archive. This does not do any extraction but simply
        returns where, in the archive, the file is located. This is the equivalent of
        get_filename but for files not extracted yet.

        Parameters
        ----------
        archive: Any
            The archive to get the filename from.
        filename: str
            The name of the file in the archive.
        content_type: ContentType
            The type of the content (e.g., code, other, etc.).
        packaging_backend: Type[PackagingBackend], default TarPackagingBackend
            The packaging backend to use.

        Returns
        -------
        str
            The filename of the archive or None if not found.
        """
        mfcontent_info = cls._extract_archive_mfcontent_info(archive, packaging_backend)
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_archive_filename_impl(
            mfcontent_info, archive, filename, content_type, packaging_backend
        )

    @classmethod
    def get_archive_content_members(
        cls,
        archive: Any,
        content_types: Optional[int] = None,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> List[Any]:
        mfcontent_info = cls._extract_archive_mfcontent_info(archive, packaging_backend)
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_archive_content_members_impl(
            mfcontent_info, archive, content_types, packaging_backend
        )

    @classmethod
    def get_distribution_finder(
        cls,
    ) -> Optional["metaflow.extension_support.metadata.DistributionFinder"]:
        """
        Get the distribution finder for the Metaflow code package (if applicable).

        Some packages will include distribution information to "pretend" that some packages
        are actually distributions even if we just include them in the code package.

        Returns
        -------
        Optional["metaflow.extension_support.metadata.DistributionFinder"]
            The distribution finder for the Metaflow code package -- None if there is no
            such finder.
        """
        mfcontent_info = cls._extract_mfcontent_info()
        handling_cls = cls._get_mfcontent_class(mfcontent_info)
        return handling_cls.get_distribution_finder_impl(mfcontent_info)

    @classmethod
    def get_post_extract_env_vars(
        cls, version_id: int, dest_dir: str = "."
    ) -> Dict[str, str]:
        """
        Get the post-extract environment variables that are needed to access the content
        that has been extracted into dest_dir.

        This will typically involve setting PYTHONPATH.

        Parameters
        ----------
        version_id: int
            The version of MetaflowCodeContent for this package.
        dest_dir: str, default "."
            The directory where the content has been extracted to.

        Returns
        -------
        Dict[str, str]
            The post-extract environment variables that are needed to access the content
            that has been extracted into extracted_dir.
        """
        if version_id not in cls._mappings:
            raise ValueError(
                "Invalid package -- unknown version %s in info: %s"
                % (version_id, cls._mappings)
            )
        v = cls._mappings[version_id].get_post_extract_env_vars_impl(dest_dir)
        v["METAFLOW_EXTRACTED_ROOT:"] = dest_dir
        return v

    # Implement the _impl methods in the base subclass (in this file). These need to
    # happen with as few imports as possible to prevent circular dependencies.
    @classmethod
    def get_info_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("get_info_impl not implemented")

    @classmethod
    def get_config_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("get_config_impl not implemented")

    @classmethod
    def get_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        filename: str,
        content_type: ContentType,
    ) -> Optional[str]:
        raise NotImplementedError("get_filename_impl not implemented")

    @classmethod
    def get_distribution_finder_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional["metaflow.extension_support.metadata.DistributionFinder"]:
        raise NotImplementedError("get_distribution_finder_impl not implemented")

    @classmethod
    def get_archive_info_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("get_archive_info_impl not implemented")

    @classmethod
    def get_archive_config_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("get_archive_config_impl not implemented")

    @classmethod
    def get_archive_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        filename: str,
        content_type: ContentType,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[str]:
        raise NotImplementedError("get_archive_filename_impl not implemented")

    @classmethod
    def get_archive_content_members_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        content_types: Optional[int] = None,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> List[Any]:
        raise NotImplementedError("get_archive_content_members_impl not implemented")

    @classmethod
    def get_post_extract_env_vars_impl(cls, dest_dir: str) -> Dict[str, str]:
        raise NotImplementedError("get_post_extract_env_vars_impl not implemented")

    def __init_subclass__(cls, version_id, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        if version_id in MetaflowCodeContent._mappings:
            raise ValueError(
                "Version ID %s already exists in MetaflowCodeContent mappings "
                "-- this is a bug in Metaflow." % str(version_id)
            )
        MetaflowCodeContent._mappings[version_id] = cls
        cls._version_id = version_id

    # Implement these methods in sub-classes of the base sub-classes. These methods
    # are called later and can have more dependencies and so can live in other files.
    def get_excluded_tl_entries(self) -> List[str]:
        """
        When packaging Metaflow from within an executing Metaflow flow, we need to
        exclude the files that are inserted by this content from being packaged (possibly).

        Use this function to return these files or top-level directories.

        Returns
        -------
        List[str]
            Files or directories to exclude
        """
        return []

    def content_names(
        self, content_types: Optional[int] = None
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Detailed list of the content of this MetaflowCodeContent. This will list all files
        (or non files -- for the INFO or CONFIG data for example) present in the archive.

        Parameters
        ----------
        content_types : Optional[int]
            The type of content to get the names of. If None, all content is returned.

        Yields
        ------
        Generator[Tuple[str, str], None, None]
            Path on the filesystem and the name in the archive
        """
        raise NotImplementedError("content_names not implemented")

    def contents(
        self, content_types: Optional[int] = None
    ) -> Generator[Tuple[Union[bytes, str], str], None, None]:
        """
        Very similar to content_names but returns the content of the non-files
        as well as bytes. For files, identical output as content_names

        Parameters
        ----------
        content_types : Optional[int]
            The type of content to get the content of. If None, all content is returned.

        Yields
        ------
        Generator[Tuple[Union[str, bytes], str], None, None]
            Content of the MF content
        """
        raise NotImplementedError("content not implemented")

    def show(self) -> str:
        """
        Returns a more human-readable string representation of the content of this
        MetaflowCodeContent. This will not, for example, list all files but summarize what
        is included at a more high level.

        Returns
        -------
        str
            A human-readable string representation of the content of this MetaflowCodeContent
        """
        raise NotImplementedError("show not implemented")

    def add_info(self, info: Dict[str, Any]) -> None:
        """
        Add the content of the INFO file to the Metaflow content

        Parameters
        ----------
        info: Dict[str, Any]
            The content of the INFO file
        """
        raise NotImplementedError("add_info not implemented")

    def add_config(self, config: Dict[str, Any]) -> None:
        """
        Add the content of the CONFIG file to the Metaflow content

        Parameters
        ----------
        config: Dict[str, Any]
            The content of the CONFIG file
        """
        raise NotImplementedError("add_config not implemented")

    def add_module(self, module_path: ModuleType) -> None:
        """
        Add a python module to the Metaflow content

        Parameters
        ----------
        module_path: ModuleType
            The module to add
        """
        raise NotImplementedError("add_module not implemented")

    def add_code_file(self, file_path: str, file_name: str) -> None:
        """
        Add a code file to the Metaflow content

        Parameters
        ----------
        file_path: str
            The path to the code file to add (on the filesystem)
        file_name: str
            The path in the archive to add the code file to
        """
        raise NotImplementedError("add_code_file not implemented")

    def add_other_file(self, file_path: str, file_name: str) -> None:
        """
        Add a non-python file to the Metaflow content

        Parameters
        ----------
        file_path: str
            The path to the file to add (on the filesystem)
        file_name: str
            The path in the archive to add the file to
        """
        raise NotImplementedError("add_other_file not implemented")

    @classmethod
    def _get_mfcontent_class(
        cls, info: Optional[Dict[str, Any]]
    ) -> Type["MetaflowCodeContent"]:
        if info is None:
            return MetaflowCodeContentV0
        if "version" not in info:
            raise ValueError("Invalid package -- missing version in info: %s" % info)
        version = info["version"]
        if version not in cls._mappings:
            raise ValueError(
                "Invalid package -- unknown version %s in info: %s" % (version, info)
            )

        return cls._mappings[version]

    @classmethod
    def _extract_archive_mfcontent_info(
        cls,
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        if id(archive) in cls._cached_mfcontent_info:
            return cls._cached_mfcontent_info[id(archive)]

        mfcontent_info = None  # type: Optional[Dict[str, Any]]
        # Here we need to extract the information from the archive
        if packaging_backend.cls_has_member(archive, MFCONTENT_MARKER):
            # The MFCONTENT_MARKER file is present in the archive
            # We can extract the information from it
            extracted_info = packaging_backend.cls_get_member(archive, MFCONTENT_MARKER)
            if extracted_info:
                mfcontent_info = json.loads(extracted_info)
        cls._cached_mfcontent_info[id(archive)] = mfcontent_info
        return mfcontent_info

    @classmethod
    def _extract_mfcontent_info(
        cls, target_dir: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        target_dir = target_dir or "_local"
        if target_dir in cls._cached_mfcontent_info:
            return cls._cached_mfcontent_info[target_dir]

        mfcontent_info = None  # type: Optional[Dict[str, Any]]
        if target_dir == "_local":
            root = os.environ.get("METAFLOW_EXTRACTED_ROOT", get_metaflow_root())
        else:
            root = target_dir
        if os.path.exists(os.path.join(root, MFCONTENT_MARKER)):
            with open(os.path.join(root, MFCONTENT_MARKER), "r", encoding="utf-8") as f:
                mfcontent_info = json.load(f)
        cls._cached_mfcontent_info[target_dir] = mfcontent_info
        return mfcontent_info

    def get_package_version(self) -> int:
        """
        Get the version of MetaflowCodeContent for this package.
        """
        # _version_id is set in __init_subclass__ when the subclass is created
        return self._version_id


class MetaflowCodeContentV0(MetaflowCodeContent, version_id=0):
    @classmethod
    def get_info_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        path_to_file = os.path.join(get_metaflow_root(), "INFO")
        if os.path.isfile(path_to_file):
            with open(path_to_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    @classmethod
    def get_config_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        path_to_file = os.path.join(get_metaflow_root(), "CONFIG")
        if os.path.isfile(path_to_file):
            with open(path_to_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    @classmethod
    def get_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        filename: str,
        content_type: ContentType,
    ) -> Optional[str]:
        """
        For V0, the filename is simply the filename passed in.
        """
        path_to_file = os.path.join(get_metaflow_root(), filename)
        if os.path.isfile(path_to_file):
            return path_to_file
        return None

    @classmethod
    def get_distribution_finder_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional["metaflow.extension_support.metadata.DistributionFinder"]:
        return None

    @classmethod
    def get_archive_info_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        info_content = packaging_backend.cls_get_member(archive, "INFO")
        if info_content:
            return json.loads(info_content)
        return None

    @classmethod
    def get_archive_config_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        info_content = packaging_backend.cls_get_member(archive, "CONFIG")
        if info_content:
            return json.loads(info_content)
        return None

    @classmethod
    def get_archive_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        filename: str,
        content_type: ContentType,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> str:
        if packaging_backend.cls_has_member(archive, filename):
            # The file is present in the archive
            return filename
        return None

    @classmethod
    def get_archive_content_members_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        content_types: Optional[int] = None,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> List[Any]:
        """
        For V0, we use a static list of known files to classify the content
        """
        known_prefixes = {
            "metaflow/": ContentType.CODE_CONTENT.value,
            "metaflow_extensions/": ContentType.CODE_CONTENT.value,
            "INFO": ContentType.OTHER_CONTENT.value,
            "CONFIG": ContentType.OTHER_CONTENT.value,
            "conda.manifest": ContentType.OTHER_CONTENT.value,
            "uv.lock": ContentType.OTHER_CONTENT.value,
            "pyproject.toml": ContentType.OTHER_CONTENT.value,
            # Used in nflx-metaflow-extensions
            "condav2-1.cnd": ContentType.OTHER_CONTENT.value,
        }
        to_return = []
        for member in packaging_backend.cls_list_members(archive):
            filename = packaging_backend.cls_member_name(member)
            added = False
            for prefix, classification in known_prefixes.items():
                if (
                    prefix[-1] == "/" and filename.startswith(prefix)
                ) or prefix == filename:
                    if content_types & classification:
                        to_return.append(member)
                        added = True
                        break
            if not added and content_types & ContentType.USER_CONTENT.value:
                # Everything else is user content
                to_return.append(member)
        return to_return

    @classmethod
    def get_post_extract_env_vars_impl(cls, dest_dir: str) -> Dict[str, str]:
        return {"PYTHONPATH": dest_dir}

    def get_excluded_tl_entries(self) -> List[str]:
        """
        When packaging Metaflow from within an executing Metaflow flow, we need to
        exclude the files that are inserted by this content from being packaged (possibly).

        Use this function to return these files or top-level directories.

        Returns
        -------
        List[str]
            Files or directories to exclude
        """
        return ["CONFIG", "INFO"]

    # Other non-implemented methods are OK not being implemented as they will never
    # be called as they are only used when creating the package and we are starting
    # with V1.


class MetaflowCodeContentV1Base(MetaflowCodeContent, version_id=1):
    _code_dir = ".mf_code"
    _other_dir = ".mf_meta"
    _info_file = "INFO"
    _config_file = "CONFIG"
    _dist_info_file = "DIST_INFO"

    def __init_subclass__(cls, **kwargs) -> None:
        # Important to add this here to prevent the subclass of MetaflowCodeContentV1Base from
        # also calling __init_subclass__ in MetaflowCodeContent (which would create a problem)
        return None

    def __init__(self, code_dir: str, other_dir: str) -> None:
        self._code_dir = code_dir
        self._other_dir = other_dir

    @classmethod
    def _get_otherfile_path(
        cls, mfcontent_info: Optional[Dict[str, Any]], filename: str, in_archive: bool
    ) -> str:
        if in_archive:
            return os.path.join(cls._other_dir, filename)
        return os.path.join(get_metaflow_root(), "..", cls._other_dir, filename)

    @classmethod
    def _get_codefile_path(
        cls, mfcontent_info: Optional[Dict[str, Any]], filename: str, in_archive: bool
    ) -> str:
        if in_archive:
            return os.path.join(cls._code_dir, filename)
        return os.path.join(get_metaflow_root(), filename)

    @classmethod
    def get_info_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        path_to_file = cls._get_otherfile_path(
            mfcontent_info, cls._info_file, in_archive=False
        )
        if os.path.isfile(path_to_file):
            with open(path_to_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    @classmethod
    def get_config_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        path_to_file = cls._get_otherfile_path(
            mfcontent_info, cls._config_file, in_archive=False
        )
        if os.path.isfile(path_to_file):
            with open(path_to_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    @classmethod
    def get_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        filename: str,
        content_type: ContentType,
    ) -> Optional[str]:
        if content_type == ContentType.CODE_CONTENT:
            path_to_file = cls._get_codefile_path(
                mfcontent_info, filename, in_archive=False
            )
        elif content_type in (ContentType.OTHER_CONTENT, ContentType.MODULE_CONTENT):
            path_to_file = cls._get_otherfile_path(
                mfcontent_info, filename, in_archive=False
            )
        else:
            raise ValueError(
                f"Invalid content type {content_type} for filename {filename}"
            )
        if os.path.isfile(path_to_file):
            return path_to_file
        return None

    @classmethod
    def get_distribution_finder_impl(
        cls, mfcontent_info: Optional[Dict[str, Any]]
    ) -> Optional["metaflow.extension_support.metadata.DistributionFinder"]:
        path_to_file = cls._get_otherfile_path(
            mfcontent_info, cls._dist_info_file, in_archive=False
        )
        if os.path.isfile(path_to_file):
            with open(path_to_file, "r", encoding="utf-8") as f:
                return PackagedDistributionFinder(json.load(f))
        return None

    @classmethod
    def get_archive_info_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        info_file = packaging_backend.cls_get_member(
            archive,
            cls._get_otherfile_path(mfcontent_info, cls._info_file, in_archive=True),
        )
        if info_file:
            return json.loads(info_file)
        return None

    @classmethod
    def get_archive_config_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> Optional[Dict[str, Any]]:
        config_file = packaging_backend.cls_get_member(
            archive,
            cls._get_otherfile_path(mfcontent_info, cls._config_file, in_archive=True),
        )
        if config_file:
            return json.loads(config_file)
        return None

    @classmethod
    def get_archive_filename_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        filename: str,
        content_type: ContentType,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> str:
        if content_type == ContentType.CODE_CONTENT:
            path_to_file = cls._get_codefile_path(
                mfcontent_info, filename, in_archive=False
            )
        elif content_type in (ContentType.OTHER_CONTENT, ContentType.MODULE_CONTENT):
            path_to_file = cls._get_otherfile_path(
                mfcontent_info, filename, in_archive=False
            )
        else:
            raise ValueError(
                f"Invalid content type {content_type} for filename {filename}"
            )
        if packaging_backend.cls_has_member(archive, path_to_file):
            # The file is present in the archive
            return path_to_file
        return None

    @classmethod
    def get_archive_content_members_impl(
        cls,
        mfcontent_info: Optional[Dict[str, Any]],
        archive: Any,
        content_types: Optional[int] = None,
        packaging_backend: Type[PackagingBackend] = TarPackagingBackend,
    ) -> List[Any]:
        to_return = []
        module_content = set(mfcontent_info.get("module_files", []))
        for member in packaging_backend.cls_list_members(archive):
            filename = packaging_backend.cls_member_name(member)
            if filename.startswith(cls._other_dir) and (
                content_types & ContentType.OTHER_CONTENT.value
            ):
                to_return.append(member)
            elif filename.startswith(cls._code_dir):
                # Special case for marker which is a other content even if in code.
                if filename == MFCONTENT_MARKER:
                    if content_types & ContentType.OTHER_CONTENT.value:
                        to_return.append(member)
                    else:
                        continue
                # Here it is either module or code
                if os.path.join(cls._code_dir, filename) in module_content:
                    if content_types & ContentType.MODULE_CONTENT.value:
                        to_return.append(member)
                elif content_types & ContentType.CODE_CONTENT.value:
                    to_return.append(member)
            else:
                if content_types & ContentType.USER_CONTENT.value:
                    # Everything else is user content
                    to_return.append(member)
        return to_return

    @classmethod
    def get_post_extract_env_vars_impl(cls, dest_dir: str) -> Dict[str, str]:
        return {"PYTHONPATH": f"{dest_dir}/{cls._code_dir}"}
