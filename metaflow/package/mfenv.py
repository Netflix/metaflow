import inspect
import json
import os
import re
import sys
import tarfile

from collections import defaultdict
from enum import IntEnum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from types import ModuleType

from .tar_backend import TarPackagingBackend
from .utils import walk

from ..debug import debug
from ..exception import MetaflowException
from ..extension_support import EXT_EXCLUDE_SUFFIXES, metadata, package_mfext_all

from ..meta_files import (
    MFCONF_DIR,
    MFENV_DIR,
    MFENV_MARKER,
    MetaFile,
    generic_get_filename,
    v1_get_filename,
    meta_file_name,
)
from ..util import get_metaflow_root

packages_distributions = None

if sys.version_info[:2] >= (3, 10):
    packages_distributions = metadata.packages_distributions
else:
    # This is the code present in 3.10+ -- we replicate here for other versions
    def _packages_distributions() -> Mapping[str, List[str]]:
        """
        Return a mapping of top-level packages to their
        distributions.
        """
        pkg_to_dist = defaultdict(list)
        for dist in metadata.distributions():
            for pkg in _top_level_declared(dist) or _top_level_inferred(dist):
                pkg_to_dist[pkg].append(dist.metadata["Name"])
        return dict(pkg_to_dist)

    def _top_level_declared(dist: metadata.Distribution) -> List[str]:
        return (dist.read_text("top_level.txt") or "").split()

    def _topmost(name: "pathlib.PurePosixPath") -> Optional[str]:
        """
        Return the top-most parent as long as there is a parent.
        """
        top, *rest = name.parts
        return top if rest else None

    def _get_toplevel_name(name: "pathlib.PurePosixPath") -> str:
        return _topmost(name) or (
            # python/typeshed#10328
            inspect.getmodulename(name)  # type: ignore
            or str(name)
        )

    def _top_level_inferred(dist: "metadata.Distribution"):
        opt_names = set(map(_get_toplevel_name, dist.files or []))

        def importable_name(name):
            return "." not in name

        return filter(importable_name, opt_names)

    packages_distributions = _packages_distributions


if TYPE_CHECKING:
    import pathlib


_cached_distributions = None

name_normalizer = re.compile(r"[-_.]+")


class AddToPackageType(IntEnum):
    CODE_ONLY = 0x100
    CONFIG_ONLY = 0x200
    FILES_ONLY = 0x400
    ALL = 0xFFF
    CODE_FILE = 0x501
    CODE_MODULE = 0x502
    CODE_METAFLOW = 0x504
    CONFIG_FILE = 0x601
    CONFIG_CONTENT = 0x202


def modules_to_distributions() -> Dict[str, List[metadata.Distribution]]:
    """
    Return a mapping of top-level modules to their distributions.

    Returns
    -------
    Dict[str, List[metadata.Distribution]]
        A mapping of top-level modules to their distributions.
    """
    global _cached_distributions
    if _cached_distributions is None:
        _cached_distributions = {
            k: [metadata.distribution(d) for d in v]
            for k, v in packages_distributions().items()
        }
    return _cached_distributions


_ModuleInfo = NamedTuple(
    "_ModuleInfo", [("name", str), ("root_paths", Set[str]), ("module", ModuleType)]
)


class PackagedDistribution(metadata.Distribution):
    """
    A Python Package packaged within a MFEnv. This allows users to use use importlib
    as they would regularly and the packaged Python Package would be considered as a
    distribution even if it really isn't (since it is just included in the PythonPath).
    """

    def __init__(self, root: str, content: Dict[str, str]):
        self._root = Path(root)
        self._content = content

    # Strongly inspired from PathDistribution in metadata.py
    def read_text(self, filename: Union[str, os.PathLike]) -> Optional[str]:
        if str(filename) in self._content:
            return self._content[str(filename)]
        return None

    read_text.__doc__ = metadata.Distribution.read_text.__doc__

    # Returns a metadata.SimplePath but not always present in importlib.metadata libs so
    # skipping return type.
    def locate_file(self, path: Union[str, os.PathLike]):
        return self._root / path


class PackagedDistributionFinder(metadata.DistributionFinder):

    def __init__(self, dist_info: Dict[str, Dict[str, str]]):
        self._dist_info = dist_info

    def find_distributions(self, context=metadata.DistributionFinder.Context()):
        if context.name is None:
            # Yields all known distributions
            for name, info in self._dist_info.items():
                yield PackagedDistribution(
                    os.path.join(get_metaflow_root(), name), info
                )
        name = name_normalizer.sub("-", context.name).lower()
        if name in self._dist_info:
            yield PackagedDistribution(
                os.path.join(get_metaflow_root(), context.name),
                self._dist_info[name],
            )
        return None


class MFEnv:
    _cached_mfenv_info = {}

    _mappings = {}

    @classmethod
    def get_filename(
        cls, name: Union[MetaFile, str], is_meta: Optional[bool] = None
    ) -> Optional[str]:
        """
        Get the filename of a file in the expanded package. The filename will point to
        a path on the local filesystem.

        Parameters
        ----------
        name : Union[MetaFile, str]
            Filename to look for. If it is a MetaFile, it is assumed to be relative
            to the configuration directory. If it is a string, it is assumed to be
            relative to the code directory.
        is_meta : bool, optional, default None
            If None, the default behavior above is assumed. If True, the file will be
            searched for in the configuration directory and if False, it will be
            searched for in the code directory.

        Returns
        -------
        Optional[str]
            The file path of the file if it is exists -- None if there is no such file.
        """
        # Get the filename of the expanded file.
        # Two cases:
        # 1. The file was encoded prior to MFEnv packaging -- it will be next to
        #    Metaflow (sibling)
        # 2. The file was encoded as part of the MFEnv packaging, we redirect
        # it to the proper version of MFEnv to do the extraction
        mfenv_info = cls._extract_mfenv_info()
        handling_cls = cls._get_mfenv_class(mfenv_info)
        if handling_cls:
            return handling_cls._get_filename(name, mfenv_info, is_meta)

        return generic_get_filename(name, is_meta)

    @classmethod
    def get_content(
        cls, name: Union[MetaFile, str], is_meta: Optional[bool] = None
    ) -> Optional[bytes]:
        """
        Get the content of a file in the expanded package. The content is returned as
        bytes

        Parameters
        ----------
        name : Union[MetaFile, str]
            Filename to look for. If it is a MetaFile, it is assumed to be relative
            to the configuration directory. If it is a string, it is assumed to be
            relative to the code directory.
        is_meta : bool, optional, default None
            If None, the default behavior above is assumed. If True, the file will be
            searched for in the configuration directory and if False, it will be
            searched for in the code directory.

        Returns
        -------
        Optional[bytes]
            The binary content of the file -- None if there is no such file.
        """
        file_to_read = cls.get_filename(name, is_meta)
        if file_to_read:
            with open(file_to_read, "rb") as f:
                return f.read()
        return None

    @classmethod
    def get_archive_filename(
        cls,
        archive: Any,
        name: Union[MetaFile, str],
        is_meta: Optional[bool] = None,
        packaging_backend=TarPackagingBackend,
    ) -> Optional[str]:
        """
        Get the filename of a file in the archive. The filename will point to
        a path in the archive.

        Parameters
        ----------
        name : Union[MetaFile, str]
            Filename to look for. If it is a MetaFile, it is assumed to be relative
            to the configuration directory. If it is a string, it is assumed to be
            relative to the code directory.
        is_meta : bool, optional, default None
            If None, the default behavior above is assumed. If True, the file will be
            searched for in the configuration directory and if False, it will be
            searched for in the code directory.

        Returns
        -------
        Optional[str]
            The file path of the file if it is exists -- None if there is no such file.
        """
        mfenv_info = cls._extract_archive_mfenv_info(archive, packaging_backend)
        handling_cls = cls._get_mfenv_class(mfenv_info)
        if handling_cls:
            return handling_cls._get_archive_filename(
                archive, name, mfenv_info, is_meta, packaging_backend
            )
        # Backward compatible way of accessing all special files. Prior to MFEnv, they
        # were stored at the TL of the archive. There is no distinction between code and
        # config.
        real_name = meta_file_name(name)
        if packaging_backend.cls_has_member(archive, real_name):
            return real_name
        return None

    @classmethod
    def get_archive_content(
        cls,
        archive: tarfile.TarFile,
        name: Union[MetaFile, str],
        is_meta: Optional[bool] = None,
        packaging_backend=TarPackagingBackend,
    ) -> Optional[bytes]:
        """
        Get the content of a file in archive. The content is returned as
        bytes

        Parameters
        ----------
        name : Union[MetaFile, str]
            Filename to look for. If it is a MetaFile, it is assumed to be relative
            to the configuration directory. If it is a string, it is assumed to be
            relative to the code directory.
        is_meta : bool, optional, default None
            If None, the default behavior above is assumed. If True, the file will be
            searched for in the configuration directory and if False, it will be
            searched for in the code directory.

        Returns
        -------
        Optional[bytes]
            The binary content of the file -- None if there is no such file.
        """
        file_to_extract = cls.get_archive_filename(
            archive, name, is_meta, packaging_backend
        )
        if file_to_extract:
            return packaging_backend.cls_extract_member(archive, file_to_extract)
        return None

    def __init_subclass__(cls, version_id, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        if version_id in MFEnv._mappings:
            raise ValueError(
                "Version ID %s already exists in MFEnv mappings "
                "-- this is a bug in Metaflow." % str(version_id)
            )
        MFEnv._mappings[version_id] = cls

    @classmethod
    def _get_mfenv_class(cls, info: Optional[Dict[str, Any]]):
        if info is None:
            return None
        if "version" not in info:
            raise MetaflowException(
                "Invalid package -- missing version in info: %s" % info
            )
        version = info["version"]
        if version not in cls._mappings:
            raise MetaflowException(
                "Invalid package -- unknown version %s in info: %s" % (version, info)
            )

        return cls._mappings[version]

    @classmethod
    def _extract_archive_mfenv_info(
        cls, archive: Any, packaging_backend=TarPackagingBackend
    ):
        if id(archive) in cls._cached_mfenv_info:
            return cls._cached_mfenv_info[id(archive)]

        mfenv_info = None
        # Here we need to extract the information from the archive
        if packaging_backend.cls_has_member(archive, MFENV_MARKER):
            # The MFENV_MARKER file is present in the archive
            # We can extract the information from it
            mfenv_info = packaging_backend.cls_extract_member(archive, MFENV_MARKER)
            if mfenv_info:
                mfenv_info = json.loads(mfenv_info)
        cls._cached_mfenv_info[id(archive)] = mfenv_info
        return mfenv_info

    @classmethod
    def _extract_mfenv_info(cls):
        if "_local" in cls._cached_mfenv_info:
            return cls._cached_mfenv_info["_local"]

        mfenv_info = None
        if os.path.exists(os.path.join(get_metaflow_root(), MFENV_MARKER)):
            with open(
                os.path.join(get_metaflow_root(), MFENV_MARKER), "r", encoding="utf-8"
            ) as f:
                mfenv_info = json.load(f)
        cls._cached_mfenv_info["_local"] = mfenv_info
        return mfenv_info


class MFEnvV1(MFEnv, version_id=1):

    METAFLOW_SUFFIXES_LIST = [".py", ".html", ".css", ".js"]

    @classmethod
    def _get_filename(
        cls,
        name: Union[MetaFile, str],
        meta_info: Dict[str, Any],
        is_meta: Optional[bool] = None,
    ) -> Optional[str]:
        return v1_get_filename(name, meta_info, is_meta)

    @classmethod
    def _get_archive_filename(
        cls,
        archive: Any,
        name: Union[MetaFile, str],
        meta_info: Dict[str, Any],
        is_meta: Optional[bool] = None,
        packaging_backend=TarPackagingBackend,
    ) -> Optional[str]:
        if is_meta is None:
            is_meta = isinstance(name, MetaFile)
        if is_meta:
            conf_dir = meta_info.get("conf_dir")
            if conf_dir is None:
                raise MetaflowException(
                    "Invalid package -- package info does not contain conf_dir key"
                )
            path_to_search = os.path.join(conf_dir, meta_file_name(name))
        else:
            code_dir = meta_info.get("code_dir")
            if code_dir is None:
                raise MetaflowException(
                    "Invalid package -- package info does not contain code_dir key"
                )
            path_to_search = os.path.join(code_dir, meta_file_name(name))
        if packaging_backend.cls_has_member(archive, path_to_search):
            return path_to_search
        return None

    def __init__(
        self,
        criteria: Callable[[ModuleType], bool],
        package_path=MFENV_DIR,
        config_path=MFCONF_DIR,
    ) -> None:
        # package_path is the directory within the archive where the files will be
        # stored (by default MFENV_DIR). This is used in internal Netflix code.

        # Look at top-level modules that are present when MFEnv is initialized
        modules = filter(lambda x: "." not in x[0], sys.modules.items())

        # Determine the version of Metaflow that we are part of
        self._metaflow_root = get_metaflow_root()

        self._package_path = package_path
        self._config_path = config_path

        self._modules = {
            name: _ModuleInfo(
                name,
                set(Path(p).resolve().as_posix() for p in getattr(mod, "__path__", [])),
                mod,
            )
            for name, mod in dict(modules).items()
        }  # type: Dict[str, Set[str]]

        # Filter the modules
        self._modules = {
            name: info for name, info in self._modules.items() if criteria(info.module)
        }

        # Contain metadata information regarding the distributions packaged.
        # This allows Metaflow to "fake" distribution information when packaged
        self._distmetainfo = {}  # type: Dict[str, Dict[str, str]]

        # Maps an absolute path on the filesystem to the path of the file in the
        # archive.
        self._files = {}  # type: Dict[str, str]
        self._files_from_modules = {}  # type: Dict[str, str]

        self._metacontent = {}  # type: Dict[Union[MetaFile, str], bytes]
        self._metafiles = {}  # type: Dict[Union[MetaFile, str], str]

        debug.package_exec(f"Used system modules found: {str(self._modules)}")

        # Populate with files from the third party modules
        for k, v in self._modules.items():
            self._files_from_modules.update(self._module_files(k, v.root_paths))

    def add_meta_content(self, content: bytes, name: Union[MetaFile, str]) -> None:
        """
        Adds metadata to code package. This content will be included under `config_path`
        which defaults to `MFCONF_DIR`.

        Parameters
        ----------
        content : bytes
            The content of the metadata file.
        name : Union[MetaFile, str]
            A known metadata file to add or the name of another metadata file.
        """
        debug.package_exec(
            f"Adding meta content {meta_file_name(name)} to the MF environment"
        )
        if name in self._metacontent:
            # TODO: We could check a hash but this seems like a really corner case.
            raise MetaflowException(
                f"Metadata {meta_file_name(name)} already present in the code package"
            )
        self._metacontent[name] = content

    def add_meta_file(self, file_path: str, file_name: Union[MetaFile, str]) -> None:
        """
        Adds metadata to code package. This file will be included under `config_path`
        which defaults to `MFCONF_DIR`.

        Parameters
        ----------
        file_path : str
            Path to the file to add in the filesystem
        file_name : Union[MetaFile, str]
            Name of the file to add (ie: name in the archive). This is always relative
            to self._config_path
        """
        arcname = meta_file_name(file_name)
        file_path = os.path.realpath(file_path)
        debug.package_exec(
            f"Adding meta file {file_path} as {file_name} to the MF environment"
        )
        if file_path in self._metafiles and self._metafiles[file_path] != os.path.join(
            self._config_path, arcname.lstrip("/")
        ):
            raise MetaflowException(
                "File %s is already present in the MF environment with a different name: %s"
                % (file_path, self._metafiles[file_path])
            )
        self._metafiles[file_path] = os.path.join(
            self._config_path, arcname.lstrip("/")
        )

    def add_module(self, module: ModuleType) -> None:
        """
        Add a module to the MF environment.

        This module will be included in the resulting code package in tar_dir
        (defaults to `MFENV_DIR`).

        Parameters
        ----------
        module : ModuleType
            The module to include in the MF environment
        """
        name = module.__name__
        debug.package_exec(f"Adding module {name} to the MF environment")
        # If the module is a single file, we handle this here by looking at __file__
        # which will point to the single file. If it is an actual module, __path__
        # will contain the path(s) to the module
        self._modules[name] = _ModuleInfo(
            name,
            set(
                Path(p).resolve().as_posix()
                for p in getattr(module, __path__, module.__file__)
            ),
            module,
        )
        self._files_from_modules.update(
            self._module_files(name, self._modules[name].root_paths)
        )

    def add_code_directory(
        self,
        directory: str,
        criteria: Callable[[str], bool],
    ) -> None:
        """
        Add a directory to the MF environment.

        This directory will be included in the resulting code package in tar_dir
        (defaults to`MFENV_DIR`).
        You can optionally specify a criteria function that takes a file path and
        returns a boolean indicating whether or not the file should be included in the
        code package.

        At runtime, the content of the directory will be accessible through the usual
        PYTHONPATH mechanism but also through `current.envdir`.

        Parameters
        ----------
        directory : str
            The directory to include in the MF environment
        criteria : Callable[[str], bool]
            A function that takes a file path and returns a boolean indicating whether or
            not the file should be included in the code package
        """
        directory = os.path.realpath(directory)
        name = os.path.basename(directory)
        debug.package_exec(f"Adding directory {directory} to the MF environment")
        for root, _, files in os.walk(directory):
            for file in files:
                if any(file.endswith(x) for x in EXT_EXCLUDE_SUFFIXES):
                    continue
                path = os.path.join(root, file)
                relpath = os.path.relpath(path, directory)
                if criteria(path):
                    self._files[path] = os.path.join(self._package_path, name, relpath)

    def add_code_file(self, file_path: str, file_name: str) -> None:
        """
        Add a file to the MF environment.

        These files will be included in the resulting code directory (defaults to
        `MFENV_DIR`).


        Parameters
        ----------
        file_path: str
            The path to the file to include in the MF environment
        file_name: str
            The name of the file to include in the MF environment. This is the name
            that will be used in the archive.
        """
        file_path = os.path.realpath(file_path)
        debug.package_exec(
            f"Adding file {file_path} as {file_name} to the MF environment"
        )

        if file_path in self._files and self._files[file_path] != os.path.join(
            self._config_path, file_name.lstrip("/")
        ):
            raise MetaflowException(
                "File %s is already present in the MF environment with a different name: %s"
                % (file_path, self._files[file_path])
            )
        self._files[file_path] = os.path.join(self._package_path, file_name.lstrip("/"))

    def path_in_code(self, path: str) -> Optional[str]:
        """
        Return the path of the file in the code package if it is included through
        add_directory or add_code_file.

        Parameters
        ----------
        path : str
            The path of the file on the filesystem

        Returns
        -------
        Optional[str]
            The path of the file in the code package or None if the file is not included
        """
        return self._files.get(os.path.realpath(path))

    def content_names(
        self,
        content_type: AddToPackageType = AddToPackageType.ALL,
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Return a generator of all files that will be included (restricted to the type
        requested)

        Returns
        -------
        Generator[Tuple[str, str], None, None]
            A generator of all files included in the MF environment. The first element of
            the tuple is the path to the file in the filesystem; the second element is the
            path in the archive.
        """
        yield from self._content(content_type=content_type, generate_value=False)

    def content(
        self,
        content_type: AddToPackageType = AddToPackageType.ALL,
    ) -> Generator[Tuple[Union[str, bytes], str], None, None]:
        """
        Return a generator of all files that will be included (restricted to the type
        requested)

        Returns
        -------
        Generator[Tuple[str, str], None, None]
            A generator of all files included in the MF environment. The first element of
            the tuple is the path to the file in the filesystem (or bytes);
            the second element is the path in the archive.
        """
        yield from self._content(content_type=content_type, generate_value=True)

    def _content(
        self,
        content_type: AddToPackageType = AddToPackageType.ALL,
        generate_value: bool = False,
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Return a generator of all files that will be included (restricted to the type
        requested)

        Returns
        -------
        Generator[Tuple[str, str], None, None]
            A generator of all files included in the MF environment. The first element of
            the tuple is the path to the file in the filesystem; the second element is the
            path in the archive.
        """
        content_type = content_type.value
        if AddToPackageType.CODE_FILE & content_type:
            yield from self._files.items()
        if AddToPackageType.CODE_MODULE & content_type:
            yield from self._files_from_modules.items()
        if AddToPackageType.CODE_METAFLOW & content_type:
            debug.package_exec("Packaging metaflow code...")
            yield from self._metaflow_distribution_files()
            yield from self._metaflow_extension_files()
        if AddToPackageType.CONFIG_FILE & content_type:
            yield from self._metafiles.items()
        if AddToPackageType.CONFIG_CONTENT & content_type:
            if generate_value:
                yield from self._meta_non_file_content(generate_value=generate_value)
            else:
                for _, name in self._meta_non_file_content(False):
                    yield "generated-%s" % name, name

    def _meta_non_file_content(
        self, generate_value: bool = False
    ) -> Generator[Tuple[Optional[bytes], str], None, None]:
        """
        Return a generator of all non file meta data. This will only generate the
        meta data if generate_value is True

        Parameters
        ----------
        generate_value : bool, default False
            If True, the generator will generate the meta data. If False, it will not
            generate but just return the name of the file in the archive

        Returns
        -------
        Generator[Tuple[Optional[bytes], str], None, None]
            A generator of all metafiles included in the MF environment. The first
            element of the tuple is the content to add; the second element is path in the
            archive.
        """
        # Generate the marker
        if generate_value:
            yield (
                json.dumps(
                    {
                        "version": 1,
                        "code_dir": self._package_path,
                        "conf_dir": self._config_path,
                    }
                ).encode("utf-8"),
                os.path.join(self._package_path, MFENV_MARKER),
            )
        else:
            yield None, os.path.join(self._package_path, MFENV_MARKER)

        # All other meta files
        for name in self._metacontent:
            yield None, os.path.join(self._config_path, name.value)
        # Include distribution information if present
        if self._distmetainfo:
            if generate_value:
                yield (
                    json.dumps(self._distmetainfo).encode("utf-8"),
                    os.path.join(self._config_path, MetaFile.INCLUDED_DIST_INFO.value),
                )
            else:
                yield None, os.path.join(
                    self._config_path, MetaFile.INCLUDED_DIST_INFO.value
                )

    def _module_files(
        self, name: str, paths: Set[str]
    ) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec(
            "    Looking for distributions for module %s in %s" % (name, paths)
        )
        paths = set(paths)  # Do not modify external paths
        has_init = False
        distributions = modules_to_distributions().get(name)
        prefix = "%s/" % name
        init_file = "%s__init__.py" % prefix

        seen_distributions = set()
        if distributions:
            for dist in distributions:
                dist_name = dist.metadata["Name"]  # dist.name not always present
                if dist_name in seen_distributions:
                    continue
                # For some reason, sometimes the same distribution appears twice. We
                # don't need to process twice.
                seen_distributions.add(dist_name)
                debug.package_exec(
                    "    Including distribution '%s' for module '%s'"
                    % (dist_name, name)
                )
                dist_root = str(dist.locate_file(name))
                if dist_root not in paths:
                    # This is an error because it means that this distribution is
                    # not contributing to the module.
                    raise RuntimeError(
                        "Distribution '%s' is not contributing to module '%s' as "
                        "expected (got '%s' when expected one of %s)"
                        % (dist.metadata["Name"], name, dist_root, paths)
                    )
                paths.discard(dist_root)
                if dist_name not in self._distmetainfo:
                    # Possible that a distribution contributes to multiple modules
                    self._distmetainfo[dist_name] = {
                        # We can add more if needed but these are likely the most
                        # useful (captures, name, version, etc and files which can
                        # be used to find non-python files in the distribution).
                        "METADATA": dist.read_text("METADATA"),
                        "RECORD": dist.read_text("RECORD"),
                    }
                for file in dist.files or []:
                    # Skip files that do not belong to this module (distribution may
                    # provide multiple modules)
                    if file.parts[0] != name:
                        continue
                    if file == init_file:
                        has_init = True
                    yield str(
                        dist.locate_file(file).resolve().as_posix()
                    ), os.path.join(self._package_path, str(file))

        # Now if there are more paths left in paths, it means there is a non-distribution
        # component to this package which we also include.
        debug.package_exec(
            "    Looking for non-distribution files for module '%s' in %s"
            % (name, paths)
        )
        for path in paths:
            if not Path(path).is_dir():
                # Single file for the module -- this will be something like <name>.py
                yield path, os.path.join(self._package_path, os.path.basename(path))
            else:
                for root, _, files in os.walk(path):
                    for file in files:
                        if any(file.endswith(x) for x in EXT_EXCLUDE_SUFFIXES):
                            continue
                        rel_path = os.path.relpath(os.path.join(root, file), path)
                        if rel_path == "__init__.py":
                            has_init = True
                        yield os.path.join(root, file), os.path.join(
                            self._package_path,
                            name,
                            rel_path,
                        )
        # We now include an empty __init__.py file to close the module and prevent
        # leaks from possible namespace packages
        if not has_init:
            yield os.path.join(
                self._metaflow_root, "metaflow", "extension_support", "_empty_file.py"
            ), os.path.join(self._package_path, name, "__init__.py")

    def _metaflow_distribution_files(self) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec("    Including Metaflow from '%s'" % self._metaflow_root)
        for path_tuple in walk(
            os.path.join(self._metaflow_root, "metaflow"),
            exclude_hidden=False,
            suffixes=self.METAFLOW_SUFFIXES_LIST,
        ):
            yield path_tuple[0], os.path.join(self._package_path, path_tuple[1])

    def _metaflow_extension_files(self) -> Generator[Tuple[str, str], None, None]:
        # Metaflow extensions; for now, we package *all* extensions but this may change
        # at a later date; it is possible to call `package_mfext_package` instead of
        # `package_mfext_all` but in that case, make sure to also add a
        # metaflow_extensions/__init__.py file to properly "close" the metaflow_extensions
        # package and prevent other extensions from being loaded that may be
        # present in the rest of the system
        for path_tuple in package_mfext_all():
            debug.package_exec(
                "    Including Metaflow extension file '%s'" % path_tuple[0]
            )
            yield path_tuple[0], os.path.join(self._package_path, path_tuple[1])
