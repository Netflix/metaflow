import inspect
import os
import sys
import tarfile

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from types import ModuleType


from ..debug import debug
from ..extension_support import EXT_EXCLUDE_SUFFIXES, metadata, package_mfext_all

from ..special_files import MFENV_DIR, SpecialFile
from ..util import get_metaflow_root, to_unicode

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


@dataclass
class _ModuleInfo:
    name: str
    root_paths: Set[str]
    module: ModuleType


class MFEnv:

    METAFLOW_SUFFIXES_LIST = [".py", ".html", ".css", ".js"]

    # this is os.walk(follow_symlinks=True) with cycle detection
    @classmethod
    def walk_without_cycles(
        cls,
        top_root: str,
    ) -> Generator[Tuple[str, List[str]], None, None]:
        seen = set()

        def _recurse(root):
            for parent, dirs, files in os.walk(root):
                for d in dirs:
                    path = os.path.join(parent, d)
                    if os.path.islink(path):
                        # Breaking loops: never follow the same symlink twice
                        #
                        # NOTE: this also means that links to sibling links are
                        # not followed. In this case:
                        #
                        #   x -> y
                        #   y -> oo
                        #   oo/real_file
                        #
                        # real_file is only included twice, not three times
                        reallink = os.path.realpath(path)
                        if reallink not in seen:
                            seen.add(reallink)
                            for x in _recurse(path):
                                yield x
                yield parent, files

        for x in _recurse(top_root):
            yield x

    @classmethod
    def walk(
        cls,
        root: str,
        exclude_hidden: bool = True,
        suffixes: Optional[List[str]] = None,
    ) -> Generator[Tuple[str, str], None, None]:
        if suffixes is None:
            suffixes = []
        root = to_unicode(root)  # handle files/folder with non ascii chars
        prefixlen = len("%s/" % os.path.dirname(root))
        for (
            path,
            files,
        ) in cls.walk_without_cycles(root):
            if exclude_hidden and "/." in path:
                continue
            # path = path[2:] # strip the ./ prefix
            # if path and (path[0] == '.' or './' in path):
            #    continue
            for fname in files:
                if (fname[0] == "." and fname in suffixes) or (
                    fname[0] != "."
                    and any(fname.endswith(suffix) for suffix in suffixes)
                ):
                    p = os.path.join(path, fname)
                    yield p, p[prefixlen:]

    @classmethod
    def get_filename(cls, name: Union[SpecialFile, str]) -> Optional[str]:
        # In all cases, the special files are siblings of the metaflow root
        # directory.
        if isinstance(name, SpecialFile):
            r = get_metaflow_root()
            path_to_file = os.path.join(r, name.value)
        else:
            path_to_file = os.path.join(MFENV_DIR, name)
        if os.path.isfile(path_to_file):
            return path_to_file
        return None

    @classmethod
    def get_content(cls, name: Union[SpecialFile, str]) -> Optional[str]:
        file_to_read = cls.get_filename(name)
        if file_to_read:
            with open(file_to_read, "r", encoding="utf-8") as f:
                return f.read()
        return None

    @classmethod
    def get_archive_filename(
        cls, archive: tarfile.TarFile, name: Union[SpecialFile, str]
    ) -> Optional[str]:
        # Backward compatible way of accessing all special files. Prior to MFEnv, they
        # were stored at the TL of the archive.
        real_name = name.value if isinstance(name, SpecialFile) else name
        if archive.getmember(MFENV_DIR):
            file_path = os.path.join(MFENV_DIR, real_name)
        else:
            file_path = real_name
        if archive.getmember(file_path):
            return file_path
        return None

    @classmethod
    def get_archive_content(
        cls, archive: tarfile.TarFile, name: Union[SpecialFile, str]
    ) -> Optional[str]:
        file_to_read = cls.get_archive_filename(archive, name)
        if file_to_read:
            with archive.extractfile(file_to_read) as f:
                return f.read().decode("utf-8")
        return None

    def __init__(self, criteria: Callable[[ModuleType], bool]) -> None:
        # Look at top-level modules that are present when MFEnv is initialized
        modules = filter(lambda x: "." not in x[0], sys.modules.items())

        # Determine the version of Metaflow that we are part of
        self._metaflow_root = get_metaflow_root()

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
        self._metainfo = {}  # type: Dict[str, Dict[str, str]]

        # Maps an absolute path on the filesystem to the path of the file in the
        # archive.
        self._files = {}  # type: Dict[str, str]

        self._content = {}  # type: Dict[SpecialFile, bytes]

        debug.package_exec(f"Used system modules found: {str(self._modules)}")

        # Populate with files from the third party modules
        for k, v in self._modules.items():
            self._files.update(self._module_files(k, v.root_paths))

        # We include Metaflow as well
        self._files.update(self._metaflow_distribution_files())

        # Include extensions as well
        self._files.update(self._metaflow_extension_files())

    @property
    def root_dir(self):
        return MFENV_DIR

    def add_special_content(self, name: SpecialFile, content: bytes) -> None:
        """
        Add a special file to the MF environment.

        This file will be included in the resulting code package in `MFENV_DIR`.

        Parameters
        ----------
        name : SpecialFile
            The special file to add to the MF environment
        content : bytes
            The content of the special file
        """
        debug.package_exec(f"Adding special content {name.value} to the MF environment")
        self._content[name] = content

    def add_module(self, module: ModuleType) -> None:
        """
        Add a module to the MF environment.

        This module will be included in the resulting code package in `MFENV_DIR`.

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
        self._files.update(self._module_files(name, self._modules[name].root_paths))

    def add_directory(
        self,
        directory: str,
        criteria: Callable[[str], bool],
    ) -> None:
        """
        Add a directory to the MF environment.

        This directory will be included in the resulting code package in `MFENV_DIR`.
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
        name = os.path.basename(directory)
        debug.package_exec(f"Adding directory {directory} to the MF environment")
        for root, _, files in os.walk(directory):
            for file in files:
                if any(file.endswith(x) for x in EXT_EXCLUDE_SUFFIXES):
                    continue
                path = os.path.join(root, file)
                relpath = os.path.relpath(path, directory)
                path = os.path.realpath(path)
                if criteria(path):
                    self._files[path] = os.path.join(name, relpath)

    def add_files(self, files: Iterator[Tuple[str, str]]) -> None:
        """
        Add a list of files to the MF environment.

        These files will be included in the resulting code package in `MFENV_DIR`.


        Parameters
        ----------
        files : Iterator[Tuple[str, str]]
            A list of files to include in the MF environment. The first element of the
            tuple is the path to the file in the filesystem; the second element is the
            path in the archive.
        """
        for file, arcname in files:
            debug.package_exec(f"Adding file {file} as {arcname} to the MF environment")
            self._files[os.path.realpath(file)] = os.path.join(MFENV_DIR, arcname)

    def path_in_archive(self, path: str) -> Optional[str]:
        """
        Return the path of the file in the code package if it is included through
        add_directory or add_files.

        Note that we will use realpath to determine if two paths are equal.
        This includes all files included as part of third party libraries as well as
        anything that was added as part of `add_files` and `add_directory`.

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

    def files(self) -> Generator[Tuple[str, str], None, None]:
        """
        Return a generator of all files included in the MF environment.

        Returns
        -------
        Generator[Tuple[str, str], None, None]
            A generator of all files included in the MF environment. The first element of
            the tuple is the path to the file in the filesystem; the second element is the
            path in the archive.
        """
        return self._files.items()

    def contents(self) -> Generator[Tuple[bytes, str], None, None]:
        """
        Return a generator of all special files included in the MF environment.

        Returns
        -------
        Generator[Tuple[bytes, str], None, None]
            A generator of all special files included in the MF environment. The first
            element of the tuple is the content to add; the second element is path in the
            archive.
        """
        for name, content in self._content.items():
            yield content, os.path.join(MFENV_DIR, name.value)

    def _module_files(
        self, name: str, paths: Set[str]
    ) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec(
            f"    Looking for distributions for module {name} in {paths}"
        )
        paths = set(paths)  # Do not modify external paths
        has_init = False
        distributions = modules_to_distributions().get(name)
        prefix = f"{name}/"
        init_file = f"{prefix}__init__.py"

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
                    f"    Including distribution {dist_name} for module {name}"
                )
                dist_root = dist.locate_file(name)
                if dist_root not in paths:
                    # This is an error because it means that this distribution is
                    # not contributing to the module.
                    raise RuntimeError(
                        f"Distribution '{dist.metadata['Name']}' is not "
                        "contributing to module '{name}' as expected."
                    )
                paths.discard(dist_root)
                if dist_name not in self._metainfo:
                    # Possible that a distribution contributes to multiple modules
                    self._metainfo[dist_name] = {
                        # We can add more if needed but these are likely the most
                        # useful (captures, name, version, etc and files which can
                        # be used to find non-python files in the distribution).
                        "METADATA": dist.read_text("METADATA"),
                        "RECORD": dist.read_text("RECORD"),
                    }
                for file in dist.files or []:
                    # Skip files that do not belong to this module (distribution may
                    # provide multiple modules)
                    if not file.startswith(prefix):
                        continue
                    if file == init_file:
                        has_init = True
                    yield str(dist.locate(file).resolve().as_posix()), os.path.join(
                        MFENV_DIR, str(file)
                    )

        # Now if there are more paths left in paths, it means there is a non-distribution
        # component to this package which we also include.
        debug.package_exec(
            f"    Looking for non-distribution files for module {name} in {paths}"
        )
        for path in paths:
            if not Path(path).is_dir():
                # Single file for the module -- this will be something like <name>.py
                yield path, os.path.join(MFENV_DIR, os.path.basename(path))
            else:
                for root, _, files in os.walk(path):
                    for file in files:
                        if any(file.endswith(x) for x in EXT_EXCLUDE_SUFFIXES):
                            continue
                        rel_path = os.path.relpath(os.path.join(root, file), path)
                        if rel_path == "__init__.py":
                            has_init = True
                        yield os.path.join(root, file), os.path.join(
                            MFENV_DIR,
                            name,
                            rel_path,
                        )
        # We now include an empty __init__.py file to close the module and prevent
        # leaks from possible namespace packages
        if not has_init:
            yield os.path.join(
                self._metaflow_root, "metaflow", "extension_support", "_empty_file.py"
            ), os.path.join(MFENV_DIR, name, "__init__.py")

    def _metaflow_distribution_files(self) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec(
            f"    Including Metaflow from {self._metaflow_root} to the MF Environment"
        )
        for path_tuple in self.walk(
            os.path.join(self._metaflow_root, "metaflow"),
            exclude_hidden=False,
            suffixes=self.METAFLOW_SUFFIXES_LIST,
        ):
            yield path_tuple[0], os.path.join(MFENV_DIR, path_tuple[1])

    def _metaflow_extension_files(self) -> Generator[Tuple[str, str], None, None]:
        # Metaflow extensions; for now, we package *all* extensions but this may change
        # at a later date; it is possible to call `package_mfext_package` instead of
        # `package_mfext_all` but in that case, make sure to also add a
        # metaflow_extensions/__init__.py file to properly "close" the metaflow_extensions
        # package and prevent other extensions from being loaded that may be
        # present in the rest of the system
        for path_tuple in package_mfext_all():
            yield path_tuple[0], os.path.join(MFENV_DIR, path_tuple[1])
