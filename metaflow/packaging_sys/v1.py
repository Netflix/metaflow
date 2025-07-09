import json
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple, Union

from ..debug import debug
from ..extension_support import (
    EXT_EXCLUDE_SUFFIXES,
    extension_info,
    package_mfext_all,
    package_mfext_all_descriptions,
)
from ..exception import MetaflowException
from ..metaflow_version import get_version
from ..user_decorators.user_flow_decorator import FlowMutatorMeta
from ..user_decorators.user_step_decorator import UserStepDecoratorMeta
from ..util import get_metaflow_root
from . import ContentType, MFCONTENT_MARKER, MetaflowCodeContentV1Base
from .distribution_support import _ModuleInfo, modules_to_distributions
from .utils import suffix_filter, walk


class MetaflowCodeContentV1(MetaflowCodeContentV1Base):
    METAFLOW_SUFFIXES_LIST = [".py", ".html", ".css", ".js"]

    def __init__(
        self,
        code_dir: str = MetaflowCodeContentV1Base._code_dir,
        other_dir: str = MetaflowCodeContentV1Base._other_dir,
        criteria: Callable[[ModuleType], bool] = lambda x: True,
    ):
        super().__init__(code_dir, other_dir)

        self._metaflow_root = get_metaflow_root()
        self._metaflow_version = get_version()

        self._criteria = criteria

        # We try to find the modules we need to package. We will first look at all modules
        # and apply the criteria to them. Then we will use the most parent module that
        # fits the criteria as the module to package
        modules = filter(lambda x: criteria(x[1]), sys.modules.items())
        # Ensure that we see the parent modules first
        modules = sorted(modules, key=lambda x: x[0])
        if modules:
            last_prefix = modules[0][0]
            new_modules = [modules[0]]
            for name, mod in modules[1:]:
                if name.startswith(last_prefix + "."):
                    # This is a submodule of the last module, we can skip it
                    continue
                # Otherwise, we have a new top-level module
                last_prefix = name
                new_modules.append((name, mod))
        else:
            new_modules = []

        self._modules = {
            name: _ModuleInfo(
                name,
                set(
                    Path(p).resolve().as_posix()
                    for p in getattr(mod, "__path__", [mod.__file__])
                ),
                mod,
                True,  # This is a Metaflow module (see filter below)
            )
            for (name, mod) in new_modules
        }

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

        self._other_files = {}  # type: Dict[str, str]
        self._other_content = {}  # type: Dict[str, bytes]

        debug.package_exec(f"Used system modules found: {str(self._modules)}")

        # Populate with files from the third party modules
        for k, v in self._modules.items():
            self._files_from_modules.update(self._module_files(k, v.root_paths))

        # Figure out the files to package for Metaflow and extensions
        self._cached_metaflow_files = list(self._metaflow_distribution_files())
        self._cached_metaflow_files.extend(list(self._metaflow_extension_files()))

    def create_mfcontent_info(self) -> Dict[str, Any]:
        return {"version": 1, "module_files": list(self._files_from_modules.values())}

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
        return [self._code_dir, self._other_dir]

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
        yield from self._content(content_types, generate_value=False)

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
        yield from self._content(content_types, generate_value=True)

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
        all_user_step_decorators = {}
        for k, v in UserStepDecoratorMeta.all_decorators().items():
            all_user_step_decorators.setdefault(
                getattr(v, "_original_module", v.__module__), []
            ).append(k)

        all_user_flow_decorators = {}
        for k, v in FlowMutatorMeta.all_decorators().items():
            all_user_flow_decorators.setdefault(
                getattr(v, "_original_module", v.__module__), []
            ).append(k)

        result = []
        if self._metaflow_version:
            result.append(f"\nMetaflow version: {self._metaflow_version}")
        ext_info = extension_info()
        if ext_info["installed"]:
            result.append("\nMetaflow extensions packaged:")
            for ext_name, ext_info in ext_info["installed"].items():
                result.append(
                    f"  - {ext_name} ({ext_info['extension_name']}) @ {ext_info['dist_version']}"
                )

        if self._modules:
            mf_modules = []
            other_modules = []
            for name, info in self._modules.items():
                if info.metaflow_module:
                    mf_modules.append(f"  - {name} @ {', '.join(info.root_paths)}")
                    module_user_step_decorators = [
                        ", ".join(v)
                        for k, v in all_user_step_decorators.items()
                        if k == info.name or k.startswith(info.name + ".")
                    ]
                    module_user_flow_decorators = [
                        ", ".join(v)
                        for k, v in all_user_flow_decorators.items()
                        if k == info.name or k.startswith(info.name + ".")
                    ]
                    if module_user_step_decorators:
                        mf_modules.append(
                            f"    - Provides step decorators: {', '.join(module_user_step_decorators)}"
                        )
                    if module_user_flow_decorators:
                        mf_modules.append(
                            f"    - Provides flow mutators: {', '.join(module_user_flow_decorators)}"
                        )
                else:
                    other_modules.append(f"  - {name} @ {', '.join(info.root_paths)}")
            if mf_modules:
                result.append("\nMetaflow modules:")
                result.extend(mf_modules)
            if other_modules:
                result.append("\nNon-Metaflow packaged modules:")
                result.extend(other_modules)

        return "\n".join(result)

    def add_info(self, info: Dict[str, Any]) -> None:
        """
        Add the content of the INFO file to the Metaflow content

        Parameters
        ----------
        info: Dict[str, Any]
            The content of the INFO file
        """
        info_file_path = os.path.join(self._other_dir, self._info_file)
        if info_file_path in self._other_content:
            raise MetaflowException("INFO file already present in the MF environment")
        self._other_content[info_file_path] = json.dumps(info).encode("utf-8")

    def add_config(self, config: Dict[str, Any]) -> None:
        """
        Add the content of the CONFIG file to the Metaflow content

        Parameters
        ----------
        config: Dict[str, Any]
            The content of the CONFIG file
        """
        config_file_path = os.path.join(self._other_dir, self._config_file)
        if config_file_path in self._other_content:
            raise MetaflowException("CONFIG file already present in the MF environment")
        self._other_content[config_file_path] = json.dumps(config).encode("utf-8")

    def add_module(self, module: ModuleType) -> None:
        """
        Add a python module to the Metaflow content

        Parameters
        ----------
        module_path: ModuleType
            The module to add
        """
        name = module.__name__
        debug.package_exec(f"Adding module {name} to the MF content")
        # If the module is a single file, we handle this here by looking at __file__
        # which will point to the single file. If it is an actual module, __path__
        # will contain the path(s) to the module
        self._modules[name] = _ModuleInfo(
            name,
            set(
                Path(p).resolve().as_posix()
                for p in getattr(module, "__path__", [module.__file__])
            ),
            module,
            False,  # This is not a Metaflow module (added by the user manually)
        )
        self._files_from_modules.update(
            self._module_files(name, self._modules[name].root_paths)
        )

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
        file_path = os.path.realpath(file_path)
        debug.package_exec(
            f"Adding code file {file_path} as {file_name} to the MF content"
        )

        if file_path in self._files and self._files[file_path] != os.path.join(
            self._code_dir, file_name.lstrip("/")
        ):
            raise MetaflowException(
                "File '%s' is already present in the MF content with a different name: '%s'"
                % (file_path, self._files[file_path])
            )
        self._files[file_path] = os.path.join(self._code_dir, file_name.lstrip("/"))

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
        file_path = os.path.realpath(file_path)
        debug.package_exec(
            f"Adding other file {file_path} as {file_name} to the MF content"
        )
        if file_path in self._other_files and self._other_files[
            file_path
        ] != os.path.join(self._other_dir, file_name.lstrip("/")):
            raise MetaflowException(
                "File %s is already present in the MF content with a different name: %s"
                % (file_path, self._other_files[file_path])
            )
        self._other_files[file_path] = os.path.join(
            self._other_dir, file_name.lstrip("/")
        )

    def _content(
        self, content_types: Optional[int] = None, generate_value: bool = False
    ) -> Generator[Tuple[Union[str, bytes], str], None, None]:
        from ..package import MetaflowPackage  # Prevent circular dependency

        if content_types is None:
            content_types = ContentType.ALL_CONTENT.value

        if content_types & ContentType.CODE_CONTENT.value:
            yield from self._cached_metaflow_files
            yield from self._files.items()
        if content_types & ContentType.MODULE_CONTENT.value:
            yield from self._files_from_modules.items()
        if content_types & ContentType.OTHER_CONTENT.value:
            yield from self._other_files.items()
            if generate_value:
                for k, v in self._other_content.items():
                    yield v, k
                # Include the distribution file too
                yield json.dumps(self._distmetainfo).encode("utf-8"), os.path.join(
                    self._other_dir, self._dist_info_file
                )
                yield json.dumps(self.create_mfcontent_info()).encode(
                    "utf-8"
                ), os.path.join(self._code_dir, MFCONTENT_MARKER)
            else:
                for k in self._other_content.keys():
                    yield "<generated %s content>" % (os.path.basename(k)), k
                yield "<generated %s content>" % (
                    os.path.basename(self._dist_info_file)
                ), os.path.join(self._other_dir, self._dist_info_file)
                yield "<generated %s content>" % MFCONTENT_MARKER, os.path.join(
                    self._code_dir, MFCONTENT_MARKER
                )

    def _metaflow_distribution_files(self) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec("Including Metaflow from '%s'" % self._metaflow_root)
        for path_tuple in walk(
            os.path.join(self._metaflow_root, "metaflow"),
            exclude_hidden=False,
            file_filter=suffix_filter(self.METAFLOW_SUFFIXES_LIST),
        ):
            yield path_tuple[0], os.path.join(self._code_dir, path_tuple[1])

    def _metaflow_extension_files(self) -> Generator[Tuple[str, str], None, None]:
        # Metaflow extensions; for now, we package *all* extensions but this may change
        # at a later date; it is possible to call `package_mfext_package` instead of
        # `package_mfext_all` but in that case, make sure to also add a
        # metaflow_extensions/__init__.py file to properly "close" the metaflow_extensions
        # package and prevent other extensions from being loaded that may be
        # present in the rest of the system
        for path_tuple in package_mfext_all():
            yield path_tuple[0], os.path.join(self._code_dir, path_tuple[1])
        if debug.package:
            ext_info = package_mfext_all_descriptions()
            ext_info = {
                k: {k1: v1 for k1, v1 in v.items() if k1 in ("root_paths",)}
                for k, v in ext_info.items()
            }
            debug.package_exec(f"Metaflow extensions packaged: {ext_info}")

    def _module_files(
        self, name: str, paths: Set[str]
    ) -> Generator[Tuple[str, str], None, None]:
        debug.package_exec(
            "    Looking for distributions for module %s in %s" % (name, paths)
        )
        paths = set(paths)  # Do not modify external paths
        has_init = False
        distributions = modules_to_distributions().get(name)
        prefix_parts = tuple(name.split("."))

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
                        "METADATA": dist.read_text("METADATA") or "",
                        "RECORD": dist.read_text("RECORD") or "",
                    }
                for file in dist.files or []:
                    # Skip files that do not belong to this module (distribution may
                    # provide multiple modules)
                    if file.parts[: len(prefix_parts)] != prefix_parts:
                        continue
                    if file.parts[len(prefix_parts)] == "__init__.py":
                        has_init = True
                    yield str(
                        dist.locate_file(file).resolve().as_posix()
                    ), os.path.join(self._code_dir, *prefix_parts, *file.parts[1:])

        # Now if there are more paths left in paths, it means there is a non-distribution
        # component to this package which we also include.
        debug.package_exec(
            "    Looking for non-distribution files for module '%s' in %s"
            % (name, paths)
        )
        for path in paths:
            if not Path(path).is_dir():
                # Single file for the module -- this will be something like <name>.py
                yield path, os.path.join(
                    self._code_dir, *prefix_parts[:-1], f"{prefix_parts[-1]}.py"
                )
                has_init = True
            else:
                for root, _, files in os.walk(path):
                    for file in files:
                        if any(file.endswith(x) for x in EXT_EXCLUDE_SUFFIXES):
                            continue
                        rel_path = os.path.relpath(os.path.join(root, file), path)
                        if rel_path == "__init__.py":
                            has_init = True
                        yield os.path.join(root, file), os.path.join(
                            self._code_dir,
                            name,
                            rel_path,
                        )
        # We now include an empty __init__.py file to close the module and prevent
        # leaks from possible namespace packages
        if not has_init:
            yield os.path.join(
                self._metaflow_root, "metaflow", "extension_support", "_empty_file.py"
            ), os.path.join(self._code_dir, *prefix_parts, "__init__.py")
