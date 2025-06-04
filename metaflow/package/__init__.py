import os
import sys
import time

from hashlib import sha1
from io import BytesIO
from types import ModuleType
from typing import List, Optional, TYPE_CHECKING, Type, cast

from ..packaging_sys import MFContent
from ..packaging_sys.tar_backend import TarPackagingBackend
from ..packaging_sys.v1 import MFContentV1
from ..packaging_sys.utils import walk
from ..metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from ..exception import MetaflowException
from ..user_configs.config_parameters import dump_config_values
from ..util import get_metaflow_root
from .. import R

if TYPE_CHECKING:
    import metaflow.packaging_sys
    import metaflow.packaging_sys.backend

DEFAULT_SUFFIXES_LIST = DEFAULT_PACKAGE_SUFFIXES.split(",")


class NonUniqueFileNameToFilePathMappingException(MetaflowException):
    headline = "Non-unique file path for a file name included in code package"

    def __init__(self, filename, file_paths, lineno=None):
        msg = (
            "Filename %s included in the code package includes multiple different "
            "paths for the same name : %s.\n"
            "The `filename` in the `add_to_package` decorator hook requires a unique "
            "`file_path` to `file_name` mapping" % (filename, ", ".join(file_paths))
        )
        super().__init__(msg=msg, lineno=lineno)


class MetaflowPackage(object):

    CODE_CONTENT = 0x1  # File being added is code
    MODULE_CONTENT = 0x2  # File being added is a python module
    OTHER_CONTENT = 0x4  # File being added is a non-python file

    def __init__(
        self,
        flow,
        environment,
        echo,
        suffixes: List[str] = DEFAULT_SUFFIXES_LIST,
        mfcontent: Optional["metaflow.packaging_sys.MFContent"] = None,
        exclude_tl_dirs=None,
        backend: Type[
            "metaflow.packaging_sys.backend.PackagingBackend"
        ] = TarPackagingBackend,
    ):
        self.suffixes = list(set().union(suffixes, DEFAULT_SUFFIXES_LIST))

        if mfcontent is None:
            self._mfcontent = MFContentV1(
                criteria=lambda x: hasattr(x, "METAFLOW_PACKAGE"),
            )
        else:
            self._mfcontent = mfcontent
        # We exclude the environment when packaging as this will be packaged separately.
        # This comes into play primarily if packaging from a node already running packaged
        # code.
        # These directories are only excluded at the top-level (ie: not further down
        # in sub-directories)
        # "_escape_trampolines" is a special directory where trampoline escape hatch
        # files are stored (used by Netflix Extension's Conda implementation).
        self.exclude_tl_dirs = (
            self._mfcontent.get_excluded_tl_entries()
            + ["_escape_trampolines"]
            + (exclude_tl_dirs or [])
        )

        self.environment = environment
        self.environment.init_environment(echo)

        self.metaflow_root = get_metaflow_root()

        self._flow = flow
        self._backend = backend
        self.create_time = time.time()

        # Can be called without a flow to package other things like functions.
        if self._flow:
            for step in self._flow:
                for deco in step.decorators:
                    deco.package_init(flow, step.__name__, environment)
            self.name = f"flow {self._flow.name}"
        else:
            self.name = "generic code"

        # Add metacontent
        self._mfcontent.add_info(
            self.environment.get_environment_info(include_ext_info=True)
        )

        if self._flow:
            self._mfcontent.add_config(dump_config_values(self._flow))

            # Add user files (from decorators and environment)
            self._add_addl_files()

        self._blob = None

    @property
    def blob(self):
        if self._blob is None:
            self._blob = self._make()
        return self._blob

    @property
    def package_version(self):
        return self._mfcontent.get_package_version()

    @classmethod
    def get_post_extract_commands(cls, version_id):
        return MFContent.get_post_extract_commands(version_id)

    def path_tuples(self):
        # Files included in the environment
        for path, arcname in self._mfcontent.content_names():
            yield path, arcname

        # Files included in the user code
        for path, arcname in self._user_code_tuples():
            yield path, arcname

    def _add_addl_files(self):
        # Look at all decorators that provide additional files
        deco_module_paths = {}
        addl_modules = set()

        def _check_tuple(path_tuple):
            if len(path_tuple) == 2:
                path_tuple = (
                    path_tuple[0],
                    path_tuple[1],
                    self.CODE_CONTENT,
                )
            file_path, file_name, file_type = path_tuple
            if file_type == self.MODULE_CONTENT:
                if file_path in addl_modules:
                    return None  # Module was already added -- we don't add twice
                addl_modules.add(file_path)
            elif file_type in (self.OTHER_CONTENT, self.CODE_CONTENT):
                path_tuple = (os.path.realpath(path_tuple[0]), path_tuple[1], file_type)
                # These are files
                # Check if the path is not duplicated as
                # many steps can have the same packages being imported
                if file_name not in deco_module_paths:
                    deco_module_paths[file_name] = file_path
                elif deco_module_paths[file_name] != file_path:
                    raise NonUniqueFileNameToFilePathMappingException(
                        file_name, [deco_module_paths[file_name], file_path]
                    )
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            return path_tuple

        def _add_tuple(path_tuple):
            file_path, file_name, file_type = path_tuple
            if file_type == self.MODULE_CONTENT:
                # file_path is actually a module
                self._mfcontent.add_module(cast(ModuleType, file_path))
            elif file_type == self.CODE_CONTENT:
                self._mfcontent.add_code_file(file_path, file_name)
            elif file_type == self.OTHER_CONTENT:
                self._mfcontent.add_other_file(file_path, file_name)

        for step in self._flow:
            for deco in step.decorators:
                for path_tuple in deco.add_to_package():
                    path_tuple = _check_tuple(path_tuple)
                    if path_tuple is None:
                        continue
                    _add_tuple(path_tuple)

        # the package folders for environment
        for path_tuple in self.environment.add_to_package():
            path_tuple = _check_tuple(path_tuple)
            if path_tuple is None:
                continue
            _add_tuple(path_tuple)

    def _user_code_tuples(self):
        if R.use_r():
            # the R working directory
            for path_tuple in walk("%s/" % R.working_dir(), suffixes=self.suffixes):
                yield path_tuple
            # the R package
            for path_tuple in R.package_paths():
                yield path_tuple
        else:
            # the user's working directory
            flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"

            for path_tuple in walk(
                flowdir, suffixes=self.suffixes, exclude_tl_dirs=self.exclude_tl_dirs
            ):
                # TODO: This is where we will check if the file is already included
                # in the mfcontent portion
                yield path_tuple

    def _make(self):
        backend = self._backend()
        with backend.create() as archive:
            # Package the environment
            for path_or_bytes, arcname in self._mfcontent.content():
                if isinstance(path_or_bytes, str):
                    archive.add_file(path_or_bytes, arcname=arcname)
                else:
                    archive.add_data(BytesIO(path_or_bytes), arcname=arcname)

            # Package the user code
            for path, arcname in self._user_code_tuples():
                archive.add_file(path, arcname=arcname)
        return backend.get_blob()

    def __str__(self):
        return f"<code package for {self.name} (created @ {self.create_time})>"
