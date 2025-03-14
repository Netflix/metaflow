import os
import sys
import tarfile
import time
import json

from abc import ABC, abstractmethod
from io import BytesIO


from .mfenv import MFEnv
from .tar_backend import TarPackagingBackend
from .utils import walk
from ..metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from ..exception import MetaflowException
from ..meta_files import MFENV_DIR, MetaFile
from ..user_configs.config_parameters import dump_config_values
from .. import R

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
    def __init__(
        self,
        flow,
        environment,
        echo,
        suffixes=DEFAULT_SUFFIXES_LIST,
        code_env=None,
        user_dir=None,
        exclude_tl_dirs=None,
        package_code_env_path=MFENV_DIR,
        package_user_path=None,
        backend=TarPackagingBackend,
    ):
        self.suffixes = list(set().union(suffixes, DEFAULT_SUFFIXES_LIST))

        # We exclude the environment when packaging as this will be packaged separately.
        # This comes into play primarily if packaging from a node already running packaged
        # code.
        # These directories are only excluded at the top-level (ie: not further down
        # in sub-directories)
        # "_escape_trampolines" is a special directory where trampoline escape hatch
        # files are stored (used by Netflix Extension's Conda implementation).
        self.exclude_tl_dirs = [MFENV_DIR, "_escape_trampolines"] + (
            exclude_tl_dirs or []
        )

        self.package_user_path = package_user_path
        self.user_dir = user_dir

        self.environment = environment
        self.environment.init_environment(echo)

        self.metaflow_root = os.path.dirname(__file__)

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

        self._code_env = code_env or MFEnv(
            lambda x: hasattr(x, "METAFLOW_PACKAGE"),
            package_path=package_code_env_path,
        )

        # Add metacontent
        self._code_env.add_meta_content(
            MetaFile.INFO_FILE,
            json.dumps(
                self.environment.get_environment_info(include_ext_info=True)
            ).encode("utf-8"),
        )

        if self._flow:
            self._code_env.add_meta_content(
                MetaFile.CONFIG_FILE,
                json.dumps(dump_config_values(self._flow)).encode("utf-8"),
            )

            # Add user files (from decorators) -- we add these to the code environment
            self._code_env.add_files(self._addl_files())

        self.blob = self._make()

    def path_tuples(self):
        # Package the environment
        for path, arcname in self._code_env.files():
            yield path, arcname
        for _, arcname in self._code_env.metacontents():
            yield f"<generated>{arcname}", arcname

        # Package the user code
        for path, arcname in self._user_code_tuples():
            yield path, arcname

    def _addl_files(self):
        # Look at all decorators that provide additional files
        deco_module_paths = {}
        for step in self._flow:
            for deco in step.decorators:
                for path_tuple in deco.add_to_package():
                    file_path, file_name = path_tuple
                    # Check if the path is not duplicated as
                    # many steps can have the same packages being imported
                    if file_name not in deco_module_paths:
                        deco_module_paths[file_name] = file_path
                        yield path_tuple
                    elif deco_module_paths[file_name] != file_path:
                        raise NonUniqueFileNameToFilePathMappingException(
                            file_name, [deco_module_paths[file_name], file_path]
                        )

        # the package folders for environment
        for path_tuple in self.environment.add_to_package():
            yield path_tuple

    def _user_code_tuples(self):
        if R.use_r():
            # the R working directory
            for path_tuple in MFEnv.walk(
                "%s/" % R.working_dir(), suffixes=self.suffixes
            ):
                yield path_tuple
            # the R package
            for path_tuple in R.package_paths():
                yield path_tuple
        else:
            # the user's working directory
            if self.user_dir:
                flowdir = os.path.abspath(self.user_dir)
            else:
                flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"

            for path_tuple in walk(
                flowdir, suffixes=self.suffixes, exclude_tl_dirs=self.exclude_tl_dirs
            ):
                # TODO: This is where we will check if the file is already included
                # in the mfenv portion using path_in_archive. If it is, we just need to
                # include a symlink.
                if self.package_user_path:
                    yield (
                        path_tuple[0],
                        os.path.join(self.package_user_path, path_tuple[1]),
                    )
                else:
                    yield path_tuple

    def _make(self):
        backend = self._backend()
        with backend.create() as archive:
            # Package the environment
            for path, arcname in self._code_env.files():
                archive.add_file(path, arcname=arcname)
            for content, arcname in self._code_env.metacontents():
                archive.add_data(BytesIO(content), arcname)

            # Package the user code
            for path, arcname in self._user_code_tuples():
                archive.add_file(path, arcname=arcname)
        return backend.get_blob()

    def __str__(self):
        return f"<code package for {self.name} (created @ {self.create_time})>"
