import os
import sys
import tarfile
import time
import json
from io import BytesIO


from ..metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from ..exception import MetaflowException
from ..special_files import SpecialFile
from ..user_configs.config_parameters import dump_config_values
from .. import R

from .mfenv import MFEnv

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
    def __init__(self, flow, environment, echo, suffixes=DEFAULT_SUFFIXES_LIST):
        self.suffixes = list(set().union(suffixes, DEFAULT_SUFFIXES_LIST))
        self.environment = environment
        self.metaflow_root = os.path.dirname(__file__)

        self.flow_name = flow.name
        self._flow = flow
        self.create_time = time.time()
        environment.init_environment(echo)
        for step in flow:
            for deco in step.decorators:
                deco.package_init(flow, step.__name__, environment)

        self._code_env = MFEnv(lambda x: hasattr(x, "METAFLOW_PACKAGE"))

        # Add special content
        self._code_env.add_special_content(
            SpecialFile.INFO_FILE,
            json.dumps(
                self.environment.get_environment_info(include_ext_info=True)
            ).encode("utf-8"),
        )

        self._code_env.add_special_content(
            SpecialFile.CONFIG_FILE,
            json.dumps(dump_config_values(self._flow)).encode("utf-8"),
        )

        # Add user files (from decorators) -- we add these to the code environment
        self._code_env.add_files(self._addl_files())

        self.blob = self._make()

    def path_tuples(self):
        # Package the environment
        for path, arcname in self._code_env.files():
            yield path, arcname
        for _, arcname in self._code_env.contents():
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
            flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"
            for path_tuple in MFEnv.walk(flowdir, suffixes=self.suffixes):
                # TODO: This is where we will check if the file is already included
                # in the mfenv portion using path_in_archive. If it is, we just need to
                # include a symlink.
                yield path_tuple

    @staticmethod
    def _add_file(tar, filename, buf):
        info = tarfile.TarInfo(filename)
        buf.seek(0)
        info.size = len(buf.getvalue())
        # Setting this default to Dec 3, 2019
        info.mtime = 1575360000
        tar.addfile(info, buf)

    def _make(self):
        def no_mtime(tarinfo):
            # a modification time change should not change the hash of
            # the package. Only content modifications will.
            # Setting this default to Dec 3, 2019
            tarinfo.mtime = 1575360000
            return tarinfo

        buf = BytesIO()
        with tarfile.open(
            fileobj=buf, mode="w:gz", compresslevel=3, dereference=True
        ) as tar:
            # Package the environment
            for path, arcname in self._code_env.files():
                tar.add(path, arcname=arcname, recursive=False, filter=no_mtime)
            for content, arcname in self._code_env.contents():
                self._add_file(tar, arcname, BytesIO(content))

            # Package the user code
            for path, arcname in self._user_code_tuples():
                tar.add(path, arcname=arcname, recursive=False, filter=no_mtime)

        blob = bytearray(buf.getvalue())
        blob[4:8] = [0] * 4  # Reset 4 bytes from offset 4 to account for ts
        return blob

    def __str__(self):
        return "<code package for flow %s (created @ %s)>" % (
            self.flow_name,
            time.strftime("%a, %d %b %Y %H:%M:%S", self.create_time),
        )
