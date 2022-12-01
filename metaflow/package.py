import importlib
import os
import sys
import tarfile
import time
import json
from io import BytesIO

from .extension_support import EXT_PKG, package_mfext_all
from .metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from .exception import MetaflowException
from .util import to_unicode
from . import R

DEFAULT_SUFFIXES_LIST = DEFAULT_PACKAGE_SUFFIXES.split(",")
METAFLOW_SUFFIXES_LIST = [".py", ".html", ".css", ".js"]


class NonUniqueFileNameToFilePathMappingException(MetaflowException):
    headline = "Non Unique file path for a file name included in code package"

    def __init__(self, filename, file_paths, lineno=None):
        msg = (
            "Filename %s included in the code package includes multiple different paths for the same name : %s.\n"
            "The `filename` in the `add_to_package` decorator hook requires a unique `file_path` to `file_name` mapping"
            % (filename, ", ".join(file_paths))
        )
        super().__init__(msg=msg, lineno=lineno)


# this is os.walk(follow_symlinks=True) with cycle detection
def walk_without_cycles(top_root):
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
        self.blob = self._make()

    def _walk(self, root, exclude_hidden=True, suffixes=None):
        if suffixes is None:
            suffixes = []
        root = to_unicode(root)  # handle files/folder with non ascii chars
        prefixlen = len("%s/" % os.path.dirname(root))
        for (
            path,
            files,
        ) in walk_without_cycles(root):
            if exclude_hidden and "/." in path:
                continue
            # path = path[2:] # strip the ./ prefix
            # if path and (path[0] == '.' or './' in path):
            #    continue
            for fname in files:
                if fname[0] == ".":
                    continue
                if any(fname.endswith(suffix) for suffix in suffixes):
                    p = os.path.join(path, fname)
                    yield p, p[prefixlen:]

    def path_tuples(self):
        """
        Returns list of (path, arcname) to be added to the job package, where
        `arcname` is the alternative name for the file in the package.
        """
        # We want the following contents in the tarball
        # Metaflow package itself
        for path_tuple in self._walk(
            self.metaflow_root, exclude_hidden=False, suffixes=METAFLOW_SUFFIXES_LIST
        ):
            yield path_tuple

        # Metaflow extensions; for now, we package *all* extensions but this may change
        # at a later date; it is possible to call `package_mfext_package` instead of
        # `package_mfext_all`
        for path_tuple in package_mfext_all():
            yield path_tuple

        # Any custom packages exposed via decorators
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
        if R.use_r():
            # the R working directory
            for path_tuple in self._walk(
                "%s/" % R.working_dir(), suffixes=self.suffixes
            ):
                yield path_tuple
            # the R package
            for path_tuple in R.package_paths():
                yield path_tuple
        else:
            # the user's working directory
            flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"
            for path_tuple in self._walk(flowdir, suffixes=self.suffixes):
                yield path_tuple

    def _add_info(self, tar):
        info = tarfile.TarInfo("INFO")
        env = self.environment.get_environment_info()
        buf = BytesIO()
        buf.write(json.dumps(env).encode("utf-8"))
        buf.seek(0)
        info.size = len(buf.getvalue())
        tar.addfile(info, buf)

    def _make(self):
        def no_mtime(tarinfo):
            # a modification time change should not change the hash of
            # the package. Only content modifications will.
            tarinfo.mtime = 0
            return tarinfo

        buf = BytesIO()
        with tarfile.open(
            fileobj=buf, mode="w:gz", compresslevel=3, dereference=True
        ) as tar:
            self._add_info(tar)
            for path, arcname in self.path_tuples():
                tar.add(path, arcname=arcname, recursive=False, filter=no_mtime)

        blob = bytearray(buf.getvalue())
        blob[4:8] = [0] * 4  # Reset 4 bytes from offset 4 to account for ts
        return blob

    def __str__(self):
        return "<code package for flow %s (created @ %s)>" % (
            self.flow_name,
            time.strftime("%a, %d %b %Y %H:%M:%S", self.create_time),
        )
