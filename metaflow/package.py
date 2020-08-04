import os
import sys
import tarfile
import json
from hashlib import sha1
from itertools import chain

from .util import to_unicode
from . import R

try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO
except:
    # python3
    import io
    BytesIO = io.BytesIO

DEFAULT_SUFFIXES = ['.py', '.R', '.RDS']


class MetaflowPackage(object):

    def __init__(self, flow, environment, logger, suffixes=DEFAULT_SUFFIXES):
        self.suffixes = list(set().union(suffixes, DEFAULT_SUFFIXES))
        self.environment = environment
        self.metaflow_root = os.path.dirname(__file__)
        environment.init_environment(logger)
        for step in flow:
            for deco in step.decorators:
                deco.package_init(flow,
                                  step.__name__,
                                  environment)
        self.blob, self.sha = self._make()

    def _walk(self, root, exclude_hidden=True):
        root = to_unicode(root)  # handle files/folder with non ascii chars
        prefixlen = len('%s/' % os.path.dirname(root))
        for path, dirs, files in os.walk(root):
            if exclude_hidden and '/.' in path:
                continue
            # path = path[2:] # strip the ./ prefix
            # if path and (path[0] == '.' or './' in path):
            #    continue
            for fname in files:
                if fname[0] == '.':
                    continue
                if any(fname.endswith(suffix) for suffix in self.suffixes):
                    p = os.path.join(path, fname)
                    yield p, p[prefixlen:]

    def path_tuples(self):
        """
        Returns list of (path, arcname) to be added to the job package, where
        `arcname` is the alternative name for the file in the package.
        """
        # We want the following contents in the tarball
        # Metaflow package itself
        for path_tuple in self._walk(self.metaflow_root, exclude_hidden=False):
            yield path_tuple
        # the package folders for environment
        for path_tuple in self.environment.add_to_package():
            yield path_tuple
        if R.use_r():
            # the R working directory
            for path_tuple in self._walk('%s/' % R.working_dir()):
                yield path_tuple
            # the R package
            for path_tuple in R.package_paths():
                yield path_tuple
        else:
            # the user's working directory
            flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + '/'
            for path_tuple in self._walk(flowdir):
                yield path_tuple

    def _add_info(self, tar):
        info = tarfile.TarInfo('INFO')
        env = self.environment.get_environment_info()
        buf = BytesIO()
        buf.write(json.dumps(env).encode('utf-8'))
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
        with tarfile.TarFile(fileobj=buf, mode='w') as tar:
            self._add_info(tar)
            for path, arcname in self.path_tuples():
                tar.add(path, arcname=arcname,
                        recursive=False, filter=no_mtime)

        blob = buf.getvalue()
        return blob, sha1(blob).hexdigest()

    def __str__(self):
        return '<code package %s>' % self.sha
