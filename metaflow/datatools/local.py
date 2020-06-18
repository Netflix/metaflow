import os

from .. import FlowSpec
from ..exception import MetaflowException
from ..metaflow_config import DATATOOLS_LOCALROOT, DATATOOLS_SUFFIX
from ..util import to_unicode

from shutil import copyfile

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

class MetaflowLocalURLException(MetaflowException):
    headline = 'Invalid path'

class MetaflowLocalNotFound(MetaflowException):
    headline = 'Local object not found'

class LocalObject(object):
    """
    This object represents a local object. It is a very thin wrapper
    to allow it to be used in the same way as the S3Object

    Get or list calls return one or more of LocalObjects.
    """

    def __init__(self, url, path, size=None):

        # all fields of S3Object should return a unicode object
        def ensure_unicode(x):
            return None if x is None else to_unicode(x)
        path = ensure_unicode(path)

        self._size = size
        self._path = path
        self._url = url
        self._prefix = None
        self._key = None

        if self._path:
            try:
                self._size = os.stat(self._path).st_size
            except FileNotFoundError:
                self._size = 0
                self._path = None
            else:
                self._prefix = os.path.dirname(self._path)
                self._key = os.path.basename(self._path)


    @property
    def exists(self):
        """
        Does this key correspond to an actual file?
        """
        return self._path is not None and os.path.isfile(self._path)

    @property
    def downloaded(self):
        """
        Has this object been downloaded?
        """
        return True

    @property
    def url(self):
        """
        Local location of the object; this is the path prefixed with local://
        """
        return self._url

    @property
    def prefix(self):
        """
        Prefix requested that matches the object.
        """
        return self._prefix

    @property
    def key(self):
        """
        Key corresponds to the filename
        """
        return self._key

    @property
    def path(self):
        """
        Path to the local file
        """
        return self._path

    @property
    def blob(self):
        """
        Contents of the object as a byte string.

        Returns None if the object does not exist or is a directory.
        """
        if self.exists:
            with open(self._path, 'rb') as f:
                return f.read()

    @property
    def text(self):
        """
        Contents of the object as a Unicode string.

        Returns None if the object does not exist or is a directory.
        """
        if self.exists:
            return self.blob.decode('utf-8', errors='replace')

    @property
    def size(self):
        """
        Size of the object in bytes.

        Returns None if the object does not exist or is a directory
        """
        if self.exists:
            return self._size

    def __str__(self):
        if self.exists:
            return '<LocalObject %s (%d bytes, local)>' % (self._url, self._size)
        else:
            return '<LocalObject %s (object does not exist)>' % self._url

    def __repr__(self):
        return str(self)

class Local(object):

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        result = DATATOOLS_LOCALROOT
        if result is None:
            from ..datastore.local import LocalDataStore
            result = LocalDataStore.get_datastore_root_from_config(echo, create_on_absent)
            result = os.path.join(result, DATATOOLS_SUFFIX)
            if create_on_absent and not os.path.exists(result):
                os.mkdir(result)
        return result

    def __init__(self,
                 prefix=None,
                 run=None,
                 localroot=None):
        """
        Initialize a new context for Local file operations. This object is based used as
        a context manager for a with statement.

        There are two ways to initialize this object depending whether you want
        to bind paths to a Metaflow run or not.

        1. With a run object:

            run: (required) Either a FlowSpec object (typically 'self') or a
                 Run object corresponding to an existing Metaflow run. These
                 are used to add a version suffix in the local path.
            prefix: (optional) Local path prefix.

        2. Without a run object:

            localroot: (optional) An S3 root URL for all operations. If this is
                    not specified, all operations require a full URL (either local:// or /)
        """

        if run:
            # 1. use a (current) run ID with optional customizations
            parsed = urlparse(DATATOOLS_LOCALROOT)
            if not prefix:
                prefix = DATATOOLS_LOCALROOT
            else:
                prefix = os.path.join(DATATOOLS_LOCALROOT, prefix.strip('/'))
            if isinstance(run, FlowSpec):
                if current.is_running_flow:
                    prefix = os.path.join(prefix,
                                          current.flow_name,
                                          current.run_id)
                else:
                    raise MetaflowLocalURLException(\
                        "Initializing Local access with a FlowSpec outside of a running "
                        "flow is not supported.")
            else:
                prefix = os.path.join(prefix, run.parent.id, run.id)

            self._localroot = u'%s' % prefix
        elif localroot:
            # 2. use an explicit local path
            self._localroot = to_unicode(localroot)
            if self._localroot.startswith(u'local://'):
                self._localroot = self._localroot[8:]
            elif self._localroot[0] != u'/':
                raise MetaflowLocalURLException(
                    "Local root needs to be absolute path or start with local://")
        else:
            # 3. use the client only with full URLs
            self._localroot = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def close(self):
        pass

    def _path(self, key):
        if self._localroot is None:
            key = to_unicode(key)
            if key.startswith(u'local://'):
                return key[8:]
            elif key[0] != u'/':
                if current.is_running_flow:
                    raise MetaflowLocalURLException(
                        "Specify Local(run=self) when you use Local inside a running "
                        "flow. Otherwise you have to use Local with full "
                        "local:// urls or absolute paths.")
                else:
                    raise MetaflowLocalURLException(
                        "Initialize Local with an 'localroot' or 'run' if you don't "
                        "want to specify full local:// urls or absolute paths.")
            else:
                return key
        elif key:
            key = to_unicode(key)
            if key.startswith(u'local://') or key.startswith(u'/'):
                raise MetaflowLocalURLException(\
                    "Don't use absolute paths when the Local client is "
                    "initialized with a prefix. URL: %s" % key)
            return os.path.join(self._localroot, key)
        else:
            return self._localroot

    def list_paths(self, keys=None):
        if keys is None:
            keys = [None]
        result = []
        for key in keys:
            d = self._path(key)
            if os.path.isdir(d):
                result.extend(
                    [LocalObject(u'local://%s' % fp, fp) for fp in 
                        [os.path.join(d, p) for p in os.listdir(d)]])
        return result

    def list_recursive(self, keys=None):
        if keys is None:
            keys = [None]
        result = []
        dirs = [d for d in [self._path(k) for k in keys] if os.path.isdir(d)]
        while dirs:
            new_dirs = []
            for d in dirs:
                r = [os.path.join(d, f) for f in os.listdir(d)]
                result.extend([LocalObject(u'local://%s' % p, p) for p in r if os.path.isfile(p)])
                new_dirs.extend([p for p in r if os.path.isdir(p)])
            dirs = new_dirs
        return result

    def get(self, key=None, return_missing=False):
        p = self._path(key)
        url = u'local://%s' % p
        if not os.path.isfile(p):
            if return_missing:
                p = None
            else:
                raise MetaflowLocalNotFound("Local URL %s not found" % url)
        return LocalObject(url, p)

    def get_many(self, keys, return_missing=False):
        return [self.get(k, return_missing) for k in keys]

    def get_recursive(self, keys):
        new_keys = self.list_recursive(keys)
        return self.get_many(new_keys) 

    def get_all(self):
        if self._localroot is None:
            raise MetaflowLocalURLException(
                "Can't get_all() when Local is initialized without a prefix")
        else:
            return self.get_recursive([None])

    def put(self, key, obj, overwrite=True):
        p = self._path(key)
        if overwrite or (not os.path.exists(p)):
            with open(p, 'wb') as f:
                f.write(obj)
        return u'local://%s' % p

    def put_many(self, key_objs, overwrite=True):
        return [self.put(o[0], o[1], overwrite) for o in key_objs]

    def put_files(self, key_objs, overwrite=True):
        result = []
        for key, path in key_objs:
            if not os.path.isfile(local_file):
                raise MetaflowLocalNotFound("Local file to put not found: %s" % path)
            out_path = self._path(key)
            if overwrite or (not os.path.exists(out_path)):
                copyfile(local_file, out_path)
                result.append(u'local://%' % out_path)
        return result






