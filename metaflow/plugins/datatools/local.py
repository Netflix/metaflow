import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DATATOOLS_LOCALROOT, DATATOOLS_SUFFIX
from metaflow.util import to_unicode


class MetaflowLocalURLException(MetaflowException):
    headline = "Invalid path"


class MetaflowLocalNotFound(MetaflowException):
    headline = "Local object not found"


class LocalObject(object):
    """
    This object represents a local object. It is a very thin wrapper
    to allow it to be used in the same way as the S3Object (only as needed
    in the IncludeFile use case)

    Get or list calls return one or more of LocalObjects.
    """

    def __init__(self, url, path):

        # all fields of S3Object should return a unicode object
        def ensure_unicode(x):
            return None if x is None else to_unicode(x)

        path = ensure_unicode(path)

        self._path = path
        self._url = url

        if self._path:
            try:
                os.stat(self._path)
            except FileNotFoundError:
                self._path = None

    @property
    def exists(self):
        """
        Does this key correspond to an actual file?
        """
        return self._path is not None and os.path.isfile(self._path)

    @property
    def url(self):
        """
        Local location of the object; this is the path prefixed with local://
        """
        return self._url

    @property
    def path(self):
        """
        Path to the local file
        """
        return self._path

    @property
    def size(self):
        """
        Size of the local file (in bytes)

        Returns None if the key does not correspond to an actual object
        """
        if self._path is None:
            return None
        return os.stat(self._path).st_size


class Local(object):
    """
    This class allows you to access the local filesystem in a way similar to the S3 datatools
    client. It is a stripped down version for now and only implements the functionality needed
    for this use case.

    In the future, we may want to allow it to be used in a way similar to the S3() client.
    """

    TYPE = "local"

    @staticmethod
    def _makedirs(path):
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        result = DATATOOLS_LOCALROOT
        if result is None:
            from metaflow.plugins.datastores.local_storage import LocalStorage

            result = LocalStorage.get_datastore_root_from_config(echo, create_on_absent)
            result = os.path.join(result, DATATOOLS_SUFFIX)
            if create_on_absent and not os.path.exists(result):
                os.mkdir(result)
        return result

    def __init__(self):
        """
        Initialize a new context for Local file operations. This object is based used as
        a context manager for a with statement.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _path(self, key):
        key = to_unicode(key)
        if key.startswith("local://"):
            return key[8:]
        else:
            return key

    def get(self, key=None, return_missing=False):
        p = self._path(key)
        url = "local://%s" % p
        if not os.path.isfile(p):
            if return_missing:
                p = None
            else:
                raise MetaflowLocalNotFound("Local URL '%s' not found" % url)
        return LocalObject(url, p)

    def put(self, key, obj, overwrite=True):
        p = self._path(key)
        if overwrite or (not os.path.exists(p)):
            Local._makedirs(os.path.dirname(p))
            with open(p, "wb") as f:
                f.write(obj)
        return "local://%s" % p

    def info(self, key=None, return_missing=False):
        p = self._path(key)
        url = "local://%s" % p
        if not os.path.isfile(p):
            if return_missing:
                p = None
            else:
                raise MetaflowLocalNotFound("Local URL '%s' not found" % url)
        return LocalObject(url, p)
