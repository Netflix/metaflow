import io
import os
import shutil
import uuid
from tempfile import mkdtemp

from metaflow.exception import MetaflowException, MetaflowInternalError


class GS(object):

    TYPE = "gs"

    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        from metaflow.metaflow_config import DATATOOLS_GSROOT

        return DATATOOLS_GSROOT

    def __init__(self):
        # This local directory is used to house any downloaded blobs, for lifetime of
        # this object as a context manager.
        self._tmpdir = None

    def _get_storage_backend_and_subpath(self, key):
        """
        Return an GSDatastore, rooted at the container level, no prefix.
        Key MUST be a fully qualified path. e.g. gs://<bucket_name>/b/l/o/b/n/a/m/e
        """
        from metaflow.plugins.gcp.gs_utils import parse_gs_full_path

        # we parse out the bucket name only, and use that to root our storage implementation
        bucket_name, subpath = parse_gs_full_path(key)
        # Import DATASTORES dynamically... otherwise, circular import
        from metaflow.plugins import DATASTORES

        storage_impl = [d for d in DATASTORES if d.TYPE == "gs"][0]
        return storage_impl("gs://" + bucket_name), subpath

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir)

    def get(self, key=None, return_missing=False):
        """Key MUST be a fully qualified path.  gs://<bucket_name>/b/l/o/b/n/a/m/e"""
        if not return_missing:
            raise MetaflowException("GS object supports only return_missing=True")
        if not key.startswith("gs://"):
            raise MetaflowInternalError(
                msg="Expected GS object key to start with 'gs://'"
            )
        storage, subpath = self._get_storage_backend_and_subpath(key)
        gs_object = None
        with storage.load_bytes([subpath]) as load_result:
            for _, tmpfile, _ in load_result:
                if tmpfile is None:
                    gs_object = GSObject(key, None, False, None)
                else:
                    if not self._tmpdir:
                        self._tmpdir = mkdtemp(prefix="metaflow.includefile.gs.")
                    output_file_path = os.path.join(self._tmpdir, str(uuid.uuid4()))
                    shutil.move(tmpfile, output_file_path)
                    sz = os.stat(output_file_path).st_size
                    gs_object = GSObject(key, output_file_path, True, sz)
                break
        return gs_object

    def put(self, key, obj, overwrite=True):
        """Key MUST be a fully qualified path.  gs://<bucket_name>/b/l/o/b/n/a/m/e"""
        storage, subpath = self._get_storage_backend_and_subpath(key)
        storage.save_bytes([(subpath, io.BytesIO(obj))], overwrite=overwrite)
        return key

    def info(self, key=None, return_missing=False):
        if not key.startswith("gs://"):
            raise MetaflowInternalError(
                msg="Expected GS object key to start with 'gs://'"
            )
        storage, subpath = self._get_storage_backend_and_subpath(key)
        blob_size = storage.size_file(subpath)
        blob_exists = blob_size is not None
        if not blob_exists and not return_missing:
            raise MetaflowException("GS object '%s' not found" % key)
        return GSObject(key, None, blob_exists, blob_size)


class GSObject(object):
    def __init__(self, url, path, exists, size):
        self._path = path
        self._url = url
        self._exists = exists
        self._size = size

    @property
    def path(self):
        return self._path

    @property
    def url(self):
        return self._url

    @property
    def exists(self):
        return self._exists

    @property
    def size(self):
        return self._size
