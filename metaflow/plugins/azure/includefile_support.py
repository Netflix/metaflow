import io
import os
import shutil
import uuid
from tempfile import mkdtemp

from metaflow.exception import MetaflowException, MetaflowInternalError


class Azure(object):
    @classmethod
    def get_root_from_config(cls, echo, create_on_absent=True):
        from metaflow.metaflow_config import DATATOOLS_AZUREROOT

        return DATATOOLS_AZUREROOT

    def __init__(self):
        # This local directory is used to house any downloaded blobs, for lifetime of
        # this object as a context manager.
        self._tmpdir = None

    def _get_storage_backend(self, key):
        """
        Return an AzureDatastore, rooted at the container level, no prefix.
        Key MUST be a fully qualified path. e.g. <container_name>/b/l/o/b/n/a/m/e
        """
        from metaflow.plugins.azure.azure_utils import parse_azure_full_path

        # we parse out the container name only, and use that to root our storage implementation
        container_name, _ = parse_azure_full_path(key)
        # Import DATASTORES dynamically... otherwise, circular import
        from metaflow.datastore import DATASTORES

        storage_impl = DATASTORES["azure"]
        return storage_impl(container_name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir)

    def get(self, key=None, return_missing=False):
        """Key MUST be a fully qualified path with uri scheme.  azure://<container_name>/b/l/o/b/n/a/m/e"""
        # Azure.get() is meant for use within includefile.py ONLY.
        # All existing call sites set return_missing=True.
        #
        # Support for return_missing=False may be added if/when the situation changes.
        if not return_missing:
            raise MetaflowException("Azure object supports only return_missing=True")
        # We fabricate a uri scheme to fit into existing includefile code (just like local://)
        if not key.startswith("azure://"):
            raise MetaflowInternalError(
                msg="Expected Azure object key to start with 'azure://'"
            )
        uri_style_key = key
        short_key = key[8:]
        storage = self._get_storage_backend(short_key)
        azure_object = None
        with storage.load_bytes([short_key]) as load_result:
            for _, tmpfile, _ in load_result:
                if tmpfile is None:
                    azure_object = AzureObject(uri_style_key, None, False, None)
                else:
                    if not self._tmpdir:
                        self._tmpdir = mkdtemp(prefix="metaflow.includefile.azure.")
                    output_file_path = os.path.join(self._tmpdir, str(uuid.uuid4()))
                    shutil.move(tmpfile, output_file_path)
                    # Beats making another Azure API call!
                    sz = os.stat(output_file_path).st_size
                    azure_object = AzureObject(
                        uri_style_key, output_file_path, True, sz
                    )
                break
        return azure_object

    def put(self, key, obj, overwrite=True):
        """Key MUST be a fully qualified path.  <container_name>/b/l/o/b/n/a/m/e"""
        storage = self._get_storage_backend(key)
        storage.save_bytes([(key, io.BytesIO(obj))], overwrite=overwrite)
        # We fabricate a uri scheme to fit into existing includefile code (just like local://)
        return "azure://%s" % key

    def info(self, key=None, return_missing=False):
        # We fabricate a uri scheme to fit into existing includefile code (just like local://)
        if not key.startswith("azure://"):
            raise MetaflowInternalError(
                msg="Expected Azure object key to start with 'azure://'"
            )
        # aliasing this purely for clarity
        uri_style_key = key
        short_key = key[8:]
        storage = self._get_storage_backend(short_key)
        blob_size = storage.size_file(short_key)
        blob_exists = blob_size is not None
        if not blob_exists and not return_missing:
            raise MetaflowException("Azure blob '%s' not found" % uri_style_key)
        return AzureObject(uri_style_key, None, blob_exists, blob_size)


class AzureObject(object):
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
