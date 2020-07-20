import gzip
import struct

from collections import namedtuple
from hashlib import sha1
from io import BytesIO

from ..exception import MetaflowInternalError
from .common import DataException

class ContentAddressedStore(object):
    """
    A Content-Addressed Store stores blobs of data in a way that eliminates
    storing the same blob twice.

    This class is not meant to be overridden and is meant to be common across
    different datastores.
    The DataStoreBackend implementation can differ though (S3, GCS, local, ...)).
    """

    save_blobs_result = namedtuple('save_blobs_result', 'uri key')

    def __init__(self, flow_datastore, prefix):
        """
        Initialize a ContentAddressedStore

        A content-addressed store stores data using a name/key that is a hash
        of the content. This means that duplicate content is only stored once.

        Parameters
        ----------
        flow_datastore : FlowDataStore
            Flow-level datastore (parent of this and any TaskDataStore)
        prefix : string
            "Directory" that will be prepended when storing a file
        """
        self._prefix = prefix
        self._backend = flow_datastore._backend
        self._logger = flow_datastore.logger
        self._monitor = flow_datastore.monitor
        self._blob_cache = flow_datastore.blob_cache

        self._magic = b'.MFBlob'
        # Pack format for header of non-raw blobs.
        # The integer corresponds to the version number of the encoding
        self._info_format = '<%dsi' % len(self._magic)

        self.TYPE = self._backend.TYPE

    def save_blobs(self, blobs, raw=False):
        """
        Saves blobs of data to the datastore

        The blobs of data are saved as is if raw is True. If raw is False, the
        datastore may process the blobs and they should then only be loaded
        using load_blob

        NOTE: The idea here is that there would be two modes to access
        the file once saved to the datastore:
          - if raw is True, you would be able to access it directly using the
            URI returned; the bytes that are passed in as 'blob' would be
            returned directly by reading the object at that URI. You would also
            be able to access it using load_blob passing the key returned
          - if raw is False, no URI would be returned (the URI would be None)
            and you would only be able to access the object using load_blob.
          - The API also specifically takes a list to allow for parallel writes
            if available in the datastore. We could also make a single
            save_blob' API and save_blobs but this seems superfluous

        Parameters
        ----------
        blobs : List of bytes objects
            Blobs to save. Each blob should be bytes
        raw : bool, optional
            Whether to save the bytes directly or process them, by default False

        Returns
        -------
        List of save_blobs_result:
            The list order is the same as the blobs passed in. The URI will be
            None if raw is False.
        """
        to_save = {}
        results = []
        for blob in blobs:
            sha = sha1(blob).hexdigest()
            key = 'r_%s' % sha if raw else sha
            path = self._backend.path_join(self._prefix, sha[:2], key)
            results.append(self.save_blobs_result(
                uri=self._backend.full_uri(path) if raw else None,
                key=key))
            if self._backend.is_file(path):
                # This already exists in the backing datastore so we can skip it
                continue
            if raw:
                blob = BytesIO(blob)
            else:
                buf = BytesIO()
                t = buf.write(struct.pack(self._info_format, self._magic, 1))
                if t != struct.calcsize(self._info_format):
                    raise MetaflowInternalError(
                        "Cannot write format in buffer -- this is bad")
                blob = self._pack_v1(buf, blob)
                blob.seek(0)
            if self._blob_cache is not None:
                self._blob_cache.register(key, blob.read(), write=True)
                blob.seek(0)
            to_save[path] = blob
        # We don't actually want to overwrite but by saying =True, we avoid
        # checking again saving some operations. We are already sure we are not
        # sending duplicate files since we already checked.
        self._backend.save_bytes(to_save, overwrite=True)
        return results

    def load_blobs(self, keys, force_raw=False):
        """
        Mirror function of save_blobs

        This function is guaranteed to return the bytes passed to save_blob for
        the keys

        Parameters
        ----------
        keys : List of string
            Key describing the object to load
        force_raw : bool, optional
            Support for backward compatibility with previous datastores. If
            True, this will force the key to be loaded as is (raw). By default,
            False

        Returns
        -------
        Dict: string -> bytes:
            Returns the blobs as bytes
        """
        to_load = []
        results = {}
        if self._blob_cache is not None:
            for k in keys:
                v = self._blob_cache.load(k)
                if v is not None:
                    results[k] = v
                else:
                    to_load.append(k)
        else:
            to_load = keys
        to_load_paths = [
            self._backend.path_join(
                self._prefix, k[:2] if not k.startswith('r_') else k[2:4], k)
            for k in to_load]
        load_results = self._backend.load_bytes(to_load_paths)
        for k, path in zip(to_load, to_load_paths):
            # At this point, we either return the object as is (if raw) or
            # decode it according to the encoding version
            result = load_results[path]
            if force_raw or k.startswith('r_'):
                with result as r:
                    results[k] = r.read()
            else:
                with result as r:
                    magic, version = struct.unpack(
                        self._info_format,
                        r.read(struct.calcsize(self._info_format)))
                    try:
                        # We need to see if this is an artifact saved by this
                        # version (or newer) of the datastore. Prior to this,
                        # things were saved gzipped but with no magic string.
                        if magic == self._magic:
                            unpack_code = getattr(self, '_unpack_v%d' % version)
                        else:
                            # This is the backward compatible mode
                            r.seek(0)
                            unpack_code = getattr(
                                self, '_unpack_backward_compatible')
                    except:
                        raise DataException(
                            "Unknown encoding version %d for %s -- the artifact "
                            "is either corrupt or you need to update Metaflow"
                            % (version, path))
                    try:
                        results[k] = unpack_code(r)
                    except Exception as e:
                        raise DataException(
                            "Could not unpack data: %s" % e)

            if self._blob_cache is not None:
                self._blob_cache.register(k, results[k])
        return results

    def _unpack_backward_compatible(self, blob):
        # This is the backward compatible unpack
        # (if the blob doesn't have a version encoded)
        return self._unpack_v1(blob)

    def _pack_v1(self, buf, blob):
        with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=3) as f:
            f.write(blob)
        return buf

    def _unpack_v1(self, blob):
        with gzip.GzipFile(fileobj=blob, mode='rb') as f:
            return f.read()
