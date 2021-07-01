import gzip
import struct

from collections import namedtuple
from hashlib import sha1
from io import BytesIO

from ..exception import MetaflowInternalError
from .exceptions import DataException

class ContentAddressedStore(object):
    """
    A Content-Addressed Store stores blobs of data in a way that eliminates
    storing the same blob twice.

    This class is not meant to be overridden and is meant to be common across
    different datastores.
    The DataStoreBackend implementation can differ though (S3, GCS, local, ...)).
    """

    save_blobs_result = namedtuple('save_blobs_result', 'uri key')

    def __init__(self, prefix, backend):
        """
        Initialize a ContentAddressedStore

        A content-addressed store stores data using a name/key that is a hash
        of the content. This means that duplicate content is only stored once.

        Parameters
        ----------
        prefix : string
            "Directory" that will be prepended when storing a file
        backend_class : type
            Class for the backing DataStoreBackend to use; if not provided use
            default_backend_class
        """
        self._prefix = prefix
        self._backend = backend
        self.TYPE = self._backend.TYPE
        self._blob_cache = None

    def set_blob_cache(self, blob_cache):
        self._blob_cache = blob_cache

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
            path = self._backend.path_join(self._prefix, sha[:2], sha)
            results.append(self.save_blobs_result(
                uri=self._backend.full_uri(path) if raw else None,
                key=sha))
            if self._backend.is_file(path):
                # This already exists in the backing datastore so we can skip it
                continue
            # Compute the meta information to store with the file
            meta = {
                'cas_raw': raw,
                'cas_version': 1
            }
            if raw:
                blob = BytesIO(blob)
            else:
                blob = self._pack_v1(blob)

            to_save[path] = (blob, meta)
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
        results = {}
        load_paths = []
        for key in keys:
            blob = None
            if self._blob_cache:
                blob = self._blob_cache.load(key)
            if blob:
                results[key] = blob
            else:
                path = self._backend.path_join(self._prefix, key[:2], key)
                load_paths.append((key, path))

        new_results = {}
        with self._backend.load_bytes([p for _, p in load_paths]) as loaded:
            for key, path in load_paths:
                # At this point, we either return the object as is (if raw) or
                # decode it according to the encoding version
                file_path, meta = loaded[path]
                with open(file_path, 'rb') as f:
                    if force_raw or (meta and meta.get('cas_raw', False)):
                        new_results[key] = f.read()
                    else:
                        if meta is None:
                            # Previous version of the datastore had no meta
                            # information
                            unpack_code = self._unpack_backward_compatible
                        else:
                            version = meta.get('cas_version', -1)
                            if version == -1:
                                raise DataException(
                                    "Could not extract encoding version for %s"
                                    % path)
                            unpack_code = getattr(
                                self, '_unpack_v%d' % version, None)
                            if unpack_code is None:
                                raise DataException(
                                    "Unknown encoding version %d for %s -- "
                                    "the artifact is either corrupt or you need "
                                    "to update Metaflow" % (version, path))
                        try:
                            new_results[key] = unpack_code(f)
                        except Exception as e:
                            raise DataException(
                                "Could not unpack data: %s" % e)
        
        if self._blob_cache:
            self._blob_cache.store(new_results)

        results.update(new_results)
        return results

    def _unpack_backward_compatible(self, blob):
        # This is the backward compatible unpack
        # (if the blob doesn't have a version encoded)
        return self._unpack_v1(blob)

    def _pack_v1(self, blob):
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=3) as f:
            f.write(blob)
        buf.seek(0)
        return buf

    def _unpack_v1(self, blob):
        with gzip.GzipFile(fileobj=blob, mode='rb') as f:
            return f.read()

class BlobCache(object):
    def load(self, key):
        pass

    def store(self, results):
        pass
