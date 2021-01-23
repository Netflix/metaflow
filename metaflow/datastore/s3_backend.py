import os

from ..metaflow_config import DATASTORE_SYSROOT_S3
from .datastore_backend import DataStoreBackend
from .exceptions import DataException


try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

# Helper class that keeps a reference to the S3 client used to obtain the file
# as well as the path to the file downloaded. We need to keep a reference
# to the S3 client to prevent it from going away and deleting the file
# that was downloaded with the client.
class FileWithClientRef(object):
    def __init__(self, client, file):
        self._client = client
        self._file = file
        self._open_file = None

    def __enter__(self):
        if self._open_file is None:
            self._open_file = open(self._file, mode='rb')
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        if self._client:
            self._client = None
        self.close()

    def close(self):
        if self._open_file:
            self._open_file.close()
            self._open_file = None

    def __getattr__(self, name):
        if self._open_file is None:
            self._open_file = open(self._file, mode='rb')
        return getattr(self._open_file, name)


class S3Backend(DataStoreBackend):
    TYPE = 's3'

    def __init__(self, root=None):
        self.s3_client = None
        self.s3_datatool = None
        super(S3Backend, self).__init__(root)

    def reset_datatools_client(self, hard_reset=False):
        if hard_reset or self.s3_datatool is None:
            from ..datatools import S3
            self.s3_datatool = S3(
                s3root=self.datastore_root, tmproot=os.getcwd())
            self.reset_s3_client()
            self.s3_datatool.reset_client(new_client=self.s3_client)

    def reset_s3_client(self, hard_reset=False):
        if hard_reset or self.s3_client is None:
            from .util.s3util import get_s3_client
            self.s3_client, _ = get_s3_client()


    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        return DATASTORE_SYSROOT_S3

    def is_file(self, path):
        """
        Returns True or False depending on whether path refers to a valid
        file-like object

        This method returns False if path points to a directory

        Parameters
        ----------
        path : string
            Path to the object
        """
        self.reset_s3_client()
        full_path = self.full_uri(path)
        src = urlparse(full_path)
        from botocore.exceptions import ClientError
        try:
            self.s3_client.head_object(
                Bucket=src.netloc, Key=src.path.lstrip('/'))
        except ClientError as err:
            if err.response['Error']['Code'] in ('404', '403'):
                return False
            # Other errors may be indicative of a bad client so we reset
            self.reset_s3_client(hard_reset=True)
            return False
        return True

    def list_content(self, paths):
        """
        Lists the content of the datastore in the directory indicated by 'paths'.

        This is similar to executing a 'ls'; it will only list the content one
        level down and simply returns the paths to the elements present as well
        as whether or not those elements are files (if not, they are further
        directories that can be traversed)

        The path returned always include the path passed in. As an example,
        if your filesystem contains the files: A/b.txt A/c.txt and the directory
        A/D, on return, you would get, for an input of ['A']:
        [('A/b.txt', True), ('A/c.txt', True), ('A/D', False)]

        Parameters
        ----------
        paths : List[string]
            Directories to list

        Returns
        -------
        List[list_content_result]
            Content of the directory
        """
        strip_prefix_len = len(self.datastore_root.rstrip('/')) + 1
        self.reset_datatools_client()
        results = self.s3_datatool.list_paths(paths)
        return [self.list_content_result(
            path=o.url[strip_prefix_len:], is_file=o.exists) for o in results]

    def save_bytes(self, path_and_bytes, overwrite=False):
        """
        Creates objects and stores them in the datastore.

        If overwrite is False, any existing object will not be overwritten and
        an error will be returned.

        The objects are specified in the objects dictionary where the key is the
        path to store the object and the value is a file-like object from which
        bytes can be read.

        Parameters
        ----------
        path_and_bytes : Dict: string -> RawIOBase or BufferedIOBase
            Objects to store
        overwrite : bool
            True if the objects can be overwritten. Defaults to False.

        Returns
        -------
        None
        """
        if len(path_and_bytes) == 0:
            return

        def _convert():
            for path, byte_obj in path_and_bytes.items():
                yield path, byte_obj

        self.reset_datatools_client()
        # HACK: The S3 datatools we rely on does not currently do a good job
        # determining if uploading things in parallel is more efficient than
        # serially. We use a heuristic for now where if we have a lot of
        # files, we will go in parallel and if we have few files, we will
        # serially upload them. This is not ideal because there is also a size
        # factor and one very large file with a few other small files, for
        # example, would benefit from a parallel upload. The S3 datatools will
        # be fixed to address this and this whole hack can then go away.
        if len(path_and_bytes) > 10:
            # Use datatools
            self.s3_datatool.put_many(_convert(), overwrite)
        else:
            # Sequential upload
            for key, obj in _convert():
                self.s3_datatool.put(key, obj, overwrite)

    def load_bytes(self, paths):
        """
        Gets objects from the datastore

        Note that objects may be fetched in parallel so if order is important
        for your consistency model, the caller is responsible for calling this
        multiple times in the proper order.

        Parameters
        ----------
        paths : List[string]
            Paths to fetch

        Returns
        -------
        Dict: string -> BufferedIOBase
            A dictionary is returned where the key is the path fetched and the
            value is a BufferedIOBase indicating the result of loading the path
        """
        if len(paths) == 0:
            return {}

        self.reset_datatools_client()
        # We similarly do things in parallel for many files. This is again
        # a hack.
        to_return = {}
        if len(paths) > 10:
            results = self.s3_datatool.get_many(paths, return_missing=True)
            for r in results:
                if r.exists:
                    to_return[r.key] = FileWithClientRef(
                        self.s3_datatool, r.path)
                else:
                    to_return[r.key] = None
        else:
            for p in paths:
                r = self.s3_datatool.get(p, return_missing=True)
                if r.exists:
                    to_return[r.key] = FileWithClientRef(
                        self.s3_datatool, r.path)
                else:
                    to_return[r.key] = None
        return to_return
