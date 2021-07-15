
import os

from itertools import starmap

from ..datatools.s3 import S3, S3PutObject
from ..datatools.s3util import S3Client
from ..metaflow_config import DATASTORE_SYSROOT_S3
from .datastore_backend import CloseAfterUse, DataStoreBackend


try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse


class S3Backend(DataStoreBackend):
    TYPE = 's3'

    def __init__(self, root=None):
        super(S3Backend, self).__init__(root)
        self.s3_client = S3Client()

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

        Returns
        -------
        bool
        """
        with S3(s3root=self.datastore_root,
                tmproot=os.getcwd(), external_client=self.s3_client) as s3:
            s3obj = s3.info(path, return_missing=True)
            return s3obj.exists

    def info_file(self, path):
        """
        Returns a tuple where the first element is True or False depending on
        whether path refers to a valid file-like object (like is_file) and the
        second element is a dictionary of metadata associated with the file or
        None if the file does not exist or there is no metadata.

        Parameters
        ----------
        path : string
            Path to the object

        Returns
        -------
        tuple
            (bool, dict)
        """
        with S3(s3root=self.datastore_root,
                tmproot=os.getcwd(), external_client=self.s3_client) as s3:
            s3obj = s3.info(path, return_missing=True)
            return s3obj.exists, s3obj.metadata

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
        with S3(s3root=self.datastore_root,
                tmproot=os.getcwd(), external_client=self.s3_client) as s3:
            results = s3.list_paths(paths)
            return [self.list_content_result(
                path=o.url[strip_prefix_len:], is_file=o.exists) for o in results]

    def save_bytes(self, path_and_bytes, overwrite=False):
        """
        Creates objects and stores them in the datastore.

        If overwrite is False, any existing object will not be overwritten and
        will be silently ignored

        The objects are specified in the objects dictionary where the key is the
        path to store the object and the value is a file-like object from which
        bytes can be read.

        Parameters
        ----------
        path_and_bytes : Dict: string -> (RawIOBase or BufferedIOBase, dict)
            Objects to store; the first element in the tuple is the actual data
            to store and the dictionary is additional metadata to store. Keys
            for the metadata must be ascii only string and elements can be
            anything that can be converted to a string using json.dumps. If you
            have no metadata, you can simply pass a RawIOBase or BufferedIOBase.
        overwrite : bool
            True if the objects can be overwritten. Defaults to False.

        Returns
        -------
        None
        """
        if len(path_and_bytes) == 0:
            return

        def _convert():
            # Output format is the same as what is needed for S3PutObject:
            # key, value, path, content_type, metadata
            for path, obj in path_and_bytes.items():
                if isinstance(obj, tuple):
                    yield path, obj[0], None, None, obj[1]
                else:
                    yield path, obj, None, None, None

        with S3(s3root=self.datastore_root,
                tmproot=os.getcwd(), external_client=self.s3_client) as s3:
            # HACK: The S3 datatools we rely on does not currently do a good job
            # determining if uploading things in parallel is more efficient than
            # serially. We use a heuristic for now where if we have a lot of
            # files, we will go in parallel and if we have few files, we will
            # serially upload them. This is not ideal because there is also a size
            # factor and one very large file with a few other small files, for
            # example, would benefit from a parallel upload. The S3 datatools will
            # be fixed to address this and this whole hack can then go away.
            if len(path_and_bytes) > 10:
                # Use put_many
                s3.put_many(starmap(S3PutObject, _convert()), overwrite)
            else:
                # Sequential upload
                for key, obj, _, _, metadata in _convert():
                    s3.put(key, obj, overwrite=overwrite, metadata=metadata)

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
        CloseAfterUse :
            A CloseAfterUse which should be used in a with statement. The data
            in the CloseAfterUse will be a dictionary string -> (BufferedIOBase, dict).
            The key is the path fetched and the value is a tuple containing:
              - a path indicating the file that needs to be read to get the object.
                This path may not be valid outside of the CloseAfterUse scope
              - a dictionary containing any additional metadata that was stored
              or None if no metadata was provided.
            If the path could not be loaded, returns None for that path
        """
        if len(paths) == 0:
            return CloseAfterUse({})

        s3 = S3(s3root=self.datastore_root,
                tmproot=os.getcwd(), external_client=self.s3_client)
        to_return = dict()
        # We similarly do things in parallel for many files. This is again
        # a hack.
        if len(paths) > 10:
            results = s3.get_many(paths, return_missing=True, return_info=True)
            for r in results:
                if r.exists:
                    to_return[r.key] = (r.path, r.metadata)
                else:
                    to_return[r.key] = None
        else:
            for p in paths:
                r = s3.get(p, return_missing=True, return_info=True)
                if r.exists:
                    to_return[r.key] = (r.path, r.metadata)
                else:
                    to_return[r.key] = None
        return CloseAfterUse(to_return, closer=s3)
