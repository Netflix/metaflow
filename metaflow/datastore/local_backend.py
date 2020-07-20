import os

from ..metaflow_config import DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_LOCAL
from .common import DataException
from .datastore_backend import DataStoreBackend

# Helper class to lazily open files returned by load_bytes to prevent
# possibly running out of open file descriptors
class LazyFile(object):
    def __init__(self, file):
        self._file = file
        self._open_file = None

    def __enter__(self):
        if self._open_file is None:
            self._open_file = open(self._file, mode='rb')
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if self._open_file:
            self._open_file.close()
            self._open_file = None

    def __getattr__(self, name):
        if self._open_file is None:
            self._open_file = open(self._file, mode='rb')
        return getattr(self._open_file, name)

class LocalBackend(DataStoreBackend):
    TYPE = 'local'
    METADATA_DIR = '_meta'

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        result = DATASTORE_SYSROOT_LOCAL
        if result is None:
            try:
                # Python2
                current_path = os.getcwdu()
            except: # noqa E722
                current_path = os.getcwd()
            check_dir = os.path.join(current_path, DATASTORE_LOCAL_DIR)
            check_dir = os.path.realpath(check_dir)
            orig_path = check_dir
            top_level_reached = False
            while not os.path.isdir(check_dir):
                new_path = os.path.dirname(current_path)
                if new_path == current_path:
                    top_level_reached = True
                    break  # We are no longer making upward progress
                current_path = new_path
                check_dir = os.path.join(current_path, DATASTORE_LOCAL_DIR)
            if top_level_reached:
                if create_on_absent:
                    # Could not find any directory to use so create a new one
                    echo('Creating local datastore in current directory (%s)'
                         % orig_path)
                    os.mkdir(orig_path)
                    result = orig_path
                else:
                    return None
            else:
                result = check_dir
        else:
            result = os.path.join(result, DATASTORE_LOCAL_DIR)
        return result

    @staticmethod
    def _makedirs(path):
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

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
        full_path = self.full_uri(path)
        return os.path.isfile(full_path)

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
        results = []
        for path in paths:
            if path == self.METADATA_DIR:
                continue
            full_path = self.full_uri(path)
            results.extend([self.list_content_result(
                path=self.path_join(path, f),
                is_file=self.is_file(
                    self.path_join(path, f))) for f in os.listdir(full_path)
                    if f != self.METADATA_DIR])
        return results

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
        results = {}
        for path, byte_obj in path_and_bytes.items():
            full_path = self.full_uri(path)
            if not overwrite and os.path.exists(full_path):
                raise DataException("Cannot overwrite file %s" % full_path)
            LocalBackend._makedirs(os.path.dirname(full_path))
            with open(full_path, mode='wb') as f:
                f.write(byte_obj.read())
        return results

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
        results = {}
        for path in paths:
            full_path = self.full_uri(path)
            if os.path.exists(full_path):
                try:
                    results[path] = LazyFile(full_path)
                except OSError as e:
                    results[path] = None
            else:
                results[path] = None
        return results
