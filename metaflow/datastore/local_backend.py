import json
import os

from ..metaflow_config import DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_LOCAL
from .datastore_backend import CloseAfterUse, DataStoreBackend
from .exceptions import DataException

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

        Returns
        -------
        bool
        """
        full_path = self.full_uri(path)
        return os.path.isfile(full_path)

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
        file_exists = self.is_file(path)
        if file_exists:
            full_meta_path = "%s_meta" % self.full_uri(path)
            try:
                with open(full_meta_path, 'r') as f:
                    return True, json.load(f)
            except OSError:
                return True, None
        return False, None

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
        for path, obj in path_and_bytes.items():
            if isinstance(obj, tuple):
                byte_obj, metadata = obj
            else:
                byte_obj, metadata = obj, None
            full_path = self.full_uri(path)
            if not overwrite and os.path.exists(full_path):
                continue
            LocalBackend._makedirs(os.path.dirname(full_path))
            with open(full_path, mode='wb') as f:
                f.write(byte_obj.read())
            if metadata:
                with open("%s_meta" % full_path, mode='w') as f:
                    json.dump(metadata, f)

    def load_bytes(self, paths):
        """
        Gets objects from the datastore

        Note that objects may be fetched in parallel so if order is important
        for your consistency model, the caller is responsible for calling this
        multiple times in the proper order.

        load_bytes should be used using a with statement:
        with backend.load_bytes(paths) as results:
            <do something>
        The reason for this is that certain backends may need to clean up
        temporary structures (for examples files used to download the bytes)
        once done. You should not use anything emanating from `results` outside
        of the with statement.

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
        results = {}
        for path in paths:
            full_path = self.full_uri(path)
            file_result = None
            metadata = None
            if os.path.exists(full_path):
                if os.path.exists("%s_meta" % full_path):
                    with open("%s_meta" % full_path, mode='r') as f:
                        metadata = json.load(f)
                results[path] = (full_path, metadata)
            else:
                results[path] = None
        return CloseAfterUse(results)
