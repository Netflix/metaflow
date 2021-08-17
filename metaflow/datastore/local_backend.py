import json
import os

from ..metaflow_config import DATASTORE_CARDS_LOCAL_DIR, DATASTORE_CARD_LOCALROOT, DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_LOCAL
from .datastore_backend import CloseAfterUse, DataStoreBackend
from .exceptions import DataException

class LocalBackend(DataStoreBackend):
    TYPE = 'local'
    METADATA_DIR = '_meta'

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        result = DATASTORE_SYSROOT_LOCAL
        return cls.get_root_dir_from_path(result,DATASTORE_LOCAL_DIR,echo,create_on_absent=create_on_absent)

    @classmethod
    def get_root_dir_from_path(cls,result_dir,local_dir_name,echo, create_on_absent=True):
        # Todo: find a better function name
        result= None
        if result_dir is None:
            try:
                # Python2
                current_path = os.getcwdu()
            except: # noqa E722
                current_path = os.getcwd()
            check_dir = os.path.join(current_path, local_dir_name)
            check_dir = os.path.realpath(check_dir)
            orig_path = check_dir
            top_level_reached = False
            while not os.path.isdir(check_dir):
                new_path = os.path.dirname(current_path)
                if new_path == current_path:
                    top_level_reached = True
                    break  # We are no longer making upward progress
                current_path = new_path
                check_dir = os.path.join(current_path, local_dir_name)
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
            result = os.path.join(result_dir, local_dir_name)
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

    def is_file(self, paths):
        results = []
        for path in paths:
            full_path = self.full_uri(path)
            results.append(os.path.isfile(full_path))
        return results

    def info_file(self, path):
        file_exists = self.is_file([path])[0]
        if file_exists:
            full_meta_path = "%s_meta" % self.full_uri(path)
            try:
                with open(full_meta_path, 'r') as f:
                    return True, json.load(f)
            except OSError:
                return True, None
        return False, None

    def list_content(self, paths):
        results = []
        for path in paths:
            if path == self.METADATA_DIR:
                continue
            full_path = self.full_uri(path)
            results.extend([self.list_content_result(
                path=self.path_join(path, f),
                is_file=self.is_file(
                    [self.path_join(path, f)])[0]) for f in os.listdir(full_path)
                    if f != self.METADATA_DIR])
        return results

    def save_bytes(self, path_and_bytes_iter, overwrite=False, len_hint=0):
        for path, obj in path_and_bytes_iter:
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
        def iter_results():
            for path in paths:
                full_path = self.full_uri(path)
                metadata = None
                if os.path.exists(full_path):
                    if os.path.exists("%s_meta" % full_path):
                        with open("%s_meta" % full_path, mode='r') as f:
                            metadata = json.load(f)
                    yield path, full_path, metadata
                else:
                    yield path, None, None
        return CloseAfterUse(iter_results())
