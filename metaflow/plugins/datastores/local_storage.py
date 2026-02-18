import json
import os
import tempfile

from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_LOCAL,
)
from metaflow.datastore.datastore_storage import CloseAfterUse, DataStoreStorage


class LocalStorage(DataStoreStorage):
    TYPE = "local"
    METADATA_DIR = "_meta"
    DATASTORE_DIR = DATASTORE_LOCAL_DIR  # ".metaflow"
    SYSROOT_VAR = DATASTORE_SYSROOT_LOCAL

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        result = cls.SYSROOT_VAR
        if result is None:
            try:
                # Python2
                current_path = os.getcwdu()
            except:  # noqa E722
                current_path = os.getcwd()
            check_dir = os.path.join(current_path, cls.DATASTORE_DIR)
            check_dir = os.path.realpath(check_dir)
            orig_path = check_dir
            top_level_reached = False
            while not os.path.isdir(check_dir):
                new_path = os.path.dirname(current_path)
                if new_path == current_path:
                    top_level_reached = True
                    break  # We are no longer making upward progress
                current_path = new_path
                check_dir = os.path.join(current_path, cls.DATASTORE_DIR)
            if top_level_reached:
                if create_on_absent:
                    # Could not find any directory to use so create a new one
                    echo(
                        "Creating %s datastore in current directory (%s)"
                        % (cls.TYPE, orig_path)
                    )
                    os.mkdir(orig_path)
                    result = orig_path
                else:
                    return None
            else:
                result = check_dir
        else:
            result = os.path.join(result, cls.DATASTORE_DIR)
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
                with open(full_meta_path, "r") as f:
                    return True, json.load(f)
            except OSError:
                return True, None
        return False, None

    def size_file(self, path):
        file_exists = self.is_file([path])[0]
        if file_exists:
            path = self.full_uri(path)
            try:
                return os.path.getsize(path)
            except OSError:
                return None
        return None

    def list_content(self, paths):
        results = []
        for path in paths:
            if path == self.METADATA_DIR:
                continue
            full_path = self.full_uri(path)
            try:
                for f in os.listdir(full_path):
                    if f == self.METADATA_DIR:
                        continue
                    results.append(
                        self.list_content_result(
                            path=self.path_join(path, f),
                            is_file=self.is_file([self.path_join(path, f)])[0],
                        )
                    )
            except FileNotFoundError as e:
                pass
        return results

    @staticmethod
    def _atomic_write(full_path, data, mode="wb"):
        """Write data to full_path atomically using a temp file + rename."""
        dir_name = os.path.dirname(full_path)
        fd, tmp_path = tempfile.mkstemp(dir=dir_name)
        success = False
        try:
            with os.fdopen(fd, mode) as f:
                f.write(data)
            os.rename(tmp_path, full_path)
            success = True
        finally:
            if not success:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def save_bytes(self, path_and_bytes_iter, overwrite=False, len_hint=0):
        for path, obj in path_and_bytes_iter:
            if isinstance(obj, tuple):
                byte_obj, metadata = obj
            else:
                byte_obj, metadata = obj, None
            full_path = self.full_uri(path)
            if not overwrite and os.path.exists(full_path):
                continue
            LocalStorage._makedirs(os.path.dirname(full_path))
            self._atomic_write(full_path, byte_obj.read(), mode="wb")
            if metadata:
                self._atomic_write(
                    "%s_meta" % full_path,
                    json.dumps(metadata).encode("utf-8"),
                    mode="wb",
                )

    def load_bytes(self, paths):
        def iter_results():
            for path in paths:
                full_path = self.full_uri(path)
                metadata = None
                if os.path.exists(full_path):
                    if os.path.exists("%s_meta" % full_path):
                        with open("%s_meta" % full_path, mode="r") as f:
                            metadata = json.load(f)
                    yield path, full_path, metadata
                else:
                    yield path, None, None

        return CloseAfterUse(iter_results())
