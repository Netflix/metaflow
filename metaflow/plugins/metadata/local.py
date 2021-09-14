import glob
import json
import os
import time

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.metadata import MetadataProvider


class LocalMetadataProvider(MetadataProvider):
    TYPE = 'local'

    def __init__(self, environment, flow, event_logger, monitor):
        super(LocalMetadataProvider, self).__init__(environment, flow, event_logger, monitor)

    @classmethod
    def compute_info(cls, val):
        from metaflow.datastore.local import LocalDataStore
        v = os.path.realpath(os.path.join(val, DATASTORE_LOCAL_DIR))
        if os.path.isdir(v):
            LocalDataStore.datastore_root = v
            return val
        raise ValueError(
            'Could not find directory %s in directory %s' % (DATASTORE_LOCAL_DIR, val))

    @classmethod
    def default_info(cls):
        from metaflow.datastore.local import LocalDataStore

        def print_clean(line, **kwargs):
            print(line)
        v = LocalDataStore.get_datastore_root_from_config(print_clean, create_on_absent=False)
        if v is None:
            return '<No %s directory found in current working tree>' % DATASTORE_LOCAL_DIR
        return os.path.dirname(v)

    def version(self):
        return 'local'

    def new_run_id(self, tags=[], sys_tags=[]):
        # We currently just use the timestamp to create an ID. We can be reasonably certain
        # that it is unique and this makes it possible to do without coordination or
        # reliance on POSIX locks in the filesystem.
        run_id = '%d' % (time.time() * 1e6)
        self._new_run(run_id, tags, sys_tags)
        return run_id

    def register_run_id(self, run_id, tags=[], sys_tags=[]):
        try:
            # This metadata provider only generates integer IDs so if this is
            # an integer, we don't register it again (since it was "registered"
            # on creation). However, some IDs are created outside the metdata
            # provider and need to be properly registered
            int(run_id)
            return
        except ValueError:
            return self._new_run(run_id, tags, sys_tags)

    def new_task_id(self, run_id, step_name, tags=[], sys_tags=[]):
        self._task_id_seq += 1
        task_id = str(self._task_id_seq)
        self._new_task(run_id, step_name, task_id, tags, sys_tags)
        return task_id

    def register_task_id(self,
                         run_id,
                         step_name,
                         task_id,
                         tags=[],
                         sys_tags=[]):
        try:
            # Same logic as register_run_id
            int(task_id)
        except ValueError:
            self._new_task(run_id, step_name, task_id, tags, sys_tags)
        finally:
            self._register_code_package_metadata(run_id, step_name, task_id)

    def register_data_artifacts(self,
                                run_id,
                                step_name,
                                task_id,
                                attempt_id,
                                artifacts):
        meta_dir = self._create_and_get_metadir(self._flow_name, run_id, step_name, task_id)
        artlist = self._artifacts_to_json(run_id, step_name, task_id, attempt_id, artifacts)
        artdict = {'%d_artifact_%s' % (attempt_id, art['name']): art for art in artlist}
        self._save_meta(meta_dir, artdict)

    def register_metadata(self, run_id, step_name, task_id, metadata):
        meta_dir = self._create_and_get_metadir(self._flow_name, run_id, step_name, task_id)
        metalist = self._metadata_to_json(run_id, step_name, task_id, metadata)
        ts = int(round(time.time() * 1000))
        metadict = {'sysmeta_%s_%d' % (meta['field_name'], ts): meta for meta in metalist}
        self._save_meta(meta_dir, metadict)

    @classmethod
    def _get_object_internal(cls, obj_type, obj_order, sub_type, sub_order, filters=None, *args):
        from metaflow.datastore.local import LocalDataStore
        if obj_type == 'artifact':
            # Artifacts are actually part of the tasks in the filesystem
            obj_type = 'task'
            sub_type = 'artifact'
            sub_order = obj_order
            obj_order = obj_order - 1

        # Special handling of self, artifact, and metadata
        if sub_type == 'self':
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            if meta_path is None:
                return None
            self_file = os.path.join(meta_path, '_self.json')
            if os.path.isfile(self_file):
                return MetadataProvider._apply_filter(
                    [LocalMetadataProvider._read_json_file(self_file)], filters)[0]
            return None

        if sub_type == 'artifact':
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            result = []
            if meta_path is None:
                return result
            attempt_done_files = os.path.join(meta_path, 'sysmeta_attempt-done_*')
            attempts_done = sorted(glob.iglob(attempt_done_files))
            if attempts_done:
                successful_attempt = int(LocalMetadataProvider._read_json_file(
                    attempts_done[-1])['value'])
                which_artifact = '*'
                if len(args) >= sub_order:
                    which_artifact = args[sub_order - 1]
                artifact_files = os.path.join(
                    meta_path, '%d_artifact_%s.json' % (successful_attempt, which_artifact))
                for obj in glob.iglob(artifact_files):
                    result.append(LocalMetadataProvider._read_json_file(obj))
            if len(result) == 1:
                return result[0]
            return result

        if sub_type == 'metadata':
            result = []
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            if meta_path is None:
                return result
            files = os.path.join(meta_path, 'sysmeta_*')
            for obj in glob.iglob(files):
                result.append(LocalMetadataProvider._read_json_file(obj))
            return result

        # For the other types, we locate all the objects we need to find and return them
        obj_path = LocalMetadataProvider._make_path(*args[:obj_order], create_on_absent=False)
        result = []
        if obj_path is None:
            return result
        skip_dirs = '*/'*(sub_order - obj_order)
        all_meta = os.path.join(obj_path, skip_dirs, LocalDataStore.METADATA_DIR)
        for meta_path in glob.iglob(all_meta):
            self_file = os.path.join(meta_path, '_self.json')
            if os.path.isfile(self_file):
                result.append(LocalMetadataProvider._read_json_file(self_file))
        return MetadataProvider._apply_filter(result, filters)

    @staticmethod
    def _makedirs(path):
        # this is for python2 compatibility.
        # Python3 has os.makedirs(exist_ok=True).
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                # Error raised when directory exists
                return
            else:
                raise

    def _ensure_meta(
            self, obj_type, run_id, step_name, task_id, tags=[], sys_tags=[]):
        subpath = self._create_and_get_metadir(self._flow_name, run_id, step_name, task_id)
        selfname = os.path.join(subpath, '_self.json')
        self._makedirs(subpath)
        if os.path.isfile(selfname):
            return
        # In this case, the metadata information does not exist so we create it
        self._save_meta(
            subpath,
            {'_self': self._object_to_json(
                obj_type,
                run_id,
                step_name,
                task_id,
                tags + self.sticky_tags, sys_tags + self.sticky_sys_tags)})

    def _new_run(self, run_id, tags=[], sys_tags=[]):
        self._ensure_meta('flow', None, None, None)
        self._ensure_meta('run', run_id, None, None, tags, sys_tags)

    def _new_task(self, run_id, step_name, task_id, tags=[], sys_tags=[]):
        self._ensure_meta('step', run_id, step_name, None)
        self._ensure_meta('task', run_id, step_name, task_id, tags, sys_tags)
        self._register_code_package_metadata(run_id, step_name, task_id)

    @staticmethod
    def _make_path(
          flow_name=None, run_id=None, step_name=None, task_id=None, pathspec=None,
          create_on_absent=True):

        from metaflow.datastore.local import LocalDataStore
        if LocalDataStore.datastore_root is None:
            def print_clean(line, **kwargs):
                print(line)
            LocalDataStore.datastore_root = LocalDataStore.get_datastore_root_from_config(
                print_clean, create_on_absent=create_on_absent)
        if LocalDataStore.datastore_root is None:
            return None

        return LocalDataStore.make_path(flow_name, run_id, step_name, task_id, pathspec)

    @staticmethod
    def _create_and_get_metadir(
          flow_name=None, run_id=None, step_name=None, task_id=None):
        from metaflow.datastore.local import LocalDataStore
        root_path = LocalMetadataProvider._make_path(flow_name, run_id, step_name, task_id)
        subpath = os.path.join(root_path, LocalDataStore.METADATA_DIR)
        LocalMetadataProvider._makedirs(subpath)
        return subpath

    @staticmethod
    def _get_metadir(flow_name=None, run_id=None, step_name=None, task_id=None):
        from metaflow.datastore.local import LocalDataStore
        root_path = LocalMetadataProvider._make_path(
            flow_name, run_id, step_name, task_id, create_on_absent=False)
        if root_path is None:
            return None
        subpath = os.path.join(root_path, LocalDataStore.METADATA_DIR)
        if os.path.isdir(subpath):
            return subpath
        return None

    @staticmethod
    def _dump_json_to_file(
            filepath, data, allow_overwrite=False):
        if os.path.isfile(filepath) and not allow_overwrite:
            return
        with open(filepath + '.tmp', 'w') as f:
            json.dump(data, f)
        os.rename(filepath + '.tmp', filepath)

    @staticmethod
    def _read_json_file(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)

    @staticmethod
    def _save_meta(root_dir, metadict):
        for name, datum in metadict.items():
            filename = os.path.join(root_dir, '%s.json' % name)
            LocalMetadataProvider._dump_json_to_file(filename, datum)
