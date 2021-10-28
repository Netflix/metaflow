import fcntl
import glob
import json
import os
import re
import time

from metaflow.exception import TaggingException
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.metadata import MetadataProvider


artifact_name_expr = re.compile("[0-9]+_artifact_.*.json")


class LocalMetadataProvider(MetadataProvider):
    TYPE = "local"

    def __init__(self, environment, flow, event_logger, monitor):
        super(LocalMetadataProvider, self).__init__(
            environment, flow, event_logger, monitor
        )

    @classmethod
    def compute_info(cls, val):
        from metaflow.datastore.local_storage import LocalStorage

        v = os.path.realpath(os.path.join(val, DATASTORE_LOCAL_DIR))
        if os.path.isdir(v):
            LocalStorage.datastore_root = v
            return val
        raise ValueError(
            "Could not find directory %s in directory %s" % (DATASTORE_LOCAL_DIR, val)
        )

    @classmethod
    def default_info(cls):
        from metaflow.datastore.local_storage import LocalStorage

        def print_clean(line, **kwargs):
            print(line)

        v = LocalStorage.get_datastore_root_from_config(
            print_clean, create_on_absent=False
        )
        if v is None:
            return (
                "<No %s directory found in current working tree>" % DATASTORE_LOCAL_DIR
            )
        return os.path.dirname(v)

    def version(self):
        return "local"

    def new_run_id(self, tags=None, sys_tags=None):
        # We currently just use the timestamp to create an ID. We can be reasonably certain
        # that it is unique and this makes it possible to do without coordination or
        # reliance on POSIX locks in the filesystem.
        run_id = "%d" % (time.time() * 1e6)
        self._new_run(run_id, tags, sys_tags)
        return run_id

    def register_run_id(self, run_id, tags=None, sys_tags=None):
        try:
            # This metadata provider only generates integer IDs so if this is
            # an integer, we don't register it again (since it was "registered"
            # on creation). However, some IDs are created outside the metadata
            # provider and need to be properly registered
            int(run_id)
            return
        except ValueError:
            return self._new_run(run_id, tags, sys_tags)

    def new_task_id(self, run_id, step_name, tags=None, sys_tags=None):
        self._task_id_seq += 1
        task_id = str(self._task_id_seq)
        self._new_task(run_id, step_name, task_id, tags, sys_tags)
        return task_id

    def register_task_id(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        try:
            # Same logic as register_run_id
            int(task_id)
        except ValueError:
            self._new_task(run_id, step_name, task_id, attempt, tags, sys_tags)
        else:
            self._register_code_package_metadata(run_id, step_name, task_id, attempt)

    def register_data_artifacts(
        self, run_id, step_name, task_id, attempt_id, artifacts
    ):
        meta_dir = self._create_and_get_metadir(
            self._flow_name, run_id, step_name, task_id
        )
        artlist = self._artifacts_to_json(
            run_id, step_name, task_id, attempt_id, artifacts
        )
        artdict = {"%d_artifact_%s" % (attempt_id, art["name"]): art for art in artlist}
        self._save_meta(meta_dir, artdict)

    def register_metadata(self, run_id, step_name, task_id, metadata):
        meta_dir = self._create_and_get_metadir(
            self._flow_name, run_id, step_name, task_id
        )
        metalist = self._metadata_to_json(run_id, step_name, task_id, metadata)
        ts = int(round(time.time() * 1000))
        metadict = {
            "sysmeta_%s_%d" % (meta["field_name"], ts): meta for meta in metalist
        }
        self._save_meta(meta_dir, metadict)

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        from metaflow.datastore.local_storage import LocalStorage

        if obj_type == "artifact":
            # Artifacts are actually part of the tasks in the filesystem
            obj_type = "task"
            sub_type = "artifact"
            sub_order = obj_order
            obj_order = obj_order - 1

        # Special handling of self, artifact, and metadata
        if sub_type == "self":
            self_file = LocalMetadataProvider._get_object_filepaths(*args[:obj_order])
            if self_file:
                return MetadataProvider._apply_filter(
                    [LocalMetadataProvider._read_json_file(self_file[0])], filters
                )[0]
            return None

        if sub_type == "artifact":
            which_artifact = args[sub_order - 1] if len(args) >= sub_order else "*"
            result = []
            path_spec = list(args[:obj_order])
            path_spec.append(which_artifact)
            if attempt is not None:
                path_spec.append(attempt)
            artifact_files = LocalMetadataProvider._get_object_filepaths(*path_spec)
            if artifact_files:
                result = [
                    LocalMetadataProvider._read_json_file(f) for f in artifact_files
                ]
            if len(result) == 1:
                return result[0]
            return result

        if sub_type == "metadata":
            result = []
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            if meta_path is None:
                return result
            files = os.path.join(meta_path, "sysmeta_*")
            for obj in glob.iglob(files):
                result.append(LocalMetadataProvider._read_json_file(obj))
            return result

        # For the other types, we locate all the objects we need to find and return them
        obj_path = LocalMetadataProvider._make_path(
            *args[:obj_order], create_on_absent=False
        )
        result = []
        if obj_path is None:
            return result
        skip_dirs = "*/" * (sub_order - obj_order)
        all_meta = os.path.join(obj_path, skip_dirs, LocalStorage.METADATA_DIR)
        for meta_path in glob.iglob(all_meta):
            self_file = os.path.join(meta_path, "_self.json")
            if os.path.isfile(self_file):
                result.append(LocalMetadataProvider._read_json_file(self_file))
        return MetadataProvider._apply_filter(result, filters)

    @classmethod
    def _perform_operations_internal(cls, operations):
        ops_per_object = dict()
        for op in operations:
            ops = ops_per_object.setdefault(op.id, [set(), set()])
            if op.operation == "add":
                ops[0].add(op.args["tag"])
            else:
                # Operations are checked by parent class so this is remove
                ops[1].add(op.args["tag"])
        # At this point, we remove any duplicates from add/remove sets since
        # that becomes a no-op
        for ops in ops_per_object.values():
            ops[0] = ops[0] - ops[1]
            ops[1] = ops[1] - ops[0]
        # Now we can get each object and update the tags; we do this using flock
        # to guarantee that concurrent accesses work OK.
        results = dict()
        for op_id, ops in ops_per_object.items():
            # Get the object we need to update
            obj_path = op_id.split("/")
            obj_file = LocalMetadataProvider._get_object_filepaths(*obj_path)
            if obj_file is None or len(obj_file) != 1:
                results[op_id] = {
                    "status": "Not found",
                    "data": "Object %s was not found" % op_id,
                }
                continue

            # Now get the file and open using flock
            with open(obj_file[0], "r+") as f:
                # Lock released when fd is closed at the end of with
                fcntl.flock(f, fcntl.LOCK_EX)

                obj_to_update = json.load(f)
                user_tags = set(obj_to_update["tags"])
                sys_tags = set(obj_to_update["system_tags"])
                if not sys_tags.isdisjoint(ops[1]):
                    # We are trying to remove a system tag so we fail
                    results[op_id] = {
                        "status": "Removing a system tag",
                        "data": "Cannot remove system tags %s"
                        % (sys_tags.intersection(ops[1])),
                    }
                    continue
                # At this point, we have no failures; compute the new tags
                user_tags.update(ops[0])
                user_tags.difference_update(ops[1])
                obj_to_update["tags"] = list(user_tags)

                f.seek(0)
                json.dump(obj_to_update, f)
                f.truncate()

                results[op_id] = {"status": "ok", "data": obj_to_update}
        return results

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
        self, obj_type, run_id, step_name, task_id, tags=None, sys_tags=None
    ):
        if tags is None:
            tags = set()
        if sys_tags is None:
            sys_tags = set()
        subpath = self._create_and_get_metadir(
            self._flow_name, run_id, step_name, task_id
        )
        selfname = os.path.join(subpath, "_self.json")
        self._makedirs(subpath)
        if os.path.isfile(selfname):
            return
        # In this case, the metadata information does not exist so we create it
        self._save_meta(
            subpath,
            {
                "_self": self._object_to_json(
                    obj_type,
                    run_id,
                    step_name,
                    task_id,
                    self.sticky_tags.union(tags),
                    self.sticky_sys_tags.union(sys_tags),
                )
            },
        )

    def _new_run(self, run_id, tags=None, sys_tags=None):
        self._ensure_meta("flow", None, None, None)
        self._ensure_meta("run", run_id, None, None, tags, sys_tags)

    def _new_task(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        self._ensure_meta("step", run_id, step_name, None)
        self._ensure_meta("task", run_id, step_name, task_id, tags, sys_tags)
        self._register_code_package_metadata(run_id, step_name, task_id, attempt)

    @staticmethod
    def _make_path(
        flow_name=None, run_id=None, step_name=None, task_id=None, create_on_absent=True
    ):

        from metaflow.datastore.local_storage import LocalStorage

        if LocalStorage.datastore_root is None:

            def print_clean(line, **kwargs):
                print(line)

            LocalStorage.datastore_root = LocalStorage.get_datastore_root_from_config(
                print_clean, create_on_absent=create_on_absent
            )
        if LocalStorage.datastore_root is None:
            return None

        if flow_name is None:
            return LocalStorage.datastore_root
        components = []
        if flow_name:
            components.append(flow_name)
            if run_id:
                components.append(run_id)
                if step_name:
                    components.append(step_name)
                    if task_id:
                        components.append(task_id)
        return LocalStorage().full_uri(LocalStorage.path_join(*components))

    @staticmethod
    def _create_and_get_metadir(
        flow_name=None, run_id=None, step_name=None, task_id=None
    ):
        from metaflow.datastore.local_storage import LocalStorage

        root_path = LocalMetadataProvider._make_path(
            flow_name, run_id, step_name, task_id
        )
        subpath = os.path.join(root_path, LocalStorage.METADATA_DIR)
        LocalMetadataProvider._makedirs(subpath)
        return subpath

    @staticmethod
    def _get_metadir(flow_name=None, run_id=None, step_name=None, task_id=None):
        from metaflow.datastore.local_storage import LocalStorage

        root_path = LocalMetadataProvider._make_path(
            flow_name, run_id, step_name, task_id, create_on_absent=False
        )
        if root_path is None:
            return None
        subpath = os.path.join(root_path, LocalStorage.METADATA_DIR)
        if os.path.isdir(subpath):
            return subpath
        return None

    @staticmethod
    def _get_object_filepaths(
        flow_name=None,
        run_id=None,
        step_name=None,
        task_id=None,
        artifact_name=None,
        attempt=None,
    ):
        meta_path = LocalMetadataProvider._get_metadir(
            flow_name, run_id, step_name, task_id
        )
        if meta_path is None:
            return None
        # For non-artifacts
        if artifact_name is None:
            self_file = os.path.join(meta_path, "_self.json")
            if os.path.isfile(self_file):
                return [self_file]
            return None
        # For artifacts
        results = []
        successful_attempt = attempt
        if successful_attempt is None:
            attempt_done_files = os.path.join(meta_path, "sysmeta_attempt-done_*")
            attempts_done = sorted(glob.iglob(attempt_done_files))
            if attempts_done:
                successful_attempt = int(
                    LocalMetadataProvider._read_json_file(attempts_done[-1])["value"]
                )
        if successful_attempt is not None:
            artifact_files = os.path.join(
                meta_path, "%d_artifact_%s.json" % (successful_attempt, artifact_name)
            )
            results = list(glob.iglob(artifact_files))
        if len(results) == 0:
            return None
        return results

    @staticmethod
    def _dump_json_to_file(filepath, data, allow_overwrite=False):
        if os.path.isfile(filepath) and not allow_overwrite:
            return
        with open(filepath + ".tmp", "w") as f:
            json.dump(data, f)
        os.rename(filepath + ".tmp", filepath)

    @staticmethod
    def _read_json_file(filepath):
        # For any files that end in `_self.json` or files that are artifact
        # files, we load using flock in case there is a concurrent modification
        # of the tags
        name = os.path.basename(filepath)
        do_flock = name == "_self.json" or artifact_name_expr.match(name)
        with open(filepath, "r") as f:
            if do_flock:
                # Released on close at end of with block
                fcntl.flock(f, fcntl.LOCK_SH)
            return json.load(f)

    @staticmethod
    def _save_meta(root_dir, metadict):
        for name, datum in metadict.items():
            filename = os.path.join(root_dir, "%s.json" % name)
            LocalMetadataProvider._dump_json_to_file(filename, datum)
