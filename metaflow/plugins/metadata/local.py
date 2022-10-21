import collections
import glob
import json
import os
import random
import tempfile
import time
from collections import namedtuple

from metaflow.exception import MetaflowInternalError, MetaflowTaggingError
from metaflow.metadata.metadata import ObjectOrder
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.metadata import MetadataProvider
from metaflow.tagging_util import MAX_USER_TAG_SET_SIZE, validate_tags


class LocalMetadataProvider(MetadataProvider):
    TYPE = "local"

    def __init__(self, environment, flow, event_logger, monitor):
        super(LocalMetadataProvider, self).__init__(
            environment, flow, event_logger, monitor
        )

    @classmethod
    def compute_info(cls, val):
        from metaflow.plugins.datastores.local_storage import LocalStorage

        v = os.path.realpath(os.path.join(val, DATASTORE_LOCAL_DIR))
        if os.path.isdir(v):
            LocalStorage.datastore_root = v
            return val
        raise ValueError(
            "Could not find directory %s in directory %s" % (DATASTORE_LOCAL_DIR, val)
        )

    @classmethod
    def default_info(cls):
        from metaflow.plugins.datastores.local_storage import LocalStorage

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
            return False
        except ValueError:
            return self._new_run(run_id, tags, sys_tags)

    def new_task_id(self, run_id, step_name, tags=None, sys_tags=None):
        self._task_id_seq += 1
        task_id = str(self._task_id_seq)
        self._new_task(run_id, step_name, task_id, tags=tags, sys_tags=sys_tags)
        return task_id

    def register_task_id(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        try:
            # Same logic as register_run_id
            int(task_id)
        except ValueError:
            return self._new_task(
                run_id,
                step_name,
                task_id,
                attempt=attempt,
                tags=tags,
                sys_tags=sys_tags,
            )
        else:
            self._register_system_metadata(run_id, step_name, task_id, attempt)
            return False

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
    def _mutate_user_tags_for_run(
        cls, flow_id, run_id, tags_to_add=None, tags_to_remove=None
    ):
        MutationResult = namedtuple(
            "MutationResult", field_names="tags_are_consistent tags"
        )

        def _optimistically_mutate():
            # get existing tags
            run = LocalMetadataProvider.get_object(
                "run", "self", {}, None, flow_id, run_id
            )
            if not run:
                raise MetaflowTaggingError(
                    msg="Run not found (%s, %s)" % (flow_id, run_id)
                )
            existing_user_tag_set = frozenset(run["tags"])
            existing_system_tag_set = frozenset(run["system_tags"])
            tags_to_remove_set = frozenset(tags_to_remove)
            # make sure no existing system tags get added as a user tag
            tags_to_add_set = frozenset(tags_to_add) - existing_system_tag_set

            # from this point on we work with sets of tags only

            if tags_to_remove_set & existing_system_tag_set:
                raise MetaflowTaggingError(
                    msg="Cannot remove a tag that is an existing system tag (%s)"
                    % str(sorted(tags_to_remove_set & existing_system_tag_set))
                )

            # remove tags first, then add
            next_user_tags_set = (
                existing_user_tag_set - tags_to_remove_set
            ) | tags_to_add_set
            # we think it will be a no-op, so let's return right away
            if next_user_tags_set == existing_user_tag_set:
                return MutationResult(
                    tags=next_user_tags_set,
                    tags_are_consistent=True,
                )

            validate_tags(next_user_tags_set, existing_tags=existing_user_tag_set)

            # write new tag set to file system
            LocalMetadataProvider._persist_tags_for_run(
                flow_id, run_id, next_user_tags_set, existing_system_tag_set
            )

            # read tags back from file system to see if our optimism is misplaced
            # I.e. did a concurrent mutate overwrite our change
            run = LocalMetadataProvider.get_object(
                "run", "self", {}, None, flow_id, run_id
            )
            if not run:
                raise MetaflowTaggingError(
                    msg="Run not found for read-back check (%s, %s)" % (flow_id, run_id)
                )
            final_tag_set = frozenset(run["tags"])
            if tags_to_add_set - final_tag_set:
                return MutationResult(tags=final_tag_set, tags_are_consistent=False)
            if (
                tags_to_remove_set & final_tag_set
            ) - tags_to_add_set:  # Remove before add, remember?  Account for this
                return MutationResult(tags=final_tag_set, tags_are_consistent=False)

            return MutationResult(tags=final_tag_set, tags_are_consistent=True)

        tries = 1
        # try up to 5 times, with a gentle exponential backoff (1.1-1.3x)
        while True:
            mutation_result = _optimistically_mutate()
            if mutation_result.tags_are_consistent:
                return mutation_result.tags
            if tries >= 5:
                break
            time.sleep(0.3 * random.uniform(1.1, 1.3) ** tries)
            tries += 1
        raise MetaflowTaggingError(
            "Tagging failed due to too many conflicting updates from other processes"
        )

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        # This is guaranteed by MetaflowProvider.get_object(), sole intended caller
        if obj_type in ("metadata", "self"):
            raise MetaflowInternalError(msg="Type %s is not allowed" % obj_type)

        if obj_type not in ("root", "flow", "run", "step", "task", "artifact"):
            raise MetaflowInternalError(msg="Unexpected object type %s" % obj_type)

        from metaflow.plugins.datastores.local_storage import LocalStorage

        if obj_type == "artifact":
            # Artifacts are actually part of the tasks in the filesystem
            # E.g. we get here for (obj_type, sub_type) == (artifact, self)
            obj_type = "task"
            sub_type = "artifact"
            sub_order = obj_order
            obj_order = obj_order - 1

        if obj_type != ObjectOrder.order_to_type(obj_order):
            raise MetaflowInternalError(
                "Object type order mismatch %s %s"
                % (obj_type, ObjectOrder.order_to_type(obj_order))
            )
        if sub_type != ObjectOrder.order_to_type(sub_order):
            raise MetaflowInternalError(
                "Sub type order mismatch %s %s"
                % (sub_type, ObjectOrder.order_to_type(sub_order))
            )

        RUN_ORDER = ObjectOrder.type_to_order("run")

        if obj_type not in ("root", "flow", "run", "step", "task"):
            raise MetaflowInternalError(msg="Unexpected object type %s" % obj_type)

        # Special handling of self, artifact, and metadata
        if sub_type == "self":
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            if meta_path is None:
                return None
            self_file = os.path.join(meta_path, "_self.json")
            if os.path.isfile(self_file):
                obj = MetadataProvider._apply_filter(
                    [LocalMetadataProvider._read_json_file(self_file)], filters
                )[0]
                # For non-descendants of a run, we are done

                if obj_order <= RUN_ORDER:
                    return obj

                if obj_type not in ("step", "task"):
                    raise MetaflowInternalError(
                        msg="Unexpected object type %s" % obj_type
                    )
                run = LocalMetadataProvider.get_object(
                    "run", "self", {}, None, *args[:RUN_ORDER]  # *[flow_id, run_id]
                )
                if not run:
                    raise MetaflowInternalError(
                        msg="Could not find run %s" % str(args[:RUN_ORDER])
                    )

                obj["tags"] = run.get("tags", [])
                obj["system_tags"] = run.get("system_tags", [])
                return obj
            return None

        if sub_type == "artifact":
            if obj_type not in ("root", "flow", "run", "step", "task"):
                raise MetaflowInternalError(msg="Unexpected object type %s" % obj_type)

            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            result = []
            if meta_path is None:
                return result

            successful_attempt = attempt
            if successful_attempt is None:
                attempt_done_files = os.path.join(meta_path, "sysmeta_attempt-done_*")
                attempts_done = sorted(glob.iglob(attempt_done_files))
                if attempts_done:
                    successful_attempt = int(
                        LocalMetadataProvider._read_json_file(attempts_done[-1])[
                            "value"
                        ]
                    )
            if successful_attempt is not None:
                which_artifact = "*"
                if len(args) >= sub_order:
                    which_artifact = args[sub_order - 1]
                artifact_files = os.path.join(
                    meta_path,
                    "%d_artifact_%s.json" % (successful_attempt, which_artifact),
                )
                for obj in glob.iglob(artifact_files):
                    result.append(LocalMetadataProvider._read_json_file(obj))

            # We are getting artifacts. We should overlay with ancestral run's tags
            run = LocalMetadataProvider.get_object(
                "run", "self", {}, None, *args[:RUN_ORDER]  # *[flow_id, run_id]
            )
            if not run:
                raise MetaflowInternalError(
                    msg="Could not find run %s" % str(args[:RUN_ORDER])
                )
            for obj in result:
                obj["tags"] = run.get("tags", [])
                obj["system_tags"] = run.get("system_tags", [])

            if len(result) == 1:
                return result[0]
            return result

        if sub_type == "metadata":
            # artifact is not expected because if obj_type=artifact on function entry, we transform to =task
            if obj_type not in ("root", "flow", "run", "step", "task"):
                raise MetaflowInternalError(msg="Unexpected object type %s" % obj_type)
            result = []
            meta_path = LocalMetadataProvider._get_metadir(*args[:obj_order])
            if meta_path is None:
                return result
            files = os.path.join(meta_path, "sysmeta_*")
            for obj in glob.iglob(files):
                result.append(LocalMetadataProvider._read_json_file(obj))
            return result

        # For the other types, we locate all the objects we need to find and return them
        if obj_type not in ("root", "flow", "run", "step", "task"):
            raise MetaflowInternalError(msg="Unexpected object type %s" % obj_type)
        if sub_type not in ("flow", "run", "step", "task"):
            raise MetaflowInternalError(msg="unexpected sub type %s" % sub_type)
        obj_path = LocalMetadataProvider._make_path(
            *args[:obj_order], create_on_absent=False
        )
        result = []
        if obj_path is None:
            return result
        skip_dirs = "*/" * (sub_order - obj_order)
        all_meta = os.path.join(obj_path, skip_dirs, LocalStorage.METADATA_DIR)
        SelfInfo = collections.namedtuple("SelfInfo", ["filepath", "run_id"])
        self_infos = []
        for meta_path in glob.iglob(all_meta):
            self_file = os.path.join(meta_path, "_self.json")
            if not os.path.isfile(self_file):
                continue
            run_id = None
            # flow and run do not need info from ancestral run
            if sub_type in ("step", "task"):
                run_id = LocalMetadataProvider._deduce_run_id_from_meta_dir(
                    meta_path, sub_type
                )
                # obj_type IS run, or more granular than run, let's do sanity check vs args
                if obj_order >= RUN_ORDER:
                    if run_id != args[RUN_ORDER - 1]:
                        raise MetaflowInternalError(
                            msg="Unexpected run id %s deduced from meta path" % run_id
                        )
            self_infos.append(SelfInfo(filepath=self_file, run_id=run_id))

        for self_info in self_infos:
            obj = LocalMetadataProvider._read_json_file(self_info.filepath)
            if self_info.run_id:
                flow_id_from_args = args[0]
                run = LocalMetadataProvider.get_object(
                    "run",
                    "self",
                    {},
                    None,
                    flow_id_from_args,
                    self_info.run_id,
                )
                if not run:
                    raise MetaflowInternalError(
                        msg="Could not find run %s, %s"
                        % (flow_id_from_args, self_info.run_id)
                    )
                obj["tags"] = run.get("tags", [])
                obj["system_tags"] = run.get("system_tags", [])
            result.append(obj)

        return MetadataProvider._apply_filter(result, filters)

    @staticmethod
    def _deduce_run_id_from_meta_dir(meta_dir_path, sub_type):
        curr_order = ObjectOrder.type_to_order(sub_type)
        levels_to_ascend = curr_order - ObjectOrder.type_to_order("run")
        if levels_to_ascend < 0:
            return None
        curr_path = meta_dir_path
        for _ in range(levels_to_ascend + 1):  # +1 to account for ../_meta
            curr_path, _ = os.path.split(curr_path)
        _, run_id = os.path.split(curr_path)
        if not run_id:
            raise MetaflowInternalError(
                "Failed to deduce run_id from meta dir %s" % meta_dir_path
            )
        return run_id

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

    @staticmethod
    def _persist_tags_for_run(flow_id, run_id, tags, system_tags):
        subpath = LocalMetadataProvider._create_and_get_metadir(
            flow_name=flow_id, run_id=run_id
        )
        selfname = os.path.join(subpath, "_self.json")
        if not os.path.isfile(selfname):
            raise MetaflowInternalError(
                msg="Could not verify Run existence on disk - missing %s" % selfname
            )
        LocalMetadataProvider._save_meta(
            subpath,
            {
                "_self": MetadataProvider._run_to_json_static(
                    flow_id, run_id=run_id, tags=tags, sys_tags=system_tags
                )
            },
            allow_overwrite=True,
        )

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
            # There is a race here, but we are not aiming to make this as solid as
            # the metadata service. This is used primarily for concurrent resumes,
            # so it is highly unlikely that this combination (multiple resumes of
            # the same flow on the same machine) happens.
            return False
        # In this case the metadata information does not exist, so we create it
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
        return True

    def _new_run(self, run_id, tags=None, sys_tags=None):
        self._ensure_meta("flow", None, None, None)
        return self._ensure_meta("run", run_id, None, None, tags, sys_tags)

    def _new_task(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        self._ensure_meta("step", run_id, step_name, None)
        to_return = self._ensure_meta(
            "task", run_id, step_name, task_id, tags, sys_tags
        )
        self._register_system_metadata(run_id, step_name, task_id, attempt)
        return to_return

    @staticmethod
    def _make_path(
        flow_name=None, run_id=None, step_name=None, task_id=None, create_on_absent=True
    ):

        from metaflow.plugins.datastores.local_storage import LocalStorage

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
        from metaflow.plugins.datastores.local_storage import LocalStorage

        root_path = LocalMetadataProvider._make_path(
            flow_name, run_id, step_name, task_id
        )
        subpath = os.path.join(root_path, LocalStorage.METADATA_DIR)
        LocalMetadataProvider._makedirs(subpath)
        return subpath

    @staticmethod
    def _get_metadir(flow_name=None, run_id=None, step_name=None, task_id=None):
        from metaflow.plugins.datastores.local_storage import LocalStorage

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
    def _dump_json_to_file(filepath, data, allow_overwrite=False):
        if os.path.isfile(filepath) and not allow_overwrite:
            return
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", dir=os.path.dirname(filepath), delete=False
            ) as f:
                json.dump(data, f)
            os.rename(f.name, filepath)
        finally:
            # clean up in case anything goes wrong
            if f and os.path.isfile(f.name):
                os.remove(f.name)

    @staticmethod
    def _read_json_file(filepath):
        with open(filepath, "r") as f:
            return json.load(f)

    @staticmethod
    def _save_meta(root_dir, metadict, allow_overwrite=False):
        for name, datum in metadict.items():
            filename = os.path.join(root_dir, "%s.json" % name)
            LocalMetadataProvider._dump_json_to_file(
                filename, datum, allow_overwrite=allow_overwrite
            )
