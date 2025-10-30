from collections import defaultdict
import json
import pickle
import sys
import time

from functools import wraps
from io import BufferedIOBase, FileIO, RawIOBase
from typing import List, Optional
from types import MethodType, FunctionType

from .. import metaflow_config
from ..exception import MetaflowInternalError
from ..metadata_provider import DataArtifact, MetaDatum
from ..parameters import Parameter
from ..util import Path, is_stringish, to_fileobj

from .exceptions import DataException, UnpicklableArtifactException

_included_file_type = "<class 'metaflow.includefile.IncludedFile'>"


def only_if_not_done(f):
    @wraps(f)
    def method(self, *args, **kwargs):
        if self._is_done_set:
            raise MetaflowInternalError(
                "Tried to write to datastore "
                "(method %s) after it was marked "
                ".done()" % f.__name__
            )
        return f(self, *args, **kwargs)

    return method


def require_mode(mode):
    def wrapper(f):
        @wraps(f)
        def method(self, *args, **kwargs):
            if mode is not None and self._mode != mode:
                raise MetaflowInternalError(
                    "Attempting a datastore operation '%s' requiring mode '%s' "
                    "but have mode '%s'" % (f.__name__, mode, self._mode)
                )
            return f(self, *args, **kwargs)

        return method

    return wrapper


class ArtifactTooLarge(object):
    def __str__(self):
        return "< artifact too large >"


class TaskDataStore(object):
    """
    TaskDataStore is obtained through FlowDataStore.get_datastore_for_task and
    is used to store three things:
        - Task artifacts (using save_artifacts and load_artifacts) which will
          ultimately be stored using ContentAddressedStore's save_blobs and
          load_blobs. This is basically the content indexed portion of the
          storage (identical objects are stored only once).
        - Metadata information (using save_metadata and load_metadata) which
          stores JSON encoded metadata about a task in a non-content indexed
          way in a hierarchical manner (ie: the files are stored
          in a path indicated by the pathspec (run_id/step_name/task_id)).
          This portion of the store can be viewed as name indexed (storing
          two metadata items with the same name will overwrite the previous item
          so the condition of equality is the name as
          opposed to the content).
        - Logs which are a special sort of task metadata but are handled
          differently (they are not JSON-encodable dictionaries).
    """

    METADATA_ATTEMPT_SUFFIX = "attempt.json"
    METADATA_DONE_SUFFIX = "DONE.lock"
    METADATA_DATA_SUFFIX = "data.json"

    @staticmethod
    def metadata_name_for_attempt(name, attempt):
        if attempt is None:
            return name
        return "%d.%s" % (attempt, name)

    @staticmethod
    def parse_attempt_metadata(name):
        return name.split(".", 1)

    def __init__(
        self,
        flow_datastore,
        run_id,
        step_name,
        task_id,
        attempt=None,
        data_metadata=None,
        mode="r",
        allow_not_done=False,
        persist=True,
    ):
        self._storage_impl = flow_datastore._storage_impl
        self.TYPE = self._storage_impl.TYPE
        self._ca_store = flow_datastore.ca_store
        self._environment = flow_datastore.environment
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._path = self._storage_impl.path_join(
            flow_datastore.flow_name, run_id, step_name, task_id
        )
        self._mode = mode
        self._attempt = attempt
        self._metadata = flow_datastore.metadata
        self._parent = flow_datastore
        self._persist = persist

        # The GZIP encodings are for backward compatibility
        self._encodings = {"pickle-v2", "gzip+pickle-v2"}
        ver = sys.version_info[0] * 10 + sys.version_info[1]
        if ver >= 36:
            self._encodings.add("pickle-v4")
            self._encodings.add("gzip+pickle-v4")

        self._is_done_set = False

        # If the mode is 'write', we initialize things to empty
        if self._mode == "w":
            self._objects = {}
            self._info = {}
        elif self._mode == "r":
            if data_metadata is not None:
                # We already loaded the data metadata so just use that
                self._objects = data_metadata.get("objects", {})
                self._info = data_metadata.get("info", {})
            else:
                # What is the latest attempt ID for this task store.
                # NOTE: We *only* access to the data if the attempt that
                # produced it is done. In particular, we do not allow access to
                # a past attempt if a new attempt has started to avoid
                # inconsistencies (depending on when the user accesses the
                # datastore, the data may change). We make an exception to that
                # rule when allow_not_done is True which allows access to things
                # like logs even for tasks that did not write a done marker
                max_attempt = None
                for i in range(metaflow_config.MAX_ATTEMPTS):
                    check_meta = self._metadata_name_for_attempt(
                        self.METADATA_ATTEMPT_SUFFIX, i
                    )
                    if self.has_metadata(check_meta, add_attempt=False):
                        max_attempt = i
                    elif max_attempt is not None:
                        break
                if self._attempt is None:
                    self._attempt = max_attempt
                elif max_attempt is None or self._attempt > max_attempt:
                    # In this case the attempt does not exist, so we can't load
                    # anything
                    self._objects = {}
                    self._info = {}
                    return

                # Check if the latest attempt was completed successfully except
                # if we have allow_not_done
                data_obj = None
                if self.has_metadata(self.METADATA_DONE_SUFFIX):
                    data_obj = self.load_metadata([self.METADATA_DATA_SUFFIX])
                    data_obj = data_obj[self.METADATA_DATA_SUFFIX]
                elif self._attempt is None or not allow_not_done:
                    raise DataException(
                        "No completed attempts of the task was found for task '%s'"
                        % self._path
                    )

                if data_obj is not None:
                    self._objects = data_obj.get("objects", {})
                    self._info = data_obj.get("info", {})
        elif self._mode == "d":
            self._objects = {}
            self._info = {}

            if self._attempt is None:
                for i in range(metaflow_config.MAX_ATTEMPTS):
                    check_meta = self._metadata_name_for_attempt(
                        self.METADATA_ATTEMPT_SUFFIX, i
                    )
                    if self.has_metadata(check_meta, add_attempt=False):
                        self._attempt = i

            # Do not allow destructive operations on the datastore if attempt is still in flight
            # and we explicitly did not allow operating on running tasks.
            if not allow_not_done and not self.has_metadata(self.METADATA_DONE_SUFFIX):
                raise DataException(
                    "No completed attempts of the task was found for task '%s'"
                    % self._path
                )

        else:
            raise DataException("Unknown datastore mode: '%s'" % self._mode)

    @property
    def pathspec(self):
        return "/".join([self.run_id, self.step_name, self.task_id])

    @property
    def run_id(self):
        return self._run_id

    @property
    def step_name(self):
        return self._step_name

    @property
    def task_id(self):
        return self._task_id

    @property
    def attempt(self):
        return self._attempt

    @property
    def ds_metadata(self):
        return {"objects": self._objects.copy(), "info": self._info.copy()}

    @property
    def pathspec_index(self):
        idxstr = ",".join(map(str, (f.index for f in self["_foreach_stack"])))
        if "_iteration_stack" in self:
            itrstr = ",".join(map(str, (f for f in self["_iteration_stack"])))
            return "%s/%s[%s][%s]" % (self._run_id, self._step_name, idxstr, itrstr)
        return "%s/%s[%s]" % (self._run_id, self._step_name, idxstr)

    @property
    def parent_datastore(self):
        return self._parent

    @require_mode(None)
    def get_log_location(self, logprefix, stream):
        log_name = self._get_log_location(logprefix, stream)
        path = self._storage_impl.path_join(
            self._path, self._metadata_name_for_attempt(log_name)
        )
        return self._storage_impl.full_uri(path)

    @require_mode("r")
    def keys_for_artifacts(self, names):
        return [self._objects.get(name) for name in names]

    @only_if_not_done
    @require_mode("w")
    def init_task(self):
        """
        Call this to initialize the datastore with a new attempt.

        This method requires mode 'w'.
        """
        self.save_metadata({self.METADATA_ATTEMPT_SUFFIX: {"time": time.time()}})

    @only_if_not_done
    @require_mode("w")
    def transfer_artifacts(
        self, other_datastore: "TaskDataStore", names: Optional[List[str]] = None
    ):
        """
        Copies the blobs from other_datastore to this datastore if the datastore roots
        are different.

        This is used specifically for spin so we can bring in artifacts from the original
        datastore.

        Parameters
        ----------
        other_datastore : TaskDataStore
            Other datastore from which to copy artifacts from
        names : List[str], optional, default None
            If provided, only transfer the artifacts with these names. If None,
            transfer all artifacts from the other datastore.
        """
        if (
            other_datastore.TYPE == self.TYPE
            and other_datastore._storage_impl.datastore_root
            == self._storage_impl.datastore_root
        ):
            # Nothing to transfer -- artifacts are already saved properly
            return

        # Determine which artifacts need to be transferred
        if names is None:
            # Transfer all artifacts from other datastore
            artifacts_to_transfer = list(other_datastore._objects.keys())
        else:
            # Transfer only specified artifacts
            artifacts_to_transfer = [
                name for name in names if name in other_datastore._objects
            ]

        if not artifacts_to_transfer:
            return

        # Get SHA keys for artifacts to transfer
        shas_to_transfer = [
            other_datastore._objects[name] for name in artifacts_to_transfer
        ]

        # Check which blobs are missing locally
        missing_shas = []
        for sha in shas_to_transfer:
            local_path = self._ca_store._storage_impl.path_join(
                self._ca_store._prefix, sha[:2], sha
            )
            if not self._ca_store._storage_impl.is_file([local_path])[0]:
                missing_shas.append(sha)

        if not missing_shas:
            return  # All blobs already exist locally

        # Load blobs from other datastore in transfer mode
        transfer_blobs = other_datastore._ca_store.load_blobs(
            missing_shas, is_transfer=True
        )

        # Save blobs to local datastore in transfer mode
        self._ca_store.save_blobs(transfer_blobs, is_transfer=True)

    @only_if_not_done
    @require_mode("w")
    def save_artifacts(self, artifacts_iter, len_hint=0):
        """
        Saves Metaflow Artifacts (Python objects) to the datastore and stores
        any relevant metadata needed to retrieve them.

        Typically, objects are pickled but the datastore may perform any
        operation that it deems necessary. You should only access artifacts
        using load_artifacts

        This method requires mode 'w'.

        Parameters
        ----------
        artifacts : Iterator[(string, object)]
            Iterator over the human-readable name of the object to save
            and the object itself
        len_hint: integer
            Estimated number of items in artifacts_iter
        """
        artifact_names = []

        def pickle_iter():
            for name, obj in artifacts_iter:
                encode_type = "gzip+pickle-v4"
                if encode_type in self._encodings:
                    try:
                        blob = pickle.dumps(obj, protocol=4)
                    except TypeError as e:
                        raise UnpicklableArtifactException(name) from e
                else:
                    try:
                        blob = pickle.dumps(obj, protocol=2)
                        encode_type = "gzip+pickle-v2"
                    except (SystemError, OverflowError) as e:
                        raise DataException(
                            "Artifact *%s* is very large (over 2GB). "
                            "You need to use Python 3.6 or newer if you want to "
                            "serialize large objects." % name
                        ) from e
                    except TypeError as e:
                        raise UnpicklableArtifactException(name) from e

                self._info[name] = {
                    "size": len(blob),
                    "type": str(type(obj)),
                    "encoding": encode_type,
                }

                artifact_names.append(name)
                yield blob

        # Use the content-addressed store to store all artifacts
        save_result = self._ca_store.save_blobs(pickle_iter(), len_hint=len_hint)
        for name, result in zip(artifact_names, save_result):
            self._objects[name] = result.key

    @require_mode(None)
    def load_artifacts(self, names):
        """
        Mirror function to save_artifacts

        This function will retrieve the objects referenced by 'name'. Each
        object will be fetched and returned if found. Note that this function
        will return objects that may not be the same as the ones saved using
        saved_objects (taking into account possible environment changes, for
        example different conda environments) but it will return objects that
        can be used as the objects passed in to save_objects.

        This method can be used in both 'r' and 'w' mode. For the latter use
        case, this can happen when `passdown_partial` is called and an artifact
        passed down that way is then loaded.

        Parameters
        ----------
        names : List[string]
            List of artifacts to retrieve

        Returns
        -------
        Iterator[(string, object)] :
            An iterator over objects retrieved.
        """
        if not self._info:
            raise DataException(
                "Datastore for task '%s' does not have the required metadata to "
                "load artifacts" % self._path
            )
        to_load = defaultdict(list)
        for name in names:
            info = self._info.get(name)
            # We use gzip+pickle-v2 as this is the oldest/most compatible.
            # This datastore will always include the proper encoding version so
            # this is just to be able to read very old artifacts
            if info:
                encode_type = info.get("encoding", "gzip+pickle-v2")
            else:
                encode_type = "gzip+pickle-v2"
            if encode_type not in self._encodings:
                raise DataException(
                    "Python 3.6 or later is required to load artifact '%s'" % name
                )
            else:
                to_load[self._objects[name]].append(name)
        # At this point, we load what we don't have from the CAS
        # We assume that if we have one "old" style artifact, all of them are
        # like that which is an easy assumption to make since artifacts are all
        # stored by the same implementation of the datastore for a given task.
        for key, blob in self._ca_store.load_blobs(to_load.keys()):
            names = to_load[key]
            for name in names:
                # We unpickle everytime to have fully distinct objects (the user
                # would not expect two artifacts with different names to actually
                # be aliases of one another)
                yield name, pickle.loads(blob)

    @require_mode("r")
    def get_artifact_sizes(self, names):
        """
        Retrieves file sizes of artifacts defined in 'names' from their respective
        stored file metadata.

        Usage restricted to only 'r' mode due to depending on the metadata being written

        Parameters
        ----------
        names : List[string]
            List of artifacts to retrieve

        Returns
        -------
        Iterator[(string, int)] :
            An iterator over sizes retrieved.
        """
        for name in names:
            info = self._info.get(name)
            if info["type"] == _included_file_type:
                sz = self[name].size
            else:
                sz = info.get("size", 0)
            yield name, sz

    @require_mode("r")
    def get_legacy_log_size(self, stream):
        name = self._metadata_name_for_attempt("%s.log" % stream)
        path = self._storage_impl.path_join(self._path, name)

        return self._storage_impl.size_file(path)

    @require_mode("r")
    def get_log_size(self, logsources, stream):
        def _path(s):
            # construct path for fetching of a single log source
            _p = self._metadata_name_for_attempt(self._get_log_location(s, stream))
            return self._storage_impl.path_join(self._path, _p)

        paths = list(map(_path, logsources))
        sizes = [self._storage_impl.size_file(p) for p in paths]

        return sum(size for size in sizes if size is not None)

    @only_if_not_done
    @require_mode("w")
    def save_metadata(self, contents, allow_overwrite=True, add_attempt=True):
        """
        Save task metadata. This is very similar to save_artifacts; this
        function takes a dictionary with the key being the name of the metadata
        to save and the value being the metadata.
        The metadata, however, will not be stored in the CAS but rather directly
        in the TaskDataStore.

        This method requires mode 'w'

        Parameters
        ----------
        contents : Dict[string -> JSON-ifiable objects]
            Dictionary of metadata to store
        allow_overwrite : boolean, optional
            If True, allows the overwriting of the metadata, defaults to True
        add_attempt : boolean, optional
            If True, adds the attempt identifier to the metadata. defaults to
            True
        """
        return self._save_file(
            {k: json.dumps(v).encode("utf-8") for k, v in contents.items()},
            allow_overwrite,
            add_attempt,
        )

    @require_mode("w")
    def _dangerous_save_metadata_post_done(
        self, contents, allow_overwrite=True, add_attempt=True
    ):
        """
        Method identical to save_metadata BUT BYPASSES THE CHECK ON DONE

        @warning This method should not be used unless you know what you are doing. This
        will write metadata to a datastore that has been marked as done which is an
        assumption that other parts of metaflow rely on (ie: when a datastore is marked
        as done, it is considered to be read-only).

        Currently only used in the case when the task is executed remotely but there is
        no (remote) metadata service configured. We therefore use the datastore to share
        metadata between the task and the Metaflow local scheduler. Due to some other
        constraints and the current plugin API, we could not use the regular method
        to save metadata.

        This method requires mode 'w'

        Parameters
        ----------
        contents : Dict[string -> JSON-ifiable objects]
            Dictionary of metadata to store
        allow_overwrite : boolean, optional
            If True, allows the overwriting of the metadata, defaults to True
        add_attempt : boolean, optional
            If True, adds the attempt identifier to the metadata. defaults to
            True
        """
        return self._save_file(
            {k: json.dumps(v).encode("utf-8") for k, v in contents.items()},
            allow_overwrite,
            add_attempt,
        )

    @require_mode("r")
    def load_metadata(self, names, add_attempt=True):
        """
        Loads metadata saved with `save_metadata`

        Parameters
        ----------
        names : List[string]
            The name of the metadata elements to load
        add_attempt : bool, optional
            Adds the attempt identifier to the metadata name if True,
            by default True

        Returns
        -------
        Dict: string -> JSON decoded object
            Results indexed by the name of the metadata loaded
        """
        transformer = lambda x: x
        if sys.version_info < (3, 6):
            transformer = lambda x: x.decode("utf-8")
        return {
            k: json.loads(transformer(v)) if v is not None else None
            for k, v in self._load_file(names, add_attempt).items()
        }

    @require_mode(None)
    def has_metadata(self, name, add_attempt=True):
        """
        Checks if this TaskDataStore has the metadata requested

        TODO: Should we make this take multiple names like the other calls?

        This method operates like load_metadata in both 'w' and 'r' modes.

        Parameters
        ----------
        names : string
            Metadata name to fetch
        add_attempt : bool, optional
            Adds the attempt identifier to the metadata name if True,
            by default True

        Returns
        -------
        boolean
            True if the metadata exists or False otherwise
        """
        if add_attempt:
            path = self._storage_impl.path_join(
                self._path, self._metadata_name_for_attempt(name)
            )
        else:
            path = self._storage_impl.path_join(self._path, name)
        return self._storage_impl.is_file([path])[0]

    @require_mode(None)
    def get(self, name, default=None):
        """
        Convenience method around load_artifacts for a given name and with a
        provided default.

        This method requires mode 'r'.

        Parameters
        ----------
        name : str
            Name of the object to get
        default : object, optional
            Returns this value if object not found, by default None
        """
        if self._objects:
            try:
                return self[name] if name in self._objects else default
            except DataException:
                return default
        return default

    @require_mode("r")
    def is_none(self, name):
        """
        Convenience method to test if an artifact is None

        This method requires mode 'r'.

        Parameters
        ----------
        name : string
            Name of the artifact
        """
        if not self._info:
            return True
        info = self._info.get(name)
        if info:
            obj_type = info.get("type")
            # Conservatively check if the actual object is None,
            # in case the artifact is stored using a different python version.
            # Note that if an object is None and stored in Py2 and accessed in
            # Py3, this test will fail and we will fall back to the slow path. This
            # is intended (being conservative)
            if obj_type == str(type(None)):
                return True
        # Slow path since this has to get the object from the datastore
        return self.get(name) is None

    @only_if_not_done
    @require_mode("w")
    def done(self):
        """
        Mark this task-datastore as 'done' for the current attempt

        Will throw an exception if mode != 'w'
        """
        self.save_metadata(
            {
                self.METADATA_DATA_SUFFIX: {
                    "datastore": self.TYPE,
                    "version": "1.0",
                    "attempt": self._attempt,
                    "python_version": sys.version,
                    "objects": self._objects,
                    "info": self._info,
                },
                self.METADATA_DONE_SUFFIX: "",
            }
        )

        if self._metadata:
            self._metadata.register_metadata(
                self._run_id,
                self._step_name,
                self._task_id,
                [
                    MetaDatum(
                        field="attempt-done",
                        value=str(self._attempt),
                        type="attempt-done",
                        tags=["attempt_id:{0}".format(self._attempt)],
                    )
                ],
            )
            artifacts = [
                DataArtifact(
                    name=var,
                    ds_type=self.TYPE,
                    ds_root=self._storage_impl.datastore_root,
                    url=None,
                    sha=sha,
                    type=self._info[var]["encoding"],
                )
                for var, sha in self._objects.items()
            ]

            self._metadata.register_data_artifacts(
                self.run_id, self.step_name, self.task_id, self._attempt, artifacts
            )

        self._is_done_set = True

    @only_if_not_done
    @require_mode("w")
    def clone(self, origin):
        """
        Clone the information located in the TaskDataStore origin into this
        datastore

        Parameters
        ----------
        origin : TaskDataStore
            TaskDataStore to clone
        """
        self._objects = origin._objects
        self._info = origin._info

    @only_if_not_done
    @require_mode("w")
    def passdown_partial(self, origin, variables):
        # Pass-down from datastore origin all information related to vars to
        # this datastore. In other words, this adds to the current datastore all
        # the variables in vars (obviously, it does not download them or
        # anything but records information about them). This is used to
        # propagate parameters between datastores without actually loading the
        # parameters as well as for merge_artifacts
        for var in variables:
            sha = origin._objects.get(var)
            if sha:
                self._objects[var] = sha
                self._info[var] = origin._info[var]

    @only_if_not_done
    @require_mode("w")
    def persist(self, flow):
        """
        Persist any new artifacts that were produced when running flow

        NOTE: This is a DESTRUCTIVE operation that deletes artifacts from
        the given flow to conserve memory. Don't rely on artifact attributes
        of the flow object after calling this function.

        Parameters
        ----------
        flow : FlowSpec
            Flow to persist
        """
        if not self._persist:
            return

        if flow._datastore:
            self._objects.update(flow._datastore._objects)
            self._info.update(flow._datastore._info)

        # Scan flow object FIRST
        valid_artifacts = []
        current_artifact_names = set()
        for var in dir(flow):
            if var.startswith("__") or var in flow._EPHEMERAL:
                continue
            # Skip over properties of the class (Parameters or class variables)
            if hasattr(flow.__class__, var) and isinstance(
                getattr(flow.__class__, var), property
            ):
                continue

            val = getattr(flow, var)
            if not (
                isinstance(val, MethodType)
                or isinstance(val, FunctionType)
                or isinstance(val, Parameter)
            ):
                valid_artifacts.append((var, val))
                current_artifact_names.add(var)

        # Transfer ONLY artifacts that aren't being overridden
        if hasattr(flow._datastore, "orig_datastore"):
            parent_artifacts = set(flow._datastore._objects.keys())
            unchanged_artifacts = parent_artifacts - current_artifact_names
            if unchanged_artifacts:
                self.transfer_artifacts(
                    flow._datastore.orig_datastore, names=list(unchanged_artifacts)
                )

        def artifacts_iter():
            # we consume the valid_artifacts list destructively to
            # make sure we don't keep references to artifacts. We
            # want to avoid keeping original artifacts and encoded
            # artifacts in memory simultaneously
            while valid_artifacts:
                var, val = valid_artifacts.pop()
                if not var.startswith("_") and var != "name":
                    # NOTE: Destructive mutation of the flow object. We keep
                    # around artifacts called 'name' and anything starting with
                    # '_' as they are used by the Metaflow runtime.
                    delattr(flow, var)
                yield var, val

        # Save current artifacts
        self.save_artifacts(artifacts_iter(), len_hint=len(valid_artifacts))

    @only_if_not_done
    @require_mode("w")
    def save_logs(self, logsource, stream_data):
        """
        Save log files for multiple streams, represented as
        a dictionary of streams. Each stream is identified by a type (a string)
        and is either a stringish or a BytesIO object or a Path object.

        Parameters
        ----------
        logsource : string
            Identifies the source of the stream (runtime, task, etc)

        stream_data : Dict[string -> bytes or Path]
            Each entry should have a string as the key indicating the type
            of the stream ('stderr', 'stdout') and as value should be bytes or
            a Path from which to stream the log.
        """
        to_store_dict = {}
        for stream, data in stream_data.items():
            n = self._get_log_location(logsource, stream)
            if isinstance(data, Path):
                to_store_dict[n] = FileIO(str(data), mode="r")
            else:
                to_store_dict[n] = data
        self._save_file(to_store_dict)

    @require_mode("d")
    def scrub_logs(self, logsources, stream, attempt_override=None):
        path_logsources = {
            self._metadata_name_for_attempt(
                self._get_log_location(s, stream),
                attempt_override=attempt_override,
            ): s
            for s in logsources
        }

        # Legacy log paths
        legacy_log = self._metadata_name_for_attempt(
            "%s.log" % stream, attempt_override
        )
        path_logsources[legacy_log] = stream

        existing_paths = [
            path
            for path in path_logsources.keys()
            if self.has_metadata(path, add_attempt=False)
        ]

        # Replace log contents with [REDACTED source stream]
        to_store_dict = {
            path: bytes("[REDACTED %s %s]" % (path_logsources[path], stream), "utf-8")
            for path in existing_paths
        }

        self._save_file(to_store_dict, add_attempt=False, allow_overwrite=True)

    @require_mode("r")
    def load_log_legacy(self, stream, attempt_override=None):
        """
        Load old-style, pre-mflog, log file represented as a bytes object.
        """
        name = self._metadata_name_for_attempt("%s.log" % stream, attempt_override)
        r = self._load_file([name], add_attempt=False)[name]
        return r if r is not None else b""

    @require_mode("r")
    def load_logs(self, logsources, stream, attempt_override=None):
        paths = dict(
            map(
                lambda s: (
                    self._metadata_name_for_attempt(
                        self._get_log_location(s, stream),
                        attempt_override=attempt_override,
                    ),
                    s,
                ),
                logsources,
            )
        )
        r = self._load_file(paths.keys(), add_attempt=False)
        return [(paths[k], v if v is not None else b"") for k, v in r.items()]

    @require_mode(None)
    def items(self):
        if self._objects:
            return self._objects.items()
        return {}

    @require_mode(None)
    def to_dict(self, show_private=False, max_value_size=None, include=None):
        d = {}
        for k, _ in self.items():
            if include and k not in include:
                continue
            if k[0] == "_" and not show_private:
                continue

            info = self._info[k]
            if max_value_size is not None:
                if info["type"] == _included_file_type:
                    sz = self[k].size
                else:
                    sz = info.get("size", 0)

                if sz == 0 or sz > max_value_size:
                    d[k] = ArtifactTooLarge()
                else:
                    d[k] = self[k]
                    if info["type"] == _included_file_type:
                        d[k] = d[k].decode(k)
            else:
                d[k] = self[k]
                if info["type"] == _included_file_type:
                    d[k] = d[k].decode(k)

        return d

    @require_mode("r")
    def format(self, **kwargs):
        def lines():
            for k, v in self.to_dict(**kwargs).items():
                if self._info[k]["type"] == _included_file_type:
                    sz = self[k].size
                else:
                    sz = self._info[k]["size"]
                yield k, "*{key}* [size: {size} type: {type}] = {value}".format(
                    key=k, value=v, size=sz, type=self._info[k]["type"]
                )

        return "\n".join(line for k, line in sorted(lines()))

    @require_mode(None)
    def __contains__(self, name):
        if self._objects:
            return name in self._objects
        return False

    @require_mode(None)
    def __getitem__(self, name):
        _, obj = next(self.load_artifacts([name]))
        return obj

    @require_mode("r")
    def __iter__(self):
        if self._objects:
            return iter(self._objects)
        return iter([])

    @require_mode("r")
    def __str__(self):
        return self.format(show_private=True, max_value_size=1000)

    def _metadata_name_for_attempt(self, name, attempt_override=None):
        return self.metadata_name_for_attempt(
            name, self._attempt if attempt_override is None else attempt_override
        )

    @staticmethod
    def _get_log_location(logprefix, stream):
        return "%s_%s.log" % (logprefix, stream)

    def _save_file(self, contents, allow_overwrite=True, add_attempt=True):
        """
        Saves files in the directory for this TaskDataStore. This can be
        metadata, a log file or any other data that doesn't need to (or
        shouldn't) be stored in the Content Addressed Store.

        Parameters
        ----------
        contents : Dict[string -> stringish or RawIOBase or BufferedIOBase]
            Dictionary of file to store
        allow_overwrite : boolean, optional
            If True, allows the overwriting of the metadata, defaults to True
        add_attempt : boolean, optional
            If True, adds the attempt identifier to the metadata,
            defaults to True
        """

        def blob_iter():
            for name, value in contents.items():
                if add_attempt:
                    path = self._storage_impl.path_join(
                        self._path, self._metadata_name_for_attempt(name)
                    )
                else:
                    path = self._storage_impl.path_join(self._path, name)
                if isinstance(value, (RawIOBase, BufferedIOBase)) and value.readable():
                    yield path, value
                elif is_stringish(value):
                    yield path, to_fileobj(value)
                else:
                    raise DataException(
                        "Metadata '%s' for task '%s' has an invalid type: %s"
                        % (name, self._path, type(value))
                    )

        self._storage_impl.save_bytes(blob_iter(), overwrite=allow_overwrite)

    def _load_file(self, names, add_attempt=True):
        """
        Loads files from the TaskDataStore directory. These can be metadata,
        logs or any other files

        Parameters
        ----------
        names : List[string]
            The names of the files to load
        add_attempt : bool, optional
            Adds the attempt identifier to the metadata name if True,
            by default True

        Returns
        -------
        Dict: string -> bytes
            Results indexed by the name of the metadata loaded
        """
        to_load = []
        for name in names:
            if add_attempt:
                path = self._storage_impl.path_join(
                    self._path, self._metadata_name_for_attempt(name)
                )
            else:
                path = self._storage_impl.path_join(self._path, name)
            to_load.append(path)
        results = {}
        with self._storage_impl.load_bytes(to_load) as load_results:
            for key, path, meta in load_results:
                if add_attempt:
                    _, name = self.parse_attempt_metadata(
                        self._storage_impl.basename(key)
                    )
                else:
                    name = self._storage_impl.basename(key)
                if path is None:
                    results[name] = None
                else:
                    with open(path, "rb") as f:
                        results[name] = f.read()
        return results
