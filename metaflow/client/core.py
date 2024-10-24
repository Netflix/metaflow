from __future__ import print_function

import json
import os
import tarfile
from collections import namedtuple
from datetime import datetime
from io import BytesIO
from itertools import chain
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    TYPE_CHECKING,
    Tuple,
)

from metaflow.metaflow_current import current
from metaflow.events import Trigger
from metaflow.exception import (
    MetaflowInternalError,
    MetaflowInvalidPathspec,
    MetaflowNamespaceMismatch,
    MetaflowNotFound,
)
from metaflow.includefile import IncludedFile
from metaflow.metaflow_config import DEFAULT_METADATA, MAX_ATTEMPTS
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins import ENVIRONMENTS, METADATA_PROVIDERS
from metaflow.unbounded_foreach import CONTROL_TASK_TAG
from metaflow.util import cached_property, is_stringish, resolve_identity, to_unicode

from ..info_file import INFO_FILE
from .filecache import FileCache

if TYPE_CHECKING:
    from metaflow.metadata import MetadataProvider

try:
    # python2
    import cPickle as pickle
except:  # noqa E722
    # python3
    import pickle

# populated at the bottom of this file
_CLASSES = {}

Metadata = namedtuple("Metadata", ["name", "value", "created_at", "type", "task"])

filecache = None
current_namespace = False

current_metadata = False


def metadata(ms: str) -> str:
    """
    Switch Metadata provider.

    This call has a global effect. Selecting the local metadata will,
    for example, not allow access to information stored in remote
    metadata providers.

    Note that you don't typically have to call this function directly. Usually
    the metadata provider is set through the Metaflow configuration file. If you
    need to switch between multiple providers, you can use the `METAFLOW_PROFILE`
    environment variable to switch between configurations.

    Parameters
    ----------
    ms : str
        Can be a path (selects local metadata), a URL starting with http (selects
        the service metadata) or an explicit specification <metadata_type>@<info>; as an
        example, you can specify local@<path> or service@<url>.

    Returns
    -------
    str
        The description of the metadata selected (equivalent to the result of
        get_metadata()).
    """
    global current_metadata
    provider, info = _metadata(ms)
    if provider is None:
        print(
            "Cannot find a metadata provider -- "
            "try specifying one explicitly using <type>@<info>",
        )
        return get_metadata()
    current_metadata = provider
    if info:
        current_metadata.INFO = info
    return get_metadata()


def get_metadata() -> str:
    """
    Returns the current Metadata provider.

    If this is not set explicitly using `metadata`, the default value is
    determined through the Metaflow configuration. You can use this call to
    check that your configuration is set up properly.

    If multiple configuration profiles are present, this call returns the one
    selected through the `METAFLOW_PROFILE` environment variable.

    Returns
    -------
    str
        Information about the Metadata provider currently selected. This information typically
        returns provider specific information (like URL for remote providers or local paths for
        local providers).
    """
    if current_metadata is False:
        default_metadata()
    return current_metadata.metadata_str()


def default_metadata() -> str:
    """
    Resets the Metadata provider to the default value, that is, to the value
    that was used prior to any `metadata` calls.

    Returns
    -------
    str
        The result of get_metadata() after resetting the provider.
    """
    global current_metadata

    # We first check if we are in a flow -- if that is the case, we use the
    # metadata provider that is being used there
    if current._metadata_str:
        return metadata(current._metadata_str)

    default = [m for m in METADATA_PROVIDERS if m.TYPE == DEFAULT_METADATA]
    if default:
        current_metadata = default[0]
    else:
        from metaflow.plugins.metadata import LocalMetadataProvider

        current_metadata = LocalMetadataProvider
    return get_metadata()


def namespace(ns: Optional[str]) -> Optional[str]:
    """
    Switch namespace to the one provided.

    This call has a global effect. No objects outside this namespace
    will be accessible. To access all objects regardless of namespaces,
    pass None to this call.

    Parameters
    ----------
    ns : str, optional
        Namespace to switch to or None to ignore namespaces.

    Returns
    -------
    str, optional
        Namespace set (result of get_namespace()).
    """
    global current_namespace
    current_namespace = ns
    return get_namespace()


def get_namespace() -> Optional[str]:
    """
    Return the current namespace that is currently being used to filter objects.

    The namespace is a tag associated with all objects in Metaflow.

    Returns
    -------
    str, optional
        The current namespace used to filter objects.
    """
    # see a comment about namespace initialization
    # in Metaflow.__init__ below
    if current_namespace is False:
        default_namespace()
    return current_namespace


def default_namespace() -> str:
    """
    Resets the namespace used to filter objects to the default one, i.e. the one that was
    used prior to any `namespace` calls.

    Returns
    -------
    str
        The result of get_namespace() after the namespace has been reset.
    """
    global current_namespace
    current_namespace = resolve_identity()
    return get_namespace()


MetaflowArtifacts = NamedTuple


class MetaflowObject(object):
    """
    Base class for all Metaflow objects.

    Creates a new object of a specific type (Flow, Run, Step, Task, DataArtifact) given
    a path to it (its `pathspec`).

    Accessing Metaflow objects is done through one of two methods:
      - either by directly instantiating it with this class
      - or by accessing it through its parent (iterating over
        all children or accessing directly using the [] operator)

    With this class, you can:
      - Get a `Flow`; use `Flow('FlowName')`.
      - Get a `Run` of a flow; use `Run('FlowName/RunID')`.
      - Get a `Step` of a run; use `Step('FlowName/RunID/StepName')`.
      - Get a `Task` of a step, use `Task('FlowName/RunID/StepName/TaskID')`
      - Get a `DataArtifact` of a task; use
           `DataArtifact('FlowName/RunID/StepName/TaskID/ArtifactName')`.

    Attributes
    ----------
    tags : FrozenSet[str]
        Tags associated with the run this object belongs to (user and system tags).
    user_tags: FrozenSet[str]
        User tags associated with the run this object belongs to.
    system_tags: FrozenSet[str]
        System tags associated with the run this object belongs to.
    created_at : datetime
        Date and time this object was first created.
    parent : MetaflowObject
        Parent of this object. The parent of a `Run` is a `Flow` for example
    pathspec : str
        Pathspec of this object (for example: 'FlowName/RunID' for a `Run`)
    path_components : List[str]
        Components of the pathspec
    origin_pathspec : str, optional
        Pathspec of the original object this object was cloned from (in the case of a resume).
        None if not applicable.
    """

    _NAME = "base"
    _CHILD_CLASS = None
    _PARENT_CLASS = None

    def __init__(
        self,
        pathspec: Optional[str] = None,
        attempt: Optional[int] = None,
        _object: Optional["MetaflowObject"] = None,
        _parent: Optional["MetaflowObject"] = None,
        _namespace_check: bool = True,
        _metaflow: Optional["Metaflow"] = None,
        _current_namespace: Optional[str] = None,
        _current_metadata: Optional[str] = None,
    ):
        # the default namespace is activated lazily at the first
        # get_namespace(). The other option of activating
        # the namespace at the import time is problematic, since there
        # may be other modules that alter environment variables etc.
        # which may affect the namespace setting.
        self._metaflow = Metaflow(_current_metadata) or _metaflow
        self._parent = _parent
        self._path_components = None
        self._attempt = attempt
        self._current_namespace = _current_namespace or get_namespace()
        self._namespace_check = _namespace_check
        # If the current namespace is False, we disable checking for namespace for this
        # and all children objects. Not setting namespace_check to False has the consequence
        # of preventing access to children objects after the namespace changes
        if self._current_namespace is None:
            self._namespace_check = False

        if self._attempt is not None:
            if self._NAME not in ["task", "artifact"]:
                raise MetaflowNotFound(
                    "Attempts can only be specified for Task or DataArtifact"
                )
            try:
                self._attempt = int(self._attempt)
            except ValueError:
                raise MetaflowNotFound("Attempt can only be an integer")

            if self._attempt < 0:
                raise MetaflowNotFound("Attempt can only be non-negative")
            elif self._attempt >= MAX_ATTEMPTS:
                raise MetaflowNotFound(
                    "Attempt can only be smaller than %d" % MAX_ATTEMPTS
                )
            # NOTE: It is possible that no attempt exists, but we can't
            # distinguish between "attempt will happen" and "no such
            # attempt exists".

        if pathspec:
            ids = pathspec.split("/")

            if self._NAME == "flow" and len(ids) != 1:
                raise MetaflowInvalidPathspec("Expects Flow('FlowName')")
            elif self._NAME == "run" and len(ids) != 2:
                raise MetaflowInvalidPathspec("Expects Run('FlowName/RunID')")
            elif self._NAME == "step" and len(ids) != 3:
                raise MetaflowInvalidPathspec("Expects Step('FlowName/RunID/StepName')")
            elif self._NAME == "task" and len(ids) != 4:
                raise MetaflowInvalidPathspec(
                    "Expects Task('FlowName/RunID/StepName/TaskID')"
                )
            elif self._NAME == "artifact" and len(ids) != 5:
                raise MetaflowInvalidPathspec(
                    "Expects DataArtifact('FlowName/RunID/StepName/TaskID/ArtifactName')"
                )

            self.id = ids[-1]
            self._pathspec = pathspec
            self._object = self._get_object(*ids)
        else:
            self._object = _object
            self._pathspec = pathspec

            if self._NAME in ("flow", "task"):
                self.id = str(self._object[self._NAME + "_id"])
            elif self._NAME == "run":
                self.id = str(self._object["run_number"])
            elif self._NAME == "step":
                self.id = str(self._object["step_name"])
            elif self._NAME == "artifact":
                self.id = str(self._object["name"])
            else:
                raise MetaflowInternalError(msg="Unknown type: %s" % self._NAME)

        self._created_at = datetime.fromtimestamp(self._object["ts_epoch"] / 1000.0)

        self._tags = frozenset(
            chain(self._object.get("system_tags") or [], self._object.get("tags") or [])
        )
        self._user_tags = frozenset(self._object.get("tags") or [])
        self._system_tags = frozenset(self._object.get("system_tags") or [])

        if self._namespace_check and not self._is_in_namespace(self._current_namespace):
            raise MetaflowNamespaceMismatch(self._current_namespace)

    def _get_object(self, *path_components):
        result = self._metaflow.metadata.get_object(
            self._NAME, "self", None, self._attempt, *path_components
        )
        if not result:
            raise MetaflowNotFound("%s does not exist" % self)
        return result

    def __iter__(self) -> Iterator["MetaflowObject"]:
        """
        Iterate over all child objects of this object if any.

        Note that only children present in the current namespace are returned if and
        only if _namespace_check is set.

        Yields
        ------
        MetaflowObject
            Children of this object
        """
        query_filter = {}

        # skip namespace filtering if _namespace_check is unset.
        if self._namespace_check and self._current_namespace:
            query_filter = {"any_tags": self._current_namespace}

        unfiltered_children = self._metaflow.metadata.get_object(
            self._NAME,
            _CLASSES[self._CHILD_CLASS]._NAME,
            query_filter,
            self._attempt,
            *self.path_components
        )
        unfiltered_children = unfiltered_children if unfiltered_children else []
        children = filter(
            lambda x: self._iter_filter(x),
            (
                _CLASSES[self._CHILD_CLASS](
                    attempt=self._attempt,
                    _object=obj,
                    _parent=self,
                    _metaflow=self._metaflow,
                    _namespace_check=self._namespace_check,
                    _current_namespace=(
                        self._current_namespace if self._namespace_check else None
                    ),
                )
                for obj in unfiltered_children
            ),
        )

        if children:
            return iter(sorted(children, reverse=True, key=lambda x: x.created_at))
        else:
            return iter([])

    def _iter_filter(self, x):
        return True

    def _filtered_children(self, *tags):
        """
        Returns an iterator over all children.

        If tags are specified, only children associated with all specified tags
        are returned.
        """
        for child in self:
            if all(tag in child.tags for tag in tags):
                yield child

    def _ipython_key_completions_(self):
        """Returns available options for ipython auto-complete."""
        return [child.id for child in self._filtered_children()]

    @classmethod
    def _url_token(cls):
        return "%ss" % cls._NAME

    def is_in_namespace(self) -> bool:
        """
        Returns whether this object is in the current namespace.

        If the current namespace is None, this will always return True.

        Returns
        -------
        bool
            Whether or not the object is in the current namespace
        """
        return self._is_in_namespace(current_namespace)

    def _is_in_namespace(self, ns: str) -> bool:
        """
        Returns whether this object is in namespace passed in.

        If the current namespace is None, this will always return True.

        Parameters
        ----------
        ns : str
            Namespace to check if the object is in.
        Returns
        -------
        bool
            Whether or not the object is in the current namespace
        """
        if self._NAME == "flow":
            return any(True for _ in self)
        else:
            return ns is None or ns in self._tags

    def __str__(self):
        if self._attempt is not None:
            return "%s('%s', attempt=%d)" % (
                self.__class__.__name__,
                self.pathspec,
                self._attempt,
            )
        return "%s('%s')" % (self.__class__.__name__, self.pathspec)

    def __repr__(self):
        return str(self)

    def _get_child(self, id):
        result = []
        for p in self.path_components:
            result.append(p)
        result.append(id)
        return self._metaflow.metadata.get_object(
            _CLASSES[self._CHILD_CLASS]._NAME, "self", None, self._attempt, *result
        )

    def __getitem__(self, id: str) -> "MetaflowObject":
        """
        Returns the child object named 'id'.

        Parameters
        ----------
        id : str
            Name of the child object

        Returns
        -------
        MetaflowObject
            Child object

        Raises
        ------
        KeyError
            If the name does not identify a valid child object
        """
        obj = self._get_child(id)
        if obj:
            return _CLASSES[self._CHILD_CLASS](
                attempt=self._attempt,
                _object=obj,
                _parent=self,
                _metaflow=self._metaflow,
                _namespace_check=self._namespace_check,
                _current_namespace=(
                    self._current_namespace if self._namespace_check else None
                ),
            )
        else:
            raise KeyError(id)

    def __contains__(self, id: str):
        """
        Tests whether a child named 'id' exists.

        Parameters
        ----------
        id : str
            Name of the child object

        Returns
        -------
        bool
            True if the child exists or False otherwise
        """
        return bool(self._get_child(id))

    def _unpickle_284(self, data):
        if len(data) != 3:
            raise MetaflowInternalError(
                "Unexpected size of array: {}".format(len(data))
            )
        pathspec, attempt, namespace_check = data
        self.__init__(
            pathspec=pathspec, attempt=attempt, _namespace_check=namespace_check
        )

    def _unpickle_2124(self, data):
        if len(data) != 4:
            raise MetaflowInternalError(
                "Unexpected size of array: {}".format(len(data))
            )
        pathspec, attempt, ns, namespace_check = data
        self.__init__(
            pathspec=pathspec,
            attempt=attempt,
            _namespace_check=namespace_check,
            _current_namespace=ns,
        )

    def _unpickle_21227(self, data):
        if len(data) != 5:
            raise MetaflowInternalError(
                "Unexpected size of array: {}".format(len(data))
            )
        pathspec, attempt, md, ns, namespace_check = data
        self.__init__(
            pathspec=pathspec,
            attempt=attempt,
            _namespace_check=namespace_check,
            _current_metadata=md,
            _current_namespace=ns,
        )

    _UNPICKLE_FUNC = {
        "2.8.4": _unpickle_284,
        "2.12.4": _unpickle_2124,
        "2.12.27": _unpickle_21227,
    }

    def __setstate__(self, state):
        """
        This function is used during the unpickling operation.
        More info here https://docs.python.org/3/library/pickle.html#object.__setstate__
        """
        if "version" in state and "data" in state:
            version = state["version"]
            if version not in self._UNPICKLE_FUNC:
                # this happens when an object pickled using a newer version of Metaflow is
                # being un-pickled using an older version of Metaflow
                raise MetaflowInternalError(
                    "Unpickling this object requires a Metaflow version greater than or equal to {}".format(
                        version
                    )
                )
            self._UNPICKLE_FUNC[version](self, state["data"])
        else:
            # For backward compatibility: handles pickled objects that were serialized without a __getstate__ override
            # We set namespace_check to False if it doesn't exist so that the user can
            # continue accessing this object once unpickled.
            self.__init__(
                pathspec=state.get("_pathspec", None),
                attempt=state.get("_attempt", None),
                _namespace_check=state.get("_namespace_check", False),
                _current_namespace=None,
            )

    def __getstate__(self):
        """
        This function is used during the pickling operation.
        More info here https://docs.python.org/3/library/pickle.html#object.__getstate__

        This function is not forward compatible i.e., if this object (or any of the objects deriving
        from this object) are pickled (serialized) in a later version of Metaflow, it may not be possible
        to unpickle (deserialize) them in a previous version of Metaflow.
        """
        # Note that we now record the namespace at the time of the object creation so
        # we don't need to force namespace_check to be False and can properly continue
        # checking for the namespace even after unpickling since we will know which
        # namespace to check.
        return {
            "version": "2.12.27",
            "data": [
                self.pathspec,
                self._attempt,
                self._metaflow.metadata.metadata_str(),
                self._current_namespace,
                self._namespace_check,
            ],
        }

    @property
    def tags(self) -> FrozenSet[str]:
        """
        Tags associated with this object.

        Tags can be user defined or system defined. This returns all tags associated
        with the object.

        Returns
        -------
        Set[str]
            Tags associated with the object
        """
        return self._tags

    @property
    def system_tags(self) -> FrozenSet[str]:
        """
        System defined tags associated with this object.

        Returns
        -------
        Set[str]
            System tags associated with the object
        """
        return self._system_tags

    @property
    def user_tags(self) -> FrozenSet[str]:
        """
        User defined tags associated with this object.

        Returns
        -------
        Set[str]
            User tags associated with the object
        """
        return self._user_tags

    @property
    def created_at(self) -> datetime:
        """
        Creation time for this object.

        This corresponds to the time the object's existence was first created which typically means
        right before any code is run.

        Returns
        -------
        datetime
            Date time of this object's creation.
        """
        return self._created_at

    @property
    def origin_pathspec(self) -> Optional[str]:
        """
        The pathspec of the object from which the current object was cloned.

        Returns:
            str, optional
                pathspec of the origin object from which current object was cloned.
        """
        origin_pathspec = None
        if self._NAME == "run":
            latest_step = next(self.steps())
            if latest_step and latest_step.task:
                # If we had a step
                task = latest_step.task
                origin_run_id = [
                    m.value for m in task.metadata if m.name == "origin-run-id"
                ]
                if origin_run_id:
                    origin_pathspec = "%s/%s" % (self.parent.id, origin_run_id[0])
        else:
            parent_pathspec = self.parent.origin_pathspec if self.parent else None
            if parent_pathspec:
                my_id = self.id
                origin_task_id = None
                if self._NAME == "task":
                    origin_task_id = [
                        m.value for m in self.metadata if m.name == "origin-task-id"
                    ]
                    if origin_task_id:
                        my_id = origin_task_id[0]
                    else:
                        my_id = None
                if my_id is not None:
                    origin_pathspec = "%s/%s" % (parent_pathspec, my_id)
        return origin_pathspec

    @property
    def parent(self) -> Optional["MetaflowObject"]:
        """
        Returns the parent object of this object or None if none exists.

        Returns
        -------
        MetaflowObject, optional
            The parent of this object
        """
        if self._NAME == "flow":
            return None
        # Compute parent from pathspec and cache it.
        if self._parent is None:
            pathspec = self.pathspec
            parent_pathspec = pathspec[: pathspec.rfind("/")]
            # Only artifacts and tasks have attempts right now, so we get the
            # right parent if we are an artifact.
            attempt_to_pass = self._attempt if self._NAME == "artifact" else None
            # We can skip the namespace check because if self._NAME = 'run',
            # the parent object is guaranteed to be in namespace.
            # Otherwise the check is moot for Flow since parent is singular.
            self._parent = _CLASSES[self._PARENT_CLASS](
                parent_pathspec, attempt=attempt_to_pass, _namespace_check=False
            )
        return self._parent

    @property
    def pathspec(self) -> str:
        """
        Returns a string representation uniquely identifying this object.

        The string is the same as the one you would pass into the constructor
        to build this object except if you are looking for a specific attempt of
        a task or a data artifact (in which case you need to add `attempt=<attempt>`
        in the constructor).

        Returns
        -------
        str
            Unique representation of this object
        """
        if self._pathspec is None:
            if self.parent is None:
                self._pathspec = self.id
            else:
                parent_pathspec = self.parent.pathspec
                self._pathspec = os.path.join(parent_pathspec, self.id)
        return self._pathspec

    @property
    def path_components(self) -> List[str]:
        """
        List of individual components of the pathspec.

        Returns
        -------
        List[str]
            Individual components of the pathspec
        """
        if self._path_components is None:
            ids = self.pathspec.split("/")
            self._path_components = ids
        return list(self._path_components)


class MetaflowCode(object):
    """
    Snapshot of the code used to execute this `Run`. Instantiate the object through
    `Run(...).code` (if any step is executed remotely) or `Task(...).code` for an
    individual task. The code package is the same for all steps of a `Run`.

    `MetaflowCode` includes a package of the user-defined `FlowSpec` class and supporting
    files, as well as a snapshot of the Metaflow library itself.

    Currently, `MetaflowCode` objects are stored only for `Run`s that have at least one `Step`
    executing outside the user's local environment.

    The `TarFile` for the `Run` is given by `Run(...).code.tarball`

    Attributes
    ----------
    path : str
        Location (in the datastore provider) of the code package.
    info : Dict[str, str]
        Dictionary of information related to this code-package.
    flowspec : str
        Source code of the file containing the `FlowSpec` in this code package.
    tarball : TarFile
        Python standard library `tarfile.TarFile` archive containing all the code.
    """

    def __init__(self, flow_name: str, code_package: str):
        global filecache

        self._flow_name = flow_name
        info = json.loads(code_package)
        self._path = info["location"]
        self._ds_type = info["ds_type"]
        self._sha = info["sha"]

        if filecache is None:
            filecache = FileCache()
        _, blobdata = filecache.get_data(
            self._ds_type, self._flow_name, self._path, self._sha
        )
        code_obj = BytesIO(blobdata)
        self._tar = tarfile.open(fileobj=code_obj, mode="r:gz")
        # The JSON module in Python3 deals with Unicode. Tar gives bytes.
        info_str = (
            self._tar.extractfile(os.path.basename(INFO_FILE)).read().decode("utf-8")
        )
        self._info = json.loads(info_str)
        self._flowspec = self._tar.extractfile(self._info["script"]).read()

    @property
    def path(self) -> str:
        """
        Location (in the datastore provider) of the code package.

        Returns
        -------
        str
            Full path of the code package
        """
        return self._path

    @property
    def info(self) -> Dict[str, str]:
        """
        Metadata associated with the code package.

        Returns
        -------
        Dict[str, str]
            Dictionary of metadata. Keys and values are strings
        """
        return self._info

    @property
    def flowspec(self) -> str:
        """
        Source code of the Python file containing the FlowSpec.

        Returns
        -------
        str
            Content of the Python file
        """
        return self._flowspec

    @property
    def tarball(self) -> tarfile.TarFile:
        """
        TarFile for this code package.

        Returns
        -------
        TarFile
            TarFile for everything in this code package
        """
        return self._tar

    def __str__(self):
        return "<MetaflowCode: %s>" % self._info["script"]


class DataArtifact(MetaflowObject):
    """
    A single data artifact and associated metadata. Note that this object does
    not contain other objects as it is the leaf object in the hierarchy.

    Attributes
    ----------
    data : object
        The data contained in this artifact, that is, the object produced during
        execution of this run.
    sha : string
        A unique ID of this artifact.
    finished_at : datetime
        Corresponds roughly to the `Task.finished_at` time of the parent `Task`.
        An alias for `DataArtifact.created_at`.
    """

    _NAME = "artifact"
    _PARENT_CLASS = "task"
    _CHILD_CLASS = None

    @property
    def data(self) -> Any:
        """
        Unpickled representation of the data contained in this artifact.

        Returns
        -------
        object
            Object contained in this artifact
        """
        global filecache

        ds_type = self._object["ds_type"]
        location = self._object["location"]
        components = self.path_components
        if filecache is None:
            # TODO: Pass proper environment to properly extract artifacts
            filecache = FileCache()

        # "create" the metadata information that the datastore needs
        # to access this object.
        # TODO: We can store more information in the metadata, particularly
        #       to determine if we need an environment to unpickle the artifact.
        meta = {
            "objects": {self._object["name"]: self._object["sha"]},
            "info": {
                self._object["name"]: {
                    "size": 0,
                    "type": None,
                    "encoding": self._object["content_type"],
                }
            },
        }
        if location.startswith(":root:"):
            obj = filecache.get_artifact(ds_type, location[6:], meta, *components)
        else:
            # Older artifacts have a location information which we can use.
            obj = filecache.get_artifact_by_location(
                ds_type, location, meta, *components
            )
        if isinstance(obj, IncludedFile):
            return obj.decode(self.id)
        return obj

    @property
    def size(self) -> int:
        """
        Returns the size (in bytes) of the pickled object representing this
        DataArtifact

        Returns
        -------
        int
            size of the pickled representation of data artifact (in bytes)
        """
        global filecache

        ds_type = self._object["ds_type"]
        location = self._object["location"]
        components = self.path_components

        if filecache is None:
            # TODO: Pass proper environment to properly extract artifacts
            filecache = FileCache()
        if location.startswith(":root:"):
            return filecache.get_artifact_size(
                ds_type, location[6:], self._attempt, *components
            )
        else:
            return filecache.get_artifact_size_by_location(
                ds_type, location, self._attempt, *components
            )

    # TODO add
    # @property
    # def type(self)

    @property
    def sha(self) -> str:
        """
        Unique identifier for this artifact.

        This is a unique hash of the artifact (historically SHA1 hash)

        Returns
        -------
        str
            Hash of this artifact
        """
        return self._object["sha"]

    @property
    def finished_at(self) -> datetime:
        """
        Creation time for this artifact.

        Alias for created_at.

        Returns
        -------
        datetime
            Creation time
        """
        return self.created_at

    def __getstate__(self):
        return super(DataArtifact, self).__getstate__()

    def __setstate__(self, state):
        super(DataArtifact, self).__setstate__(state)


class MetaflowData(object):
    """
    Container of data artifacts produced by a `Task`. This object is
    instantiated through `Task.data`.

    `MetaflowData` allows results to be retrieved by their name
    through a convenient dot notation:

    ```python
    Task(...).data.my_object
    ```

    You can also test the existence of an object

    ```python
    if 'my_object' in Task(...).data:
        print('my_object found')
    ```

    Note that this container relies on the local cache to load all data
    artifacts. If your `Task` contains a lot of data, a more efficient
    approach is to load artifacts individually like so

    ```
    Task(...)['my_object'].data
    ```
    """

    def __init__(self, artifacts: Iterable[DataArtifact]):
        self._artifacts = dict((art.id, art) for art in artifacts)

    def __getattr__(self, name: str):
        if name not in self._artifacts:
            raise AttributeError(name)
        return self._artifacts[name].data

    def __contains__(self, var):
        return var in self._artifacts

    def __str__(self):
        return "<MetaflowData: %s>" % ", ".join(self._artifacts)

    def __repr__(self):
        return str(self)


class Task(MetaflowObject):
    """
    A `Task` represents an execution of a `Step`.

    It contains all `DataArtifact` objects produced by the task as
    well as metadata related to execution.

    Note that the `@retry` decorator may cause multiple attempts of
    the task to be present. Usually you want the latest attempt, which
    is what instantiating a `Task` object returns by default. If
    you need to e.g. retrieve logs from a failed attempt, you can
    explicitly get information about a specific attempt by using the
    following syntax when creating a task:

    `Task('flow/run/step/task', attempt=<attempt>)`

    where `attempt=0` corresponds to the first attempt etc.

    Attributes
    ----------
    metadata : List[Metadata]
        List of all metadata events associated with the task.
    metadata_dict : Dict[str, str]
        A condensed version of `metadata`: A dictionary where keys
        are names of metadata events and values the latest corresponding event.
    data : MetaflowData
        Container of all data artifacts produced by this task. Note that this
        call downloads all data locally, so it can be slower than accessing
        artifacts individually. See `MetaflowData` for more information.
    artifacts : MetaflowArtifacts
        Container of `DataArtifact` objects produced by this task.
    successful : bool
        True if the task completed successfully.
    finished : bool
        True if the task completed.
    exception : object
        Exception raised by this task if there was one.
    finished_at : datetime
        Time this task finished.
    runtime_name : str
        Runtime this task was executed on.
    stdout : str
        Standard output for the task execution.
    stderr : str
        Standard error output for the task execution.
    code : MetaflowCode
        Code package for this task (if present). See `MetaflowCode`.
    environment_info : Dict[str, str]
        Information about the execution environment.
    """

    _NAME = "task"
    _PARENT_CLASS = "step"
    _CHILD_CLASS = "artifact"

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

    def _iter_filter(self, x):
        # exclude private data artifacts
        return x.id[0] != "_"

    @property
    def metadata(self) -> List[Metadata]:
        """
        Metadata events produced by this task across all attempts of the task
        *except* if you selected a specific task attempt.

        Note that Metadata is different from tags.

        Returns
        -------
        List[Metadata]
            Metadata produced by this task
        """
        all_metadata = self._metaflow.metadata.get_object(
            self._NAME, "metadata", None, self._attempt, *self.path_components
        )
        all_metadata = all_metadata if all_metadata else []

        # For "clones" (ie: they have an origin-run-id AND a origin-task-id), we
        # copy a set of metadata from the original task. This is needed to make things
        # like logs work (which rely on having proper values for ds-root for example)
        origin_run_id = None
        origin_task_id = None
        result = []
        existing_keys = []
        for obj in all_metadata:
            result.append(
                Metadata(
                    name=obj.get("field_name"),
                    value=obj.get("value"),
                    created_at=obj.get("ts_epoch"),
                    type=obj.get("type"),
                    task=self,
                )
            )
            existing_keys.append(obj.get("field_name"))
            if obj.get("field_name") == "origin-run-id":
                origin_run_id = obj.get("value")
            elif obj.get("field_name") == "origin-task-id":
                origin_task_id = obj.get("value")

        if origin_task_id:
            # This is a "cloned" task. We consider that it has the same
            # metadata as the last attempt of the cloned task.

            origin_obj_pathcomponents = self.path_components
            origin_obj_pathcomponents[1] = origin_run_id
            origin_obj_pathcomponents[3] = origin_task_id
            origin_task = Task(
                "/".join(origin_obj_pathcomponents), _namespace_check=False
            )
            latest_metadata = {
                m.name: m
                for m in sorted(origin_task.metadata, key=lambda m: m.created_at)
            }
            # We point to ourselves in the Metadata object
            for v in latest_metadata.values():
                if v.name in existing_keys:
                    continue
                result.append(
                    Metadata(
                        name=v.name,
                        value=v.value,
                        created_at=v.created_at,
                        type=v.type,
                        task=self,
                    )
                )

        return result

    @property
    def metadata_dict(self) -> Dict[str, str]:
        """
        Dictionary mapping metadata names (keys) and their associated values.

        Note that unlike the metadata() method, this call will only return the latest
        metadata for a given name. For example, if a task executes multiple times (retries),
        the same metadata name will be generated multiple times (one for each execution of the
        task). The metadata() method returns all those metadata elements whereas this call will
        return the metadata associated with the latest execution of the task.

        Returns
        -------
        Dict[str, str]
            Dictionary mapping metadata name with value
        """
        # use the newest version of each key, hence sorting
        return {
            m.name: m.value for m in sorted(self.metadata, key=lambda m: m.created_at)
        }

    @property
    def index(self) -> Optional[int]:
        """
        Returns the index of the innermost foreach loop if this task is run inside at least
        one foreach.

        The index is what distinguishes the various tasks inside a given step.
        This call returns None if this task was not run in a foreach loop.

        Returns
        -------
        int, optional
            Index in the innermost loop for this task
        """
        try:
            return self["_foreach_stack"].data[-1].index
        except (KeyError, IndexError):
            return None

    @property
    def data(self) -> MetaflowData:
        """
        Returns a container of data artifacts produced by this task.

        You can access data produced by this task as follows:
        ```
        print(task.data.my_var)
        ```

        Returns
        -------
        MetaflowData
            Container of all artifacts produced by this task
        """
        return MetaflowData(self)

    @property
    def artifacts(self) -> MetaflowArtifacts:
        """
        Returns a container of DataArtifacts produced by this task.

        You can access each DataArtifact by name like so:
        ```
        print(task.artifacts.my_var)
        ```
        This method differs from data() because it returns DataArtifact objects
        (which contain additional metadata) as opposed to just the data.

        Returns
        -------
        MetaflowArtifacts
            Container of all DataArtifacts produced by this task
        """
        arts = list(self)
        obj = namedtuple("MetaflowArtifacts", [art.id for art in arts])
        return obj._make(arts)

    @property
    def successful(self) -> bool:
        """
        Indicates whether or not the task completed successfully.

        This information is always about the latest task to have completed (in case
        of retries).

        Returns
        -------
        bool
            True if the task completed successfully and False otherwise
        """
        try:
            return self["_success"].data
        except KeyError:
            return False

    @property
    def finished(self) -> bool:
        """
        Indicates whether or not the task completed.

        This information is always about the latest task to have completed (in case
        of retries).

        Returns
        -------
        bool
            True if the task completed and False otherwise
        """
        try:
            return self["_task_ok"].data
        except KeyError:
            return False

    @property
    def exception(self) -> Optional[Any]:
        """
        Returns the exception that caused the task to fail, if any.

        This information is always about the latest task to have completed (in case
        of retries). If successful() returns False and finished() returns True,
        this method can help determine what went wrong.

        Returns
        -------
        object
            Exception raised by the task or None if not applicable
        """
        try:
            return self["_exception"].data
        except KeyError:
            return None

    @property
    def finished_at(self) -> Optional[datetime]:
        """
        Returns the datetime object of when the task finished (successfully or not).

        This information is always about the latest task to have completed (in case
        of retries). This call will return None if the task is not finished.

        Returns
        -------
        datetime
            Datetime of when the task finished
        """
        try:
            return self["_task_ok"].created_at
        except KeyError:
            return None

    @property
    def runtime_name(self) -> Optional[str]:
        """
        Returns the name of the runtime this task executed on.


        Returns
        -------
        str
            Name of the runtime this task executed on
        """
        for t in self._tags:
            if t.startswith("runtime:"):
                return t.split(":")[1]
        return None

    @property
    def stdout(self) -> str:
        """
        Returns the full standard out of this task.

        If you specify a specific attempt for this task, it will return the
        standard out for that attempt. If you do not specify an attempt,
        this will return the current standard out for the latest *started*
        attempt of the task. In both cases, multiple calls to this
        method will return the most up-to-date log (so if an attempt is not
        done, each call will fetch the latest log).

        Returns
        -------
        str
            Standard output of this task
        """
        return self._load_log("stdout")

    @property
    def stdout_size(self) -> int:
        """
        Returns the size of the stdout log of this task.

        Similar to `stdout`, the size returned is the latest size of the log
        (so for a running attempt, this value will increase as the task produces
        more output).

        Returns
        -------
        int
            Size of the stdout log content (in bytes)
        """
        return self._get_logsize("stdout")

    @property
    def stderr(self) -> str:
        """
        Returns the full standard error of this task.

        If you specify a specific attempt for this task, it will return the
        standard error for that attempt. If you do not specify an attempt,
        this will return the current standard error for the latest *started*
        attempt. In both cases, multiple calls to this
        method will return the most up-to-date log (so if an attempt is not
        done, each call will fetch the latest log).

        Returns
        -------
        str
            Standard error of this task
        """
        return self._load_log("stderr")

    @property
    def stderr_size(self) -> int:
        """
        Returns the size of the stderr log of this task.

        Similar to `stderr`, the size returned is the latest size of the log
        (so for a running attempt, this value will increase as the task produces
        more output).

        Returns
        -------
        int
            Size of the stderr log content (in bytes)
        """
        return self._get_logsize("stderr")

    @property
    def current_attempt(self) -> int:
        """
        Get the relevant attempt for this Task.

        Returns the specific attempt used when
        initializing the instance, or the latest *started* attempt for the Task.

        Returns
        -------
        int
            attempt id for this task object
        """
        if self._attempt is not None:
            attempt = self._attempt
        else:
            # It is possible that a task fails before any metadata has been
            # recorded. In this case, we assume that we are executing the
            # first attempt.
            #
            # FIXME: Technically we are looking at the latest *recorded* attempt
            # here. It is possible that logs exists for a newer attempt that
            # just failed to record metadata. We could make this logic more robust
            # and guarantee that we always return the latest available log.
            attempt = int(self.metadata_dict.get("attempt", 0))
        return attempt

    @cached_property
    def code(self) -> Optional[MetaflowCode]:
        """
        Returns the MetaflowCode object for this task, if present.

        Not all tasks save their code so this call may return None in those cases.

        Returns
        -------
        MetaflowCode
            Code package for this task
        """
        code_package = self.metadata_dict.get("code-package")
        if code_package:
            return MetaflowCode(self.path_components[0], code_package)
        return None

    @cached_property
    def environment_info(self) -> Dict[str, Any]:
        """
        Returns information about the environment that was used to execute this task. As an
        example, if the Conda environment is selected, this will return information about the
        dependencies that were used in the environment.

        This environment information is only available for tasks that have a code package.

        Returns
        -------
        Dict
            Dictionary describing the environment
        """
        my_code = self.code
        if not my_code:
            return None
        env_type = my_code.info["environment_type"]
        if not env_type:
            return None
        env = [m for m in ENVIRONMENTS + [MetaflowEnvironment] if m.TYPE == env_type][0]
        meta_dict = self.metadata_dict
        return env.get_client_info(self.path_components[0], meta_dict)

    def _load_log(self, stream):
        meta_dict = self.metadata_dict
        log_location = meta_dict.get("log_location_%s" % stream)
        if log_location:
            return self._load_log_legacy(log_location, stream)
        else:
            return "".join(
                line + "\n" for _, line in self.loglines(stream, meta_dict=meta_dict)
            )

    def _get_logsize(self, stream):
        meta_dict = self.metadata_dict
        log_location = meta_dict.get("log_location_%s" % stream)
        if log_location:
            return self._legacy_log_size(log_location, stream)
        else:
            return self._log_size(stream, meta_dict)

    def loglines(
        self,
        stream: str,
        as_unicode: bool = True,
        meta_dict: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Tuple[datetime, str]]:
        """
        Return an iterator over (utc_timestamp, logline) tuples.

        Parameters
        ----------
        stream : str
            Either 'stdout' or 'stderr'.
        as_unicode : bool, default: True
            If as_unicode=False, each logline is returned as a byte object. Otherwise,
            it is returned as a (unicode) string.

        Yields
        ------
        Tuple[datetime, str]
            Tuple of timestamp, logline pairs.
        """
        from metaflow.mflog.mflog import merge_logs

        global filecache

        if meta_dict is None:
            meta_dict = self.metadata_dict
        ds_type = meta_dict.get("ds-type")
        ds_root = meta_dict.get("ds-root")
        if ds_type is None or ds_root is None:
            yield None, ""
            return
        if filecache is None:
            filecache = FileCache()

        attempt = self.current_attempt
        logs = filecache.get_logs_stream(
            ds_type, ds_root, stream, attempt, *self.path_components
        )
        for line in merge_logs([blob for _, blob in logs]):
            msg = to_unicode(line.msg) if as_unicode else line.msg
            yield line.utc_tstamp, msg

    def _load_log_legacy(self, log_location, logtype, as_unicode=True):
        # this function is used to load pre-mflog style logfiles
        global filecache

        log_info = json.loads(log_location)
        location = log_info["location"]
        ds_type = log_info["ds_type"]
        attempt = log_info["attempt"]
        if filecache is None:
            filecache = FileCache()
        ret_val = filecache.get_log_legacy(
            ds_type, location, logtype, int(attempt), *self.path_components
        )
        if as_unicode and (ret_val is not None):
            return ret_val.decode(encoding="utf8")
        else:
            return ret_val

    def _legacy_log_size(self, log_location, logtype):
        global filecache

        log_info = json.loads(log_location)
        location = log_info["location"]
        ds_type = log_info["ds_type"]
        attempt = log_info["attempt"]
        if filecache is None:
            filecache = FileCache()

        return filecache.get_legacy_log_size(
            ds_type, location, logtype, int(attempt), *self.path_components
        )

    def _log_size(self, stream, meta_dict):
        global filecache

        ds_type = meta_dict.get("ds-type")
        ds_root = meta_dict.get("ds-root")
        if ds_type is None or ds_root is None:
            return 0
        if filecache is None:
            filecache = FileCache()
        attempt = self.current_attempt

        return filecache.get_log_size(
            ds_type, ds_root, stream, attempt, *self.path_components
        )

    def __iter__(self) -> Iterator[DataArtifact]:
        """
        Iterate over all children DataArtifact of this Task

        Yields
        ------
        DataArtifact
            A DataArtifact in this Step
        """
        for d in super(Task, self).__iter__():
            yield d

    def __getitem__(self, name: str) -> DataArtifact:
        """
        Returns the DataArtifact object with the artifact name 'name'

        Parameters
        ----------
        name : str
            Data artifact name

        Returns
        -------
        DataArtifact
            DataArtifact for this artifact name in this task

        Raises
        ------
        KeyError
            If the name does not identify a valid DataArtifact object
        """
        return super(Task, self).__getitem__(name)

    def __getstate__(self):
        return super(Task, self).__getstate__()

    def __setstate__(self, state):
        super(Task, self).__setstate__(state)


class Step(MetaflowObject):
    """
    A `Step` represents a user-defined step, that is, a method annotated with the `@step` decorator.

    It contains `Task` objects associated with the step, that is, all executions of the
    `Step`. The step may contain multiple `Task`s in the case of a foreach step.

    Attributes
    ----------
    task : Task
        The first `Task` object in this step. This is a shortcut for retrieving the only
        task contained in a non-foreach step.
    finished_at : datetime
        Time when the latest `Task` of this step finished. Note that in the case of foreaches,
        this time may change during execution of the step.
    environment_info : Dict[str, Any]
        Information about the execution environment.
    """

    _NAME = "step"
    _PARENT_CLASS = "run"
    _CHILD_CLASS = "task"

    @property
    def task(self) -> Optional[Task]:
        """
        Returns a Task object belonging to this step.

        This is useful when the step only contains one task (a linear step for example).

        Returns
        -------
        Task
            A task in the step
        """
        for t in self:
            return t

    def tasks(self, *tags: str) -> Iterable[Task]:
        """
        [Legacy function - do not use]

        Returns an iterator over all `Task` objects in the step. This is an alias
        to iterating the object itself, i.e.
        ```
        list(Step(...)) == list(Step(...).tasks())
        ```

        Parameters
        ----------
        tags : str
            No op (legacy functionality)

        Yields
        ------
        Task
            `Task` objects in this step.
        """
        return self._filtered_children(*tags)

    @property
    def control_task(self) -> Optional[Task]:
        """
        [Unpublished API - use with caution!]

        Returns a Control Task object belonging to this step.
        This is useful when the step only contains one control task.

        Returns
        -------
        Task
            A control task in the step
        """
        return next(self.control_tasks(), None)

    def control_tasks(self, *tags: str) -> Iterator[Task]:
        """
        [Unpublished API - use with caution!]

        Returns an iterator over all the control tasks in the step.
        An optional filter is available that allows you to filter on tags. The
        control tasks returned if the filter is specified will contain all the
        tags specified.
        Parameters
        ----------
        tags : str
            Tags to match

        Yields
        ------
        Task
            Control Task objects for this step
        """
        children = super(Step, self).__iter__()
        for child in children:
            # first filter by standard tag filters
            if not all(tag in child.tags for tag in tags):
                continue
            # Then look for control task indicator in one of two ways
            # Look in tags - this path will activate for metadata service
            # backends that pre-date tag mutation release
            if CONTROL_TASK_TAG in child.tags:
                yield child
            else:
                # Look in task metadata
                for task_metadata in child.metadata:
                    if (
                        task_metadata.name == "internal_task_type"
                        and task_metadata.value == CONTROL_TASK_TAG
                    ):
                        yield child

    def __iter__(self) -> Iterator[Task]:
        """
        Iterate over all children Task of this Step

        Yields
        ------
        Task
            A Task in this Step
        """
        for t in super(Step, self).__iter__():
            yield t

    def __getitem__(self, task_id: str) -> Task:
        """
        Returns the Task object with the task ID 'task_id'

        Parameters
        ----------
        task_id : str
            Task ID

        Returns
        -------
        Task
            Task for this task ID in this Step

        Raises
        ------
        KeyError
            If the task_id does not identify a valid Task object
        """
        return super(Step, self).__getitem__(task_id)

    def __getstate__(self):
        return super(Step, self).__getstate__()

    def __setstate__(self, state):
        super(Step, self).__setstate__(state)

    @property
    def finished_at(self) -> Optional[datetime]:
        """
        Returns the datetime object of when the step finished (successfully or not).

        A step is considered finished when all the tasks that belong to it have
        finished. This call will return None if the step has not finished

        Returns
        -------
        datetime
            Datetime of when the step finished
        """
        try:
            return max(task.finished_at for task in self)
        except TypeError:
            # Raised if None is present in max
            return None

    @property
    def environment_info(self) -> Optional[Dict[str, Any]]:
        """
        Returns information about the environment that was used to execute this step. As an
        example, if the Conda environment is selected, this will return information about the
        dependencies that were used in the environment.

        This environment information is only available for steps that have tasks
        for which the code package has been saved.

        Returns
        -------
        Dict[str, Any], optional
            Dictionary describing the environment
        """
        # All tasks have the same environment info so just use the first one
        for t in self:
            return t.environment_info


class Run(MetaflowObject):
    """
    A `Run` represents an execution of a `Flow`. It is a container of `Step`s.

    Attributes
    ----------
    data : MetaflowData
        a shortcut to run['end'].task.data, i.e. data produced by this run.
    successful : bool
        True if the run completed successfully.
    finished : bool
        True if the run completed.
    finished_at : datetime
        Time this run finished.
    code : MetaflowCode
        Code package for this run (if present). See `MetaflowCode`.
    trigger : MetaflowTrigger
        Information about event(s) that triggered this run (if present). See `MetaflowTrigger`.
    end_task : Task
        `Task` for the end step (if it is present already).
    """

    _NAME = "run"
    _PARENT_CLASS = "flow"
    _CHILD_CLASS = "step"

    def _iter_filter(self, x):
        # exclude _parameters step
        return x.id[0] != "_"

    def steps(self, *tags: str) -> Iterator[Step]:
        """
        [Legacy function - do not use]

        Returns an iterator over all `Step` objects in the step. This is an alias
        to iterating the object itself, i.e.
        ```
        list(Run(...)) == list(Run(...).steps())
        ```

        Parameters
        ----------
        tags : str
            No op (legacy functionality)

        Yields
        ------
        Step
            `Step` objects in this run.
        """
        return self._filtered_children(*tags)

    @property
    def code(self) -> Optional[MetaflowCode]:
        """
        Returns the MetaflowCode object for this run, if present.
        Code is packed if atleast one `Step` runs remotely, else None is returned.

        Returns
        -------
        MetaflowCode, optional
            Code package for this run
        """
        # Note that this can be quite slow in the edge-case where the codepackage is only available
        # for the last step on the list. Steps are reverse-ordered, so the worst-case scenario is
        # if the start step executes remotely and every step after that is remote.
        #
        # TODO: A more optimized way of figuring out if a run has remote steps (and thus a codepackage) available.
        # This might require changes to the metadata-service as well.
        for step in self:
            if step.task:
                code = step.task.code
                if code:
                    return code

    @property
    def data(self) -> Optional[MetaflowData]:
        """
        Returns a container of data artifacts produced by this run.

        You can access data produced by this run as follows:
        ```
        print(run.data.my_var)
        ```
        This is a shorthand for `run['end'].task.data`. If the 'end' step has not yet
        executed, returns None.

        Returns
        -------
        MetaflowData, optional
            Container of all artifacts produced by this task
        """
        end = self.end_task
        if end:
            return end.data

    @property
    def successful(self) -> bool:
        """
        Indicates whether or not the run completed successfully.

        A run is successful if its 'end' step is successful.

        Returns
        -------
        bool
            True if the run completed successfully and False otherwise
        """
        end = self.end_task
        if end:
            return end.successful
        else:
            return False

    @property
    def finished(self) -> bool:
        """
        Indicates whether or not the run completed.

        A run completed if its 'end' step completed.

        Returns
        -------
        bool
            True if the run completed and False otherwise
        """
        end = self.end_task
        if end:
            return end.finished
        else:
            return False

    @property
    def finished_at(self) -> Optional[datetime]:
        """
        Returns the datetime object of when the run finished (successfully or not).

        The completion time of a run is the same as the completion time of its 'end' step.
        If the 'end' step has not completed, returns None.

        Returns
        -------
        datetime, optional
            Datetime of when the run finished
        """
        end = self.end_task
        if end:
            return end.finished_at

    @property
    def end_task(self) -> Optional[Task]:
        """
        Returns the Task corresponding to the 'end' step.

        This returns None if the end step does not yet exist.

        Returns
        -------
        Task, optional
            The 'end' task
        """
        try:
            end_step = self["end"]
        except KeyError:
            return None

        return end_step.task

    def add_tag(self, tag: str):
        """
        Add a tag to this `Run`.

        Note that if the tag is already a system tag, it is not added as a user tag,
        and no error is thrown.

        Parameters
        ----------
        tag : str
            Tag to add.
        """

        # For backwards compatibility with Netflix's early version of this functionality,
        # this function shall accept both an individual tag AND iterables of tags.
        #
        # Iterable of tags support shall be removed in future once existing
        # usage has been migrated off.
        if is_stringish(tag):
            tag = [tag]
        return self.replace_tag([], tag)

    def add_tags(self, tags: Iterable[str]):
        """
        Add one or more tags to this `Run`.

        Note that if any tag is already a system tag, it is not added as a user tag
        and no error is thrown.

        Parameters
        ----------
        tags : Iterable[str]
            Tags to add.
        """
        return self.replace_tag([], tags)

    def remove_tag(self, tag: str):
        """
        Remove one tag from this `Run`.

        Removing a system tag is an error. Removing a non-existent
        user tag is a no-op.

        Parameters
        ----------
        tag : str
            Tag to remove.
        """

        # For backwards compatibility with Netflix's early version of this functionality,
        # this function shall accept both an individual tag AND iterables of tags.
        #
        # Iterable of tags support shall be removed in future once existing
        # usage has been migrated off.
        if is_stringish(tag):
            tag = [tag]
        return self.replace_tag(tag, [])

    def remove_tags(self, tags: Iterable[str]):
        """
        Remove one or more tags to this `Run`.

        Removing a system tag will result in an error. Removing a non-existent
        user tag is a no-op.

        Parameters
        ----------
        tags : Iterable[str]
            Tags to remove.
        """
        return self.replace_tags(tags, [])

    def replace_tag(self, tag_to_remove: str, tag_to_add: str):
        """
        Remove a tag and add a tag atomically. Removal is done first.
        The rules for `Run.add_tag` and `Run.remove_tag` also apply here.

        Parameters
        ----------
        tag_to_remove : str
            Tag to remove.
        tag_to_add : str
            Tag to add.
        """

        # For backwards compatibility with Netflix's early version of this functionality,
        # this function shall accept both individual tags AND iterables of tags.
        #
        # Iterable of tags support shall be removed in future once existing
        # usage has been migrated off.
        if is_stringish(tag_to_remove):
            tag_to_remove = [tag_to_remove]
        if is_stringish(tag_to_add):
            tag_to_add = [tag_to_add]
        return self.replace_tags(tag_to_remove, tag_to_add)

    def replace_tags(self, tags_to_remove: Iterable[str], tags_to_add: Iterable[str]):
        """
        Remove and add tags atomically; the removal is done first.
        The rules for `Run.add_tag` and `Run.remove_tag` also apply here.

        Parameters
        ----------
        tags_to_remove : Iterable[str]
            Tags to remove.
        tags_to_add : Iterable[str]
            Tags to add.
        """
        flow_id = self.path_components[0]
        final_user_tags = self._metaflow.metadata.mutate_user_tags_for_run(
            flow_id, self.id, tags_to_remove=tags_to_remove, tags_to_add=tags_to_add
        )
        # refresh Run object with the latest tags
        self._user_tags = frozenset(final_user_tags)
        self._tags = frozenset([*self._user_tags, *self._system_tags])

    def __iter__(self) -> Iterator[Step]:
        """
        Iterate over all children Step of this Run

        Yields
        ------
        Step
            A Step in this Run
        """
        for s in super(Run, self).__iter__():
            yield s

    def __getitem__(self, name: str) -> Step:
        """
        Returns the Step object with the step name 'name'

        Parameters
        ----------
        name : str
            Step name

        Returns
        -------
        Step
            Step for this step name in this Run

        Raises
        ------
        KeyError
            If the name does not identify a valid Step object
        """
        return super(Run, self).__getitem__(name)

    def __getstate__(self):
        return super(Run, self).__getstate__()

    def __setstate__(self, state):
        super(Run, self).__setstate__(state)

    @property
    def trigger(self) -> Optional[Trigger]:
        """
        Returns a container of events that triggered this run.

        This returns None if the run was not triggered by any events.

        Returns
        -------
        Trigger, optional
            Container of triggering events
        """
        if "start" in self and self["start"].task:
            meta = self["start"].task.metadata_dict.get("execution-triggers")
            if meta:
                return Trigger(json.loads(meta))
        return None


class Flow(MetaflowObject):
    """
    A Flow represents all existing flows with a certain name, in other words,
    classes derived from `FlowSpec`. A container of `Run` objects.

    Attributes
    ----------
    latest_run : Run
        Latest `Run` (in progress or completed, successfully or not) of this flow.
    latest_successful_run : Run
        Latest successfully completed `Run` of this flow.
    """

    _NAME = "flow"
    _PARENT_CLASS = None
    _CHILD_CLASS = "run"

    def __init__(self, *args, **kwargs):
        super(Flow, self).__init__(*args, **kwargs)

    @property
    def latest_run(self) -> Optional[Run]:
        """
        Returns the latest run (either in progress or completed) of this flow.

        Note that an in-progress run may be returned by this call. Use latest_successful_run
        to get an object representing a completed successful run.

        Returns
        -------
        Run, optional
            Latest run of this flow
        """
        for run in self:
            return run

    @property
    def latest_successful_run(self) -> Optional[Run]:
        """
        Returns the latest successful run of this flow.

        Returns
        -------
        Run, optional
            Latest successful run of this flow
        """
        for run in self:
            if run.successful:
                return run

    def runs(self, *tags: str) -> Iterator[Run]:
        """
        Returns an iterator over all `Run`s of this flow.

        An optional filter is available that allows you to filter on tags.
        If multiple tags are specified, only runs that have all the
        specified tags are returned.

        Parameters
        ----------
        tags : str
            Tags to match.

        Yields
        ------
        Run
            `Run` objects in this flow.
        """
        return self._filtered_children(*tags)

    def __iter__(self) -> Iterator[Task]:
        """
        Iterate over all children Run of this Flow.

        Note that only runs in the current namespace are returned unless
        _namespace_check is False

        Yields
        ------
        Run
            A Run in this Flow
        """
        for r in super(Flow, self).__iter__():
            yield r

    def __getitem__(self, run_id: str) -> Run:
        """
        Returns the Run object with the run ID 'run_id'

        Parameters
        ----------
        run_id : str
            Run OD

        Returns
        -------
        Run
            Run for this run ID in this Flow

        Raises
        ------
        KeyError
            If the run_id does not identify a valid Run object
        """
        return super(Flow, self).__getitem__(run_id)

    def __getstate__(self):
        return super(Flow, self).__getstate__()

    def __setstate__(self, state):
        super(Flow, self).__setstate__(state)


class Metaflow(object):
    """
    Entry point to all objects in the Metaflow universe.

    This object can be used to list all the flows present either through the explicit property
    or by iterating over this object.

    Attributes
    ----------
    flows : List[Flow]
        Returns the list of all `Flow` objects known to this metadata provider. Note that only
        flows present in the current namespace will be returned. A `Flow` is present in a namespace
        if it has at least one run in the namespace.
    """

    def __init__(self, _current_metadata: Optional[str] = None):
        if _current_metadata:
            provider, info = _metadata(_current_metadata)
            self.metadata = provider
            if info:
                self.metadata.INFO = info
        else:
            if current_metadata is False:
                default_metadata()
            self.metadata = current_metadata

    @property
    def flows(self) -> List[Flow]:
        """
        Returns a list of all the flows present.

        Only flows present in the set namespace are returned. A flow is present in a namespace if
        it has at least one run that is in the namespace.

        Returns
        -------
        List[Flow]
            List of all flows present.
        """
        return list(self)

    def __iter__(self) -> Iterator[Flow]:
        """
        Iterator over all flows present.

        Only flows present in the set namespace are returned. A flow is present in a
        namespace if it has at least one run that is in the namespace.

        Yields
        -------
        Flow
            A Flow present in the Metaflow universe.
        """
        # We do not filter on namespace in the request because
        # filtering on namespace on flows means finding at least one
        # run in this namespace. This is_in_namespace() function
        # does this properly in this case
        all_flows = self.metadata.get_object("root", "flow", None, None)
        all_flows = all_flows if all_flows else []
        for flow in all_flows:
            try:
                v = Flow(_object=flow, _metaflow=self)
                yield v
            except MetaflowNamespaceMismatch:
                continue

    def __str__(self) -> str:
        return "Metaflow()"

    def __getitem__(self, name: str) -> Flow:
        """
        Returns a specific flow by name.

        The flow will only be returned if it is present in the current namespace.

        Parameters
        ----------
        name : str
            Name of the Flow

        Returns
        -------
        Flow
            Flow with the given name.
        """
        return Flow(name, _metaflow=self)


def _metadata(ms: str) -> Tuple[Optional["MetadataProvider"], Optional[str]]:
    infos = ms.split("@", 1)
    types = [m.TYPE for m in METADATA_PROVIDERS]
    if infos[0] in types:
        provider = [m for m in METADATA_PROVIDERS if m.TYPE == infos[0]][0]
        if len(infos) > 1:
            return provider, infos[1]
        return provider, None
    # Deduce from ms; if starts with http, use service or else use local
    if ms.startswith("http"):
        metadata_type = "service"
    else:
        metadata_type = "local"
    res = [m for m in METADATA_PROVIDERS if m.TYPE == metadata_type]
    if not res:
        return None, None
    return res[0], ms


_CLASSES["flow"] = Flow
_CLASSES["run"] = Run
_CLASSES["step"] = Step
_CLASSES["task"] = Task
_CLASSES["artifact"] = DataArtifact
