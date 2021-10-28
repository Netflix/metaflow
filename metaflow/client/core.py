from __future__ import print_function
from datetime import datetime
import os
import tarfile
import json
from io import BytesIO
from collections import namedtuple
from itertools import chain

from metaflow.metadata.metadata import MetadataOperation
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.exception import (
    MetaflowNotFound,
    MetaflowNamespaceMismatch,
    MetaflowInternalError,
    TaggingException,
)

from metaflow.metaflow_config import DEFAULT_METADATA, MAX_ATTEMPTS
from metaflow.plugins import ENVIRONMENTS, METADATA_PROVIDERS
from metaflow.unbounded_foreach import CONTROL_TASK_TAG
from metaflow.util import cached_property, is_stringish, resolve_identity, to_unicode

from .filecache import FileCache

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


def metadata(ms):
    """
    Switch Metadata provider.

    This call has a global effect. Selecting the local metadata will,
    for example, not allow access to information stored in remote
    metadata providers.

    Parameters
    ----------
    ms : string
        Can be a path (selects local metadata), a URL starting with http (selects
        the service metadata) or an explicit specification <metadata_type>@<info>; as an
        example, you can specify local@<path> or service@<url>.

    Returns
    -------
    string
        The description of the metadata selected (equivalent to the result of
        get_metadata())
    """
    global current_metadata
    infos = ms.split("@", 1)
    types = [m.TYPE for m in METADATA_PROVIDERS]
    if infos[0] in types:
        current_metadata = [m for m in METADATA_PROVIDERS if m.TYPE == infos[0]][0]
        if len(infos) > 1:
            current_metadata.INFO = infos[1]
    else:
        # Deduce from ms; if starts with http, use service or else use local
        if ms.startswith("http"):
            metadata_type = "service"
        else:
            metadata_type = "local"
        res = [m for m in METADATA_PROVIDERS if m.TYPE == metadata_type]
        if not res:
            print(
                "Cannot find a '%s' metadata provider -- "
                "try specifying one explicitly using <type>@<info>",
                metadata_type,
            )
            return get_metadata()
        current_metadata = res[0]
        current_metadata.INFO = ms
    return get_metadata()


def get_metadata():
    """
    Returns the current Metadata provider.

    This call returns the current Metadata being used to return information
    about Metaflow objects.

    If this is not set explicitly using metadata(), the default value is
    determined through environment variables.

    Returns
    -------
    string
        Information about the Metadata provider currently selected. This information typically
        returns provider specific information (like URL for remote providers or local paths for
        local providers).
    """
    if current_metadata is False:
        default_metadata()
    return "%s@%s" % (current_metadata.TYPE, current_metadata.INFO)


def default_metadata():
    """
    Resets the Metadata provider to the default value.

    The default value of the Metadata provider is determined through a combination of
    environment variables.

    Returns
    -------
    string
        The result of get_metadata() after resetting the provider.
    """
    global current_metadata
    default = [m for m in METADATA_PROVIDERS if m.TYPE == DEFAULT_METADATA]
    if default:
        current_metadata = default[0]
    else:
        from metaflow.plugins.metadata import LocalMetadataProvider

        current_metadata = LocalMetadataProvider
    return get_metadata()


def namespace(ns):
    """
    Switch namespace to the one provided.

    This call has a global effect. No objects outside this namespace
    will be accessible. To access all objects regardless of namespaces,
    pass None to this call.

    Parameters
    ----------
    ns : string
        Namespace to switch to or None to ignore namespaces.

    Returns
    -------
    string
        Namespace set (result of get_namespace()).
    """
    global current_namespace
    current_namespace = ns
    return get_namespace()


def get_namespace():
    """
    Return the current namespace that is currently being used to filter objects.

    The namespace is a tag associated with all objects in Metaflow.

    Returns
    -------
    string or None
        The current namespace used to filter objects.
    """
    # see a comment about namespace initialization
    # in Metaflow.__init__ below
    if current_namespace is False:
        default_namespace()
    return current_namespace


def default_namespace():
    """
    Sets or resets the namespace used to filter objects.

    The default namespace is in the form 'user:<username>' and is intended to filter
    objects belonging to the user.

    Returns
    -------
    string
        The result of get_namespace() after
    """
    global current_namespace
    current_namespace = resolve_identity()
    return get_namespace()


class Metaflow(object):
    """
    Entry point to all objects in the Metaflow universe.

    This object can be used to list all the flows present either through the explicit property
    or by iterating over this object.

    Attributes
    ----------
    flows : List of all flows.
        Returns the list of all flows. Note that only flows present in the set namespace will
        be returned. A flow is present in a namespace if it has at least one run in the
        namespace.

    """

    def __init__(self):
        # the default namespace is activated lazily at the first object
        # invocation or get_namespace(). The other option of activating
        # the namespace at the import time is problematic, since there
        # may be other modules that alter environment variables etc.
        # which may affect the namescape setting.
        if current_namespace is False:
            default_namespace()
        if current_metadata is False:
            default_metadata()
        self.metadata = current_metadata

    @property
    def flows(self):
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

    def __iter__(self):
        """
        Iterator over all flows present.

        Only flows present in the set namespace are returned. A flow is present in a namespace if
        it has at least one run that is in the namespace.

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
                v = Flow(_object=flow)
                yield v
            except MetaflowNamespaceMismatch:
                continue

    def __str__(self):
        return "Metaflow()"

    def __getitem__(self, id):
        """
        Returns a specific flow by name.

        The flow will only be returned if it is present in the current namespace.

        Parameters
        ----------
        id : string
            Name of the Flow

        Returns
        -------
        Flow
            Flow with the given ID.
        """
        return Flow(id)


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
    tags : Set
        User and system tags associated with the object.
    system_tags : Set
        System tags associated with the object
    created_at : datetime
        Date and time this object was first created.
    parent : MetaflowObject
        Parent of this object. The parent of a `Run` is a `Flow` for example
    pathspec : string
        Pathspec of this object (for example: 'FlowName/RunID' for a `Run`)
    path_components : List[string]
        Components of the pathspec
    origin_pathspec : str
        Pathspec of the original object this object was cloned from (in the case of a resume).
        None if not applicable.
    """

    _NAME = "base"
    _CHILD_CLASS = None
    _PARENT_CLASS = None

    def __init__(
        self,
        pathspec=None,
        attempt=None,
        _object=None,
        _parent=None,
        _namespace_check=True,
    ):
        self._metaflow = Metaflow()
        self._parent = _parent
        self._path_components = None
        self._attempt = attempt

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
            # NOTE: It is possible that no attempt exists but we can't
            # distinguish between "attempt will happen" and "no such
            # attempt exists".

        if pathspec:
            ids = pathspec.split("/")

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

        # Keep system tags and a set union of system tags + customized tags
        self._system_tags = self._get_tags_set(self._object.get("system_tags"), None)
        self._tags = self._get_tags_set(self._system_tags, self._object.get("tags"))

        if _namespace_check and not self.is_in_namespace():
            raise MetaflowNamespaceMismatch(current_namespace)

    def _get_tags_set(self, system_tags, tags):
        return frozenset(chain(system_tags or [], tags or []))

    def _get_object(self, *path_components):
        result = self._metaflow.metadata.get_object(
            self._NAME, "self", None, self._attempt, *path_components
        )
        if not result:
            raise MetaflowNotFound("%s does not exist" % self)
        return result

    def __iter__(self):
        """
        Iterate over all child objects of this object if any.

        Note that only children present in the current namespace are returned.

        Returns
        -------
        Iterator[MetaflowObject]
            Iterator over all children
        """
        query_filter = {}
        if current_namespace:
            query_filter = {"any_tags": current_namespace}

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
                    _namespace_check=False,
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

    @classmethod
    def _url_token(cls):
        return "%ss" % cls._NAME

    def _tag_operations(self, operations):
        if len(operations) == 0:
            raise TaggingException("At least one tagging operation must be provided")
        formatted_operations = []
        # Perform some sanity checks to report more precisely to the user
        for o, t in operations:
            if o not in ["add", "remove"]:
                raise TaggingException("Invalid tagging operation %s" % o)
            if o == "add" and t in self._system_tags:
                # We don't throw an error but just don't send it; it's a no-op
                continue
            if o == "remove":
                if t in self._system_tags:
                    raise TaggingException("Cannot remove system tag *%s*" % t)
            formatted_operations.append(
                MetadataOperation(
                    op_type="tags",
                    operation=o,
                    id=self.pathspec,
                    object_type=self._NAME,
                    args={"tag": t},
                )
            )
        result = self._metaflow.metadata.perform_operations(formatted_operations).get(
            self.pathspec, None
        )
        if result and result["status"] == "ok":
            self._object = result["data"]
        else:
            raise TaggingException(
                "Unable to perform tagging operation on %s: %s"
                % (
                    self.pathspec,
                    ": ".join([result["status"], result["data"]])
                    if result
                    else "<unknown error>",
                )
            )
        # Keep system tags and a set union of system tags + customized tags
        self._system_tags = self._get_tags_set(self._object.get("system_tags"), None)
        self._tags = self._get_tags_set(self._system_tags, self._object.get("tags"))

    def is_in_namespace(self):
        """
        Returns whether this object is in the current namespace.

        If the current namespace is None, this will always return True.

        Returns
        -------
        bool
            Whether or not the object is in the current namespace
        """
        if self._NAME == "flow":
            return any(True for _ in self)
        else:
            return current_namespace is None or current_namespace in self._tags

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

    def __getitem__(self, id):
        """
        Returns the child object named 'id'.

        Parameters
        ----------
        id : string
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
                attempt=self._attempt, _object=obj, _parent=self
            )
        else:
            raise KeyError(id)

    def __contains__(self, id):
        """
        Tests whether a child named 'id' exists.

        Parameters
        ----------
        id : string
            Name of the child object

        Returns
        -------
        bool
            True if the child exists or False otherwise
        """
        return bool(self._get_child(id))

    @property
    def tags(self):
        """
        Tags associated with this object.

        Tags can be user defined or system defined. This returns all tags associated
        with the object.

        Returns
        -------
        Set[string]
            Tags associated with the object
        """
        return self._tags

    @property
    def system_tags(self):
        """
        System tags associated with this object.

        Tags can be user defined or system defined. This returns only the
        system tags associated with the object.

        Returns
        -------
        List[string]
            System tags associated with the object
        """
        return self._system_tags

    @property
    def created_at(self):
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
    def origin_pathspec(self):
        """
        The pathspec of the object from which the current object was cloned.

        Returns:
            str
                pathspec of the origin object from which current object was cloned.
        """
        origin_pathspec = None
        if self._NAME == "run":
            latest_step = next(self.steps())
            if latest_step:
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
    def parent(self):
        """
        Returns the parent object of this object or None if none exists.

        Returns
        -------
        MetaflowObject
            The parent of this object
        """
        if self._NAME == "flow":
            return None
        # Compute parent from pathspec and cache it.
        if self._parent is None:
            pathspec = self.pathspec
            parent_pathspec = pathspec[: pathspec.rfind("/")]
            # Only artifacts and tasks have attempts right now so we get the
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
    def pathspec(self):
        """
        Returns a string representation uniquely identifying this object.

        The string is the same as the one you would pass into the constructor
        to build this object except if you are looking for a specific attempt of
        a task or a data artifact (in which case you need to add `attempt=<attempt>`
        in the constructor).

        Returns
        -------
        string
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
    def path_components(self):
        """
        List of individual components of the pathspec.

        Returns
        -------
        List[string]
            Individual components of the pathspec
        """
        if self._path_components is None:
            ids = self.pathspec.split("/")
            self._path_components = ids
        return list(self._path_components)

    def add_tag(self, tags):
        """
        Add the tags to the current object

        If any of the tags is already a system tag, it is not added as a
        user-tag but no error is returned (since it already exists as a
        system tag). On success, all tags will have been added.

        Parameters
        ----------
        tags : Iterable over string
            Tags to add
        """
        return self.replace_tag([], tags)

    def remove_tag(self, tags):
        """
        Remove the tags to the current object

        Removing a system tag will result in an error. Removing a non-existent
        user-tag is a no-op. On success, all tags will have been removed.

        Parameters
        ----------
        tags : string or Iterable over string
            Tags to remove
        """
        return self.replace_tag(tags, [])

    def replace_tag(self, remove_tags, add_tags):
        """
        Removes and adds tags; the removal is done first

        The same rules that apply to `add_tags` and `remove_tags` apply here,
        respectively to the `add_tags` and `remove_tags` arguments.

        Parameters
        ----------
        remove_tags : string or Iterable over string
            Tags to remove
        add_tags : string or Iterable over string
            Tags to add
        """
        if not isinstance(remove_tags, (tuple, list)):
            remove_tags = [remove_tags]
        if not isinstance(add_tags, (tuple, list)):
            add_tags = [add_tags]

        if not all([is_stringish(x) for x in chain(remove_tags, add_tags)]):
            print("Found tags: %s and %s" % (str(remove_tags), str(add_tags)))
            raise ValueError("Tags can only be strings")
        operations = [("remove", tag) for tag in remove_tags]
        operations.extend(("add", tag) for tag in add_tags)
        self._tag_operations(operations)


class MetaflowData(object):
    def __init__(self, artifacts):
        self._artifacts = dict((art.id, art) for art in artifacts)

    def __getattr__(self, name):
        return self._artifacts[name].data

    def __contains__(self, var):
        return var in self._artifacts

    def __str__(self):
        return "<MetaflowData: %s>" % ", ".join(self._artifacts)

    def __repr__(self):
        return str(self)


class MetaflowCode(object):
    """
    Describes the code that is occasionally stored with a run.

    A code package will contain the version of Metaflow that was used (all the files comprising
    the Metaflow library) as well as selected files from the directory containing the Python
    file of the FlowSpec.

    Attributes
    ----------
    path : string
        Location (in the datastore provider) of the code package
    info : Dict
        Dictionary of information related to this code-package
    flowspec : string
        Source code of the file containing the FlowSpec in this code package
    tarball : TarFile
        Tar ball containing all the code
    """

    def __init__(self, flow_name, code_package):
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
        info_str = self._tar.extractfile("INFO").read().decode("utf-8")
        self._info = json.loads(info_str)
        self._flowspec = self._tar.extractfile(self._info["script"]).read()

    @property
    def path(self):
        """
        Location (in the datastore provider) of the code package.

        Returns
        -------
        string
            Full path of the code package
        """
        return self._path

    @property
    def info(self):
        """
        Metadata associated with the code package.

        Returns
        -------
        Dict
            Dictionary of metadata. Keys and values are strings
        """
        return self._info

    @property
    def flowspec(self):
        """
        Source code of the Python file containing the FlowSpec.

        Returns
        -------
        string
            Content of the Python file
        """
        return self._flowspec

    @property
    def tarball(self):
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
    A single data artifact and associated metadata.

    Attributes
    ----------
    data : object
        The unpickled representation of the data contained in this artifact
    sha : string
        Encoding representing the unique identity of this artifact
    finished_at : datetime
        Alias for created_at
    """

    _NAME = "artifact"
    _PARENT_CLASS = "task"
    _CHILD_CLASS = None

    @property
    def data(self):
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
            return filecache.get_artifact(ds_type, location[6:], meta, *components)
        else:
            # Older artifacts have a location information which we can use.
            return filecache.get_artifact_by_location(
                ds_type, location, meta, *components
            )

    @property
    def size(self):
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
    def sha(self):
        """
        Unique identifier for this artifact.

        This is a unique hash of the artifact (historically SHA1 hash)

        Returns
        -------
        string
            Hash of this artifact
        """
        return self._object["sha"]

    @property
    def finished_at(self):
        """
        Creation time for this artifact.

        Alias for created_at.

        Returns
        -------
        datetime
            Creation time
        """
        return self.created_at


class Task(MetaflowObject):
    """
    A Task represents an execution of a step.

    As such, it contains all data artifacts associated with that execution as
    well as all metadata associated with the execution.

    Note that you can also get information about a specific *attempt* of a
    task. By default, the latest finished attempt is returned but you can
    explicitly get information about a specific attempt by using the
    following syntax when creating a task:
    `Task('flow/run/step/task', attempt=<attempt>)`. Note that you will not be able to
    access a specific attempt of a task through the `.tasks` method of a step
    for example (that will always return the latest attempt).

    Attributes
    ----------
    metadata : List[Metadata]
        List of all metadata associated with the task
    metadata_dict : Dict
        Dictionary where the keys are the names of the metadata and the value are the values
        associated with those names
    data : MetaflowData
        Container of all data artifacts produced by this task
    artifacts : MetaflowArtifacts
        Container of DataArtifact objects produced by this task
    successful : boolean
        True if the task successfully completed
    finished : boolean
        True if the task completed
    exception : object
        Exception raised by this task if there was one
    finished_at : datetime
        Time this task finished
    runtime_name : string
        Runtime this task was executed on
    stdout : string
        Standard output for the task execution
    stderr : string
        Standard error output for the task execution
    code : MetaflowCode
        Code package for this task (if present)
    environment_info : Dict
        Information about the execution environment (for example Conda)
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
    def metadata(self):
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
        return [
            Metadata(
                name=obj.get("field_name"),
                value=obj.get("value"),
                created_at=obj.get("ts_epoch"),
                type=obj.get("type"),
                task=self,
            )
            for obj in all_metadata
        ]

    @property
    def metadata_dict(self):
        """
        Dictionary mapping metadata names (keys) and their associated values.

        Note that unlike the metadata() method, this call will only return the latest
        metadata for a given name. For example, if a task executes multiple times (retries),
        the same metadata name will be generated multiple times (one for each execution of the
        task). The metadata() method returns all those metadata elements whereas this call will
        return the metadata associated with the latest execution of the task.

        Returns
        -------
        Dict
            Dictionary mapping metadata name with value
        """
        # use the newest version of each key, hence sorting
        return {
            m.name: m.value for m in sorted(self.metadata, key=lambda m: m.created_at)
        }

    @property
    def index(self):
        """
        Returns the index of the innermost foreach loop if this task is run inside at least
        one foreach.

        The index is what distinguishes the various tasks inside a given step.
        This call returns None if this task was not run in a foreach loop.

        Returns
        -------
        int
            Index in the innermost loop for this task
        """
        try:
            return self["_foreach_stack"].data[-1].index
        except (KeyError, IndexError):
            return None

    @property
    def data(self):
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
    def artifacts(self):
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
    def successful(self):
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
    def finished(self):
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
    def exception(self):
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
    def finished_at(self):
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
    def runtime_name(self):
        """
        Returns the name of the runtime this task executed on.


        Returns
        -------
        string
            Name of the runtime this task executed on
        """
        for t in self._tags:
            if t.startswith("runtime:"):
                return t.split(":")[1]
        return None

    @property
    def stdout(self):
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
        string
            Standard output of this task
        """
        return self._load_log("stdout")

    @property
    def stdout_size(self):
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
    def stderr(self):
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
        string
            Standard error of this task
        """
        return self._load_log("stderr")

    @property
    def stderr_size(self):
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
    def current_attempt(self):
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
    def code(self):
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
    def environment_info(self):
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
        return env.get_client_info(self.path_components[0], self.metadata_dict)

    def _load_log(self, stream):
        log_location = self.metadata_dict.get("log_location_%s" % stream)
        if log_location:
            return self._load_log_legacy(log_location, stream)
        else:
            return "".join(line + "\n" for _, line in self.loglines(stream))

    def _get_logsize(self, stream):
        log_location = self.metadata_dict.get("log_location_%s" % stream)
        if log_location:
            return self._legacy_log_size(log_location, stream)
        else:
            return self._log_size(stream)

    def loglines(self, stream, as_unicode=True):
        """
        Return an iterator over (utc_timestamp, logline) tuples.

        If as_unicode=False, logline is returned as a byte object. Otherwise,
        it is returned as a (unicode) string.
        """
        from metaflow.mflog.mflog import merge_logs

        global filecache

        ds_type = self.metadata_dict.get("ds-type")
        ds_root = self.metadata_dict.get("ds-root")
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

    def _log_size(self, stream):
        global filecache

        ds_type = self.metadata_dict.get("ds-type")
        ds_root = self.metadata_dict.get("ds-root")
        if ds_type is None or ds_root is None:
            return 0
        if filecache is None:
            filecache = FileCache()
        attempt = self.current_attempt

        return filecache.get_log_size(
            ds_type, ds_root, stream, attempt, *self.path_components
        )


class Step(MetaflowObject):
    """
    A Step represents a user-defined Step (a method annotated with the @step decorator).

    As such, it contains all Tasks associated with the step (ie: all executions of the
    Step). A linear Step will have only one associated task whereas a foreach Step will have
    multiple Tasks.

    Attributes
    ----------
    task : Task
        Returns a Task object from the step
    finished_at : datetime
        Time this step finished (time of completion of the last task)
    environment_info : Dict
        Information about the execution environment (for example Conda)
    """

    _NAME = "step"
    _PARENT_CLASS = "run"
    _CHILD_CLASS = "task"

    @property
    def task(self):
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

    def tasks(self, *tags):
        """
        Returns an iterator over all the tasks in the step.

        An optional filter is available that allows you to filter on tags.
        If tags are specified, only tasks associated with all specified tags
        are returned.

        Parameters
        ----------
        tags : string
            Tags to match

        Returns
        -------
        Iterator[Task]
            Iterator over Task objects in this step
        """
        return self._filtered_children(*tags)

    @property
    def control_task(self):
        """
        Returns a Control Task object belonging to this step.
        This is useful when the step only contains one control task.
        Returns
        -------
        Task
            A control task in the step
        """
        children = super(Step, self).__iter__()
        for t in children:
            if CONTROL_TASK_TAG in t.tags:
                return t

    def control_tasks(self, *tags):
        """
        Returns an iterator over all the control tasks in the step.
        An optional filter is available that allows you to filter on tags. The
        control tasks returned if the filter is specified will contain all the
        tags specified.
        Parameters
        ----------
        tags : string
            Tags to match
        Returns
        -------
        Iterator[Task]
            Iterator over Control Task objects in this step
        """
        children = super(Step, self).__iter__()
        filter_tags = [CONTROL_TASK_TAG]
        filter_tags.extend(tags)
        for child in children:
            if all(tag in child.tags for tag in filter_tags):
                yield child

    def __iter__(self):
        children = super(Step, self).__iter__()
        for t in children:
            yield t

    @property
    def finished_at(self):
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
    def environment_info(self):
        """
        Returns information about the environment that was used to execute this step. As an
        example, if the Conda environment is selected, this will return information about the
        dependencies that were used in the environment.

        This environment information is only available for steps that have tasks
        for which the code package has been saved.

        Returns
        -------
        Dict
            Dictionary describing the environment
        """
        # All tasks have the same environment info so just use the first one
        for t in self:
            return t.environment_info


class Run(MetaflowObject):
    """
    A Run represents an execution of a Flow

    As such, it contains all Steps associated with the flow.

    Attributes
    ----------
    data : MetaflowData
        Container of all data artifacts produced by this run
    successful : boolean
        True if the run successfully completed
    finished : boolean
        True if the run completed
    finished_at : datetime
        Time this run finished
    code : MetaflowCode
        Code package for this run (if present)
    end_task : Task
        Task for the end step (if it is present already)
    """

    _NAME = "run"
    _PARENT_CLASS = "flow"
    _CHILD_CLASS = "step"

    def _iter_filter(self, x):
        # exclude _parameters step
        return x.id[0] != "_"

    def steps(self, *tags):
        """
        Returns an iterator over all the steps in the run.

        An optional filter is available that allows you to filter on tags.
        If tags are specified, only steps associated with all specified tags
        are returned.

        Parameters
        ----------
        tags : string
            Tags to match

        Returns
        -------
        Iterator[Step]
            Iterator over Step objects in this run
        """
        return self._filtered_children(*tags)

    @property
    def code(self):
        """
        Returns the MetaflowCode object for this run, if present.

        Not all runs save their code so this call may return None in those cases.

        Returns
        -------
        MetaflowCode
            Code package for this run
        """
        if "start" in self:
            return self["start"].task.code

    @property
    def data(self):
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
        MetaflowData
            Container of all artifacts produced by this task
        """
        end = self.end_task
        if end:
            return end.data

    @property
    def successful(self):
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
    def finished(self):
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
    def finished_at(self):
        """
        Returns the datetime object of when the run finished (successfully or not).

        The completion time of a run is the same as the completion time of its 'end' step.
        If the 'end' step has not completed, returns None.

        Returns
        -------
        datetime
            Datetime of when the run finished
        """
        end = self.end_task
        if end:
            return end.finished_at

    @property
    def end_task(self):
        """
        Returns the Task corresponding to the 'end' step.

        This returns None if the end step does not yet exist.

        Returns
        -------
        Task
            The 'end' task
        """
        try:
            end_step = self["end"]
        except KeyError:
            return None

        return end_step.task


class Flow(MetaflowObject):
    """
    A Flow represents all existing flows with a certain name, in other words,
    classes derived from 'FlowSpec'

    As such, it contains all Runs (executions of a flow) related to this flow.

    Attributes
    ----------
    latest_run : Run
        Latest Run (in progress or completed, successfully or not) of this Flow
    latest_successful_run : Run
        Latest successfully completed Run of this Flow
    """

    _NAME = "flow"
    _PARENT_CLASS = None
    _CHILD_CLASS = "run"

    def __init__(self, *args, **kwargs):
        super(Flow, self).__init__(*args, **kwargs)

    @property
    def latest_run(self):
        """
        Returns the latest run (either in progress or completed) of this flow.

        Note that an in-progress run may be returned by this call. Use latest_successful_run
        to get an object representing a completed successful run.

        Returns
        -------
        Run
            Latest run of this flow
        """
        for run in self:
            return run

    @property
    def latest_successful_run(self):
        """
        Returns the latest successful run of this flow.

        Returns
        -------
        Run
            Latest successful run of this flow
        """
        for run in self:
            if run.successful:
                return run

    def runs(self, *tags):
        """
        Returns an iterator over all the runs in the flow.

        An optional filter is available that allows you to filter on tags.
        If tags are specified, only runs associated with all specified tags
        are returned.

        Parameters
        ----------
        tags : string
            Tags to match

        Returns
        -------
        Iterator[Run]
            Iterator over Run objects in this flow
        """
        return self._filtered_children(*tags)


_CLASSES["flow"] = Flow
_CLASSES["run"] = Run
_CLASSES["step"] = Step
_CLASSES["task"] = Task
_CLASSES["artifact"] = DataArtifact
