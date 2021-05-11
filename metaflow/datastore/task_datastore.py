import json
import pickle
import sys
import time

from io import BufferedIOBase, RawIOBase
from types import MethodType, FunctionType

from .. import metaflow_config
from ..exception import MetaflowInternalError
from ..metadata import DataArtifact, MetaDatum
from ..parameters import Parameter
from ..util import is_stringish, to_fileobj

from .exceptions import ArtifactTooLarge, DataException

def only_if_not_done(f):
    def method(self, *args, **kwargs):
        if self._is_done_set:
            raise MetaflowInternalError("Tried to write to datastore "\
                                        "(method %s) after it was marked "\
                                        ".done()" % f.func_name)
        return f(self, *args, **kwargs)
    return method

def require_mode(mode):
    def wrapper(f):
        def method(self, *args, **kwargs):
            if self._mode != mode:
                raise MetaflowInternalError(
                    "Attempting a datastore operation '%s' requiring mode '%s' "
                    "but have mode '%s'" % (f.__name__, mode, self._mode))
            return f(self, *args, **kwargs)
        return method
    return wrapper

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

    METADATA_ATTEMPT_SUFFIX = 'attempt.json'
    METADATA_DONE_SUFFIX = 'DONE.lock'
    METADATA_DATA_SUFFIX = 'data.json'

    @staticmethod
    def metadata_name_for_attempt(name, attempt):
        if attempt is None:
            return name
        return '%d.%s' % (attempt, name)

    @staticmethod
    def parse_attempt_metadata(name):
        return name.split('.', 1)

    def __init__(self,
                 flow_datastore,
                 run_id,
                 step_name,
                 task_id,
                 attempt=None,
                 data_metadata=None,
                 mode='r'):

        self._backend = flow_datastore._backend
        self.TYPE = self._backend.TYPE
        self._ca_store = flow_datastore.ca_store
        self._environment = flow_datastore.environment
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._path = self._backend.path_join(
            flow_datastore.flow_name, run_id, step_name, task_id)
        self._mode = mode
        self._attempt = attempt
        self._metadata = flow_datastore.metadata
        self._logger = flow_datastore.logger
        self._monitor = flow_datastore.monitor
        self._artifact_cache = flow_datastore.artifact_cache
        self._parent = flow_datastore

        # The GZIP encodings are for backward compatibility
        self._encodings = {'pickle-v2', 'gzip+pickle-v2'}
        ver = sys.version_info[0] * 10 + sys.version_info[1]
        if ver >= 34:
            self._encodings.add('pickle-v4')
            self._encodings.add('gzip+pickle-v4')

        self._is_done_set = False

        # If the mode is 'write', we initialize things to empty
        if self._mode == 'w':
            self._objects = {}
            self._info = {}
        elif self._mode == 'r':
            if data_metadata is not None:
                # We already loaded the data metadata so just use that
                self._objects = data_metadata.get('objects', {})
                self._info = data_metadata.get('info', {})
            else:
                # What is the latest attempt ID for this task store.
                # NOTE: We *only* access to the data if the attempt that
                # produced it is done. In particular, we do not allow access to
                # a past attempt if a new attempt has started to avoid
                # inconsistencies (depending on when the user accesses the
                # datastore, the data may change)
                self._attempt = None
                for i in range(metaflow_config.MAX_ATTEMPTS):
                    check_meta = self._metadata_name_for_attempt(
                        self.METADATA_ATTEMPT_SUFFIX, i)
                    if self.has_metadata(check_meta, add_attempt=False):
                        self._attempt = i
                # Check if the latest attempt was completed successfully
                if not self.has_metadata(self.METADATA_DONE_SUFFIX):
                    raise DataException(
                        "Data was not found or not finished at %s" % self._path)

                data_obj = self.load_metadata([self.METADATA_DATA_SUFFIX])
                data_obj = data_obj[self.METADATA_DATA_SUFFIX]
                self._objects = data_obj.get('objects', {})
                self._info = data_obj.get('info', {})
        else:
            raise DataException("Unknown datastore mode: '%s'" % self._mode)

    @property
    def pathspec(self):
        return '/'.join([self.run_id, self.step_name, self.task_id])

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
    def pathspec_index(self):
        idxstr = ','.join(map(str, (f.index for f in self['_foreach_stack'])))
        return '%s/%s[%s]' % (self._run_id, self._step_name, idxstr)

    @property
    def artifact_cache(self):
        return self._artifact_cache

    @property
    def parent_datastore(self):
        return self._parent

    @require_mode('r')
    def keys_for_artifacts(self, names):
        return [self._objects.get(name) for name in names]

    @only_if_not_done
    @require_mode('w')
    def init_task(self):
        """
        Call this to initialize the datastore with a new attempt.

        This method requires mode 'w'.
        """
        self.save_metadata(
            {self.METADATA_ATTEMPT_SUFFIX: {'time': time.time()}},
            allow_overwrite=True)

    @only_if_not_done
    @require_mode('w')
    def save_artifacts(self, artifacts, force_v4=False):
        """
        Saves Metaflow Artifacts (Python objects) to the datastore and stores
        any relevant metadata needed to retrieve them.

        Typically, objects are pickled but the datastore may perform any
        operation that it deems necessary. You should only access artifacts
        using load_artifacts

        This method requires mode 'w'.

        Parameters
        ----------
        artifacts : Dict[string -> object]
            Dictionary containing the human-readable name of the object to save
            and the object itself
        force_v4 : boolean or Dict[string -> boolean]
            Indicates whether the artifact should be pickled using the v4
            version of pickle. If a single boolean, applies to all artifacts.
            If a dictionary, applies to the object named only. Defaults to False
            if not present or not specified
        """
        artifact_names = []
        to_save = []
        originals = []
        for name, obj in artifacts.items():
            original_type = str(type(obj))
            if self._artifact_cache is not None:
                original_object = obj
            do_v4 = force_v4 and \
                force_v4 if isinstance(force_v4, bool) else \
                    force_v4.get('name', False)
            if do_v4:
                encode_type = 'pickle-v4'
                if encode_type not in self._encodings:
                    raise DataException(
                        "Artifact *%s* requires a serialization encoding that "
                        "requires Python 3.4 or newer." % name)
                obj = pickle.dumps(obj, protocol=4)
            else:
                try:
                    obj = pickle.dumps(obj, protocol=2)
                    encode_type = 'pickle-v2'
                except (SystemError, OverflowError):
                    encode_type = 'pickle-v4'
                    if encode_type not in self._encodings:
                        raise DataException(
                            "Artifact *%s* is very large (over 2GB). "
                            "You need to use Python 3.4 or newer if you want to "
                            "serialize large objects." % name)
                    obj = pickle.dumps(obj, protocol=4)
            sz = len(obj)
            self._info[name] = {
                'size': sz,
                'type': original_type,
                'encoding': encode_type
            }
            artifact_names.append(name)
            to_save.append(obj)
            if self._artifact_cache is not None:
                originals.append(original_object)
        # Use the content-addressed store to store all artifacts
        save_result = self._ca_store.save_blobs(to_save)
        for name, result in zip(artifact_names, save_result):
            self._objects[name] = result.key
        if self._artifact_cache is not None:
            for original, result in zip(originals, save_result):
                self._artifact_cache.register(result.key, original, write=True)

    @require_mode('r')
    def load_artifacts(self, names):
        """
        Mirror function to save_artifacts

        This function will retrieve the objects referenced by 'name'. Each
        object will be fetched and returned if found. Note that this function
        will return objects that may not be the same as the ones saved using
        saved_objects (taking into account possible environment changes, for
        example different conda environments) but it will return objects that
        can be used as the objects passed in to save_objects.

        This method requires mode 'r'.

        Parameters
        ----------
        names : List[string]
            List of artifacts to retrieve

        Returns
        -------
        Dict[string -> object] :
            Dictionary containing the objects retrieved.
        """
        results = {}
        if not self._info:
            raise DataException(
                "No info object available to retrieve artifacts")
        to_load = []
        sha_to_names = {}
        for name in names:
            info = self._info.get(name)
            # We use gzip+pickle-v2 as this is the oldest/most compatible.
            # This datastore will always include the proper encoding version so
            # this is just to be able to read very old artifacts
            if info:
                encode_type = info.get('encoding', 'gzip+pickle-v2')
            else:
                encode_type = 'gzip+pickle-v2'
            if encode_type not in self._encodings:
                raise DataException(
                    "Python 3.4 or later is required to load %s" % name)
            else:
                # Check if the object is in the cache
                cached_artifact = None
                if self._artifact_cache is not None:
                    cached_artifact = self._artifact_cache.load(
                        self._objects[name])
                if cached_artifact is not None:
                    results[name] = cached_artifact
                else:
                    sha_to_names[self._objects[name]] = name
                    to_load.append(self._objects[name])
        # At this point, we load what we don't have from the CAS
        # We assume that if we have one "old" style artifact, all of them are
        # like that which is an easy assumption to make since artifacts are all
        # stored by the same implementation of the datastore for a given task.
        loaded_data = self._ca_store.load_blobs(to_load)
        for sha, blob in loaded_data.items():
            obj = pickle.loads(blob)
            if self._artifact_cache:
                self._artifact_cache.register(sha, obj)
            results[sha_to_names[sha]] = obj
        return results

    @only_if_not_done
    @require_mode('w')
    def save_metadata(self, contents, allow_overwrite=False, add_attempt=True):
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
            If True, allows the overwriting of the metadata, defaults to False
        add_attempt : boolean, optional
            If True, adds the attempt identifier to the metadata. defaults to
            True
        """
        return self._save_file(
            {k: json.dumps(v).encode('utf-8') for k, v in contents.items()},
            allow_overwrite, add_attempt)

    @require_mode('r')
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
        return {k: json.loads(v) for k, v \
            in self._load_file(names, add_attempt).items()}

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
            path = self._backend.path_join(
                self._path, self._metadata_name_for_attempt(name))
        else:
            path = self._backend.path_join(self._path, name)
        return self._backend.is_file(path)

    @require_mode('r')
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

    @require_mode('r')
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
            obj_type = info.get('type')
            # Conservatively check if the actual object is None,
            # in case the artifact is stored using a different python version.
            # Note that if an object is None and stored in Py2 and accessed in
            # Py3, this test will fail and we will fallback to the slow path. This
            # is intended (being conservative)
            if obj_type == str(type(None)):
                return True
        # Slow path since this has to get the object from the datastore
        return self.get(name) is None

    @only_if_not_done
    @require_mode('w')
    def done(self):
        """
        Mark this task-datastore as 'done' for the current attempt

        Will throw an exception if mode != 'w'
        """
        self.save_metadata({
            self.METADATA_DATA_SUFFIX:
                {'datastore': self.TYPE,
                 'version': '1.0',
                 'attempt': self._attempt,
                 'python_version': sys.version,
                 'objects': self._objects,
                 'info': self._info},
            self.METADATA_DONE_SUFFIX: ""
        })

        if self._metadata:
            self._metadata.register_metadata(
                self._run_id, self._step_name, self._task_id,
                [MetaDatum(field='attempt-done', value=str(self._attempt),
                           type='attempt-done', tags=[])])
            artifacts = [DataArtifact(
                name=var,
                ds_type=self.TYPE,
                ds_root=self._backend.datastore_root,
                url=None,
                sha=sha,
                type=self._info[var]['encoding'])
                         for var, sha in self._objects.items()]

            self._metadata.register_data_artifacts(
                self.run_id, self.step_name, self.task_id, self._attempt,
                artifacts)

        self._is_done_set = True

    @only_if_not_done
    @require_mode('w')
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
    @require_mode('w')
    def passdown_partial(self, origin, variables):
        # Pass-down from datastore origin all information related to vars to
        # this datastore. In other words, this adds to the current datastore all
        # the variables in vars (obviously, it does not download them or
        # anything but records information about them). This is used to
        # propagate parameters between datastores without actually loading the
        # parameters
        for var in variables:
            sha = origin._objects.get(var)
            if sha:
                self._objects[var] = sha
                self._info[var] = origin._info[var]

    @only_if_not_done
    @require_mode('w')
    def persist(self, flow):
        """
        Persist any new artifacts that were produced when running flow

        Parameters
        ----------
        flow : FlowSpec
            Flow to persist
        """
        def serializable_attributes():
            for var in dir(flow):
                if var.startswith('__') or var in flow._EPHEMERAL:
                    continue
                # Skip over properties of the class (Parameters)
                if hasattr(flow.__class__, var) and \
                        isinstance(getattr(flow.__class__, var), property):
                    continue
                val = getattr(flow, var)
                if not (isinstance(val, MethodType) or
                        isinstance(val, FunctionType) or
                        isinstance(val, Parameter)):
                    yield var, val, False

        # initialize with old values...
        if flow._datastore:
            self._objects.update(flow._datastore._objects)
            self._info.update(flow._datastore._info)

        # ...overwrite with new
        to_save = {}
        force_v4_dict = {}
        for var, obj, force_v4 in serializable_attributes():
            to_save[var] = obj
            if force_v4:
                force_v4_dict[var] = True
        self.save_artifacts(to_save, force_v4_dict)

    def save_logs(self, logtype_to_bytebuffer):
        to_store_dict = {
            '%s.log' % k: v for k, v in logtype_to_bytebuffer.items()}
        self._save_file(to_store_dict)
        return {
            k.rsplit('.', 1)[0]: self._backend.full_uri(self._backend.path_join(
                self._path, self._metadata_name_for_attempt(k)))
            for k in to_store_dict.keys()}

    def load_log(self, logtype, attempt_override=None):
        name = self._metadata_name_for_attempt(
            '%s.log' % logtype, attempt_override)
        return self._load_file([name], add_attempt=False)[name]

    def items(self):
        if self._objects:
            return self._objects.items()
        return {}

    def to_dict(self, show_private=False, max_value_size=None, include=None):
        d = {}
        for k, _ in self.items():
            if include and k not in include:
                continue
            if k[0] == '_' and not show_private:
                continue
            if max_value_size is not None and\
               self._info[k]['size'] > max_value_size:
                d[k] = ArtifactTooLarge()
            else:
                d[k] = self[k]
        return d

    def format(self, **kwargs):
        def lines():
            for k, v in self.to_dict(**kwargs).items():
                yield k, '*{key}* [size: {size} type: {type}] = {value}'\
                    .format(key=k, value=v, **self._info[k])
        return '\n'.join(line for k, line in sorted(lines()))

    def __contains__(self, name):
        if self._objects:
            return name in self._objects
        return False

    def __getitem__(self, name):
        return self.load_artifacts([name])[name]

    def __iter__(self):
        if self._objects:
            return iter(self._objects)
        return iter([])

    def __str__(self):
        return self.format(show_private=True, max_value_size=1000)

    def _metadata_name_for_attempt(self, name, attempt_override=None):
        return self.metadata_name_for_attempt(
            name, self._attempt if attempt_override is None else
            attempt_override)

    def _save_file(self, contents, allow_overwrite=False, add_attempt=True):
        """
        Saves files in the directory for this TaskDataStore. This can be
        metadata, a log file or any other data that doesn't need to (or
        shouldn't) be stored in the Content Addressed Store.

        Parameters
        ----------
        contents : Dict: stringish or RawIOBase or BufferedIOBase
            Dictionary of file to store
        allow_overwrite : boolean, optional
            If True, allows the overwriting of the metadata, defaults to False
        add_attempt : boolean, optional
            If True, adds the attempt identifier to the metadata,
            defaults to True
        """
        to_store = {}
        for name, value in contents.items():
            if add_attempt:
                path = self._backend.path_join(
                    self._path, self._metadata_name_for_attempt(name))
            else:
                path = self._backend.path_join(self._path, name)
            if isinstance(value, (RawIOBase, BufferedIOBase)) and \
                    value.readable():
                to_store[path] = value
            elif is_stringish(value):
                value = to_fileobj(value)
                to_store[path] = value
            else:
                raise DataException("Metadata '%s' has an invalid type: %s" %
                                    (name, type(value)))
        path_results = self._backend.save_bytes(
            to_store, overwrite=allow_overwrite)

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
                path = self._backend.path_join(
                    self._path, self._metadata_name_for_attempt(name))
            else:
                path = self._backend.path_join(self._path, name)
            to_load.append(path)
        load_results = self._backend.load_bytes(to_load)
        results = {}
        for path, result in load_results.items():
            if add_attempt:
                _, name = self.parse_attempt_metadata(
                    self._backend.basename(path))
            else:
                name = self._backend.basename(path)
            if result is None:
                results[name] = None
            else:
                with result[0] as r:
                    results[name] = r.read()
        return results
