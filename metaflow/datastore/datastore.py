import gzip
import os
import sys
import time
from hashlib import sha1
from functools import partial

try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle

from types import MethodType, FunctionType
from ..event_logger import NullEventLogger
from ..monitor import NullMonitor
from ..parameters import Parameter
from ..exception import MetaflowException, MetaflowInternalError
from ..metadata import DataArtifact
from .. import metaflow_config

class DataException(MetaflowException):
    headline = "Data store error"

class Inputs(object):
    """
    split-and: inputs.step_a.x inputs.step_b.x
    foreach: inputs[0].x
    both: (inp.x for inp in inputs)
    """
    def __init__(self, flows):
        # TODO sort by foreach index
        self.flows = list(flows)
        for flow in self.flows:
            setattr(self, flow._current_step, flow)

    def __getitem__(self, idx):
        return self.flows[idx]

    def __iter__(self):
        return iter(self.flows)

def only_if_not_done(f):
    def method(self, *args, **kwargs):
        if self._is_done_set:
            raise MetaflowInternalError("Tried to write to datastore "\
                                        "(method %s) after it was marked "\
                                        ".done()" % f.func_name)
        return f(self, *args, **kwargs)
    return method


class TransformableObject(object):
    # Very simple wrapper class to only keep one transform
    # of an object. This is to force garbage collection
    # on the transformed object if the transformation is
    # successful
    def __init__(self, current_object):
        self._object = current_object
        self._original_type = type(self._object)

    def transform(self, transformer):
        # Transformer is a function taking one argument (the current object) and returning another
        # object which will replace the current object if transformer does not raise an
        # exception
        try:
            temp = transformer(self._object)
            self._object = temp
        except:  # noqa E722
            raise

    def current(self):
        return self._object

    def current_type(self):
        return type(self._object)

    def original_type(self):
        return self._original_type


class MetaflowDataStore(object):
    datastore_root = None

    # Datastore needs to implement the methods below
    def save_metadata(self, name, data):
        """
        Save a task-specific metadata dictionary as JSON.
        """
        raise NotImplementedError()

    def load_metadata(self, name):
        """
        Load a task-specific metadata dictionary as JSON.
        """
        raise NotImplementedError()

    def has_metadata(self, name):
        """
        Return True if this metadata file exists.
        """
        raise NotImplementedError()

    def save_data(self, sha, transformable_object):
        """
        Save a content-addressed data blob if it doesn't exist already.
        """
        raise NotImplementedError()

    def load_data(self, sha):
        """
        Load a content-addressed data blob.
        """
        raise NotImplementedError()

    def save_logs(self, logsource, stream_data):
        """
        Save log files for multiple streams, represented as
        as a list of (stream, bytes) or (stream, Path) tuples.
        """
        raise NotImplementedError()

    def load_log_legacy(self, stream, attempt_override=None):
        """
        Load old-style, pre-mflog, log file represented as a bytes object.
        """
        raise NotImplementedError()

    def load_logs(self, logsources, stream, attempt_override=None):
        """
        Given a list of logsources, return a list of (logsource, logblob)
        tuples. Returns empty contents for missing log files.
        """
        raise NotImplementedError()

    def done(self):
        """
        Write a marker indicating that datastore has finished writing to
        this path.
        """
        raise NotImplementedError()

    def is_done(self):
        """
        A flag indicating whether this datastore directory was closed
        succesfully with done().
        """
        raise NotImplementedError()

    def object_path(self, sha):
        """
        Return URL of an object identified by a sha.
        """
        raise NotImplementedError()

    @classmethod
    def get_latest_tasks(cls,
                         flow_name,
                         run_id=None,
                         steps=None,
                         pathspecs=None):
        """
        Return a list of (step, task, attempt, metadata_blob) for a subset of
        the tasks (consider eventual consistency) for which the latest attempt
        is done for the input `flow_name, run_id`.
        We filter the list based on `steps` if non-None.
        Alternatively, `pathspecs` can contain the exact list of pathspec(s)
        (run_id/step_name/task_id) that should be filtered.
        Note: When `pathspecs` is specified, we expect strict consistency and
        not eventual consistency in contrast to other modes.
        """
        raise NotImplementedError()

    @classmethod
    def get_artifacts(cls, artifacts_to_prefetch):
        """
        Return a list of (sha, obj_blob) for all the object_path(s) specified in
        `artifacts_to_prefetch`.
        """
        raise NotImplementedError()

    def artifact_path(self, artifact_name):
        """
        Return the object_path() for `artifact_name`.
        Pre-condition: `artifact_name` is in datastore.
        """
        if self.objects:
            return self.object_path(self.objects[artifact_name])
        return None

    def get_log_location(self, logsource, stream, attempt_override=None):
        """
        Returns a string indicating the location of the log of the specified type.
        """
        filename = self.get_log_location_for_attempt(logsource, stream,
                        attempt_override if attempt_override is not None 
                            else self.attempt)
        return os.path.join(self.root, filename)

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        """
        Returns a default choice for datastore_root from metaflow_config
        depending on the datastore type.
        """
        return getattr(metaflow_config,
                       'DATASTORE_SYSROOT_%s' % cls.TYPE.upper())

    @classmethod
    def decode_gzip_data(cls, filename, fileobj=None):
        """
        Returns the gzip data in file with `filename` or passed via `fileobj`.
        """
        with gzip.GzipFile(filename, fileobj=fileobj, mode='rb') as f:
            return f.read()

    @classmethod
    def make_path(cls,
                  flow_name,
                  run_id=None,
                  step_name=None,
                  task_id=None,
                  pathspec=None):
        """
        Return the path for a given flow using this datastore.
        path = cls.datastore_root/flow_name/run_id/step_name/task_id
        Callers are expected to invoke this function with a sequence of non-None
        values depending on the nested path they want.
        For example,
        If run_id is None, return path = cls.datastore_root/flow_name
        If step_name is None, return path = cls.datastore_root/flow_name/run_id
        and so on.
        If pathspec is non None,
        return path = cls.datastore_root/flow_name/pathspec
        """
        sysroot = cls.datastore_root
        if pathspec:
            return os.path.join(sysroot, flow_name, pathspec)
        elif flow_name is None:
            return sysroot
        elif run_id is None:
            return os.path.join(sysroot, flow_name)
        elif step_name is None:
            return os.path.join(sysroot, flow_name, run_id)
        elif task_id is None:
            return os.path.join(sysroot, flow_name, run_id, step_name)
        else:
            return os.path.join(sysroot, flow_name, run_id, step_name, task_id)

    @classmethod
    def filename_with_attempt_prefix(cls, name, attempt):
        """
        Return the equivalent filename for `name` depending
        whether an attempt prefix must be used, if `attempt` isn't None.
        """
        if attempt is None:
            return name
        else:
            return '%d.%s' % (attempt, name)

    @classmethod
    def get_metadata_filename_for_attempt(cls, attempt):
        """
        Return the metadata filename (.data.json) based on `attempt`.
        """
        return cls.filename_with_attempt_prefix('data.json', attempt)

    @classmethod
    def get_log_location_for_attempt(cls, logprefix, stream, attempt):
        """
        Return the log_location based on `logprefix`, `stream` and `attempt`.
        """
        fname = '%s_%s.log' % (logprefix, stream)
        return cls.filename_with_attempt_prefix(fname, attempt)

    @classmethod
    def is_metadata_filename(cls, fname):
        """
        Returns if the filename is a metadata filename (ends in .data.json).
        """
        return fname.endswith('data.json')

    @classmethod
    def get_done_filename_for_attempt(cls, attempt):
        """
        Returns the done filename (.DONE.lock) based on `attempt`.
        """
        return cls.filename_with_attempt_prefix('DONE.lock', attempt)

    @classmethod
    def is_done_filename(cls, fname):
        """
        Returns if the filename is a done filename (ends in .DONE.lock).
        """
        return fname.endswith('DONE.lock')

    @classmethod
    def get_filename_for_attempt(cls, attempt):
        """
        Returns the attempt filename (.attempt.json) based on `attempt`.
        """
        return cls.filename_with_attempt_prefix('attempt.json', attempt)

    @classmethod
    def is_attempt_filename(cls, fname):
        """
        Returns if the filename is an attempt filename (ends in .attempt.json).
        """
        return fname.endswith('attempt.json')

    @classmethod
    def parse_filename(cls, fname):
        """
        Parse the filename and returns (name, attempt).
        When using old style paths (pre-attempt id), returns
        (name=fname, attempt=None).
        This is expected to be the converse to
        filename_with_attempt_prefix() method.
        """
        if len(fname) >= 1 and fname[0] >= '0' and fname[0] <= '9':
            # new style paths = <attempt>.<name>
            attempt = int(fname[0])
            name = fname[2:]
        else:
            # old style paths.
            attempt = None
            name = fname
        return name, attempt

    def __init__(self,
                 flow_name,
                 run_id=None,
                 step_name=None,
                 task_id=None,
                 mode='r',
                 metadata=None,
                 attempt=None,
                 event_logger=None,
                 monitor=None,
                 data_obj=None,
                 artifact_cache=None,
                 allow_unsuccessful=False):
        if run_id == 'data':
            raise DataException("Run ID 'data' is reserved. "
                                "Try with a different --run-id.")
        if self.datastore_root is None:
            raise DataException("Datastore root not found. "
                                "Specify with METAFLOW_DATASTORE_SYSROOT_%s "
                                "environment variable." % self.TYPE.upper())
        # NOTE: calling __init__(mode='w') should be a cheap operation:
        # no file system accesses are allowed. It is called frequently
        # e.g. to resolve log file location.
        self.event_logger = event_logger if event_logger else NullEventLogger()
        self.monitor = monitor if monitor else NullMonitor()
        self.metadata = metadata
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self._is_done_set = False

        self._encodings = {'gzip+pickle-v2'}
        ver = sys.version_info[0] * 10 + sys.version_info[1]
        if ver >= 34:
            self._encodings.add('gzip+pickle-v4')

        self.artifact_cache = artifact_cache
        if self.artifact_cache is None:
            self.artifact_cache = {}

        self.data_root = os.path.join(self.datastore_root, flow_name, 'data')
        self.root = self.make_path(flow_name,
                                   run_id,
                                   step_name,
                                   task_id)

        self.attempt = attempt
        if mode == 'r':
            if data_obj is None:
                # what is the latest attempt ID of this data store?

                # In the case of S3, the has_metadata() below makes a
                # HEAD request to a non-existent object, which results
                # to this object becoming eventually consistent. This
                # could result to a situation that has_metadata() misses
                # the latest version although it is already existing.

                # As long as nothing opens a datastore for reading before
                # writing, this should not be a problem.

                # We have to make MAX_ATTEMPTS HEAD requests, which is
                # very unfortunate performance-wise (TODO: parallelize this).
                # On AWS Step Functions it is possible that some attempts are 
                # missing, so we have to check all possible attempt files to 
                # find the latest one. Compared to doing a LIST operation, 
                # these checks are guaranteed to be consistent as long as the 
                # task to be looked up has already finished.
                self.attempt = None # backwards-compatibility for pre-attempts.
                for i in range(0, metaflow_config.MAX_ATTEMPTS):
                    if self.has_metadata('%d.attempt' % i, with_attempt=False):
                        self.attempt = i

                # was the latest attempt completed successfully?
                if self.is_done():
                    # load the data from the latest attempt
                    data_obj = self.load_metadata('data')
                elif allow_unsuccessful and self.attempt is not None:
                    # this mode can be used to load_logs, for instance
                    data_obj = None
                else:
                    raise DataException("Data was not found or not finished at %s"\
                                        % self.root)

            if data_obj:
                self.origin = data_obj.get('origin')
                self.objects = data_obj['objects']
                self.info = data_obj.get('info', {})
        elif mode == 'd':
            # Direct access mode used by the client. We effectively don't load any
            # objects and can only access things using the load_* functions
            self.origin = None
            self.objects = None
            self.info = None
        elif mode != 'w':
            raise DataException('Unknown datastore mode: %s' % mode)

    def init_task(self):
        # this method should be called once after datastore has been opened
        # for task-related write operations
        self.save_metadata('attempt', {'time': time.time()})
        self.objects = {}
        self.info = {}


    @property
    def pathspec(self):
        return '%s/%s/%s' % (self.run_id, self.step_name, self.task_id)

    @property
    def pathspec_index(self):
        idxstr = ','.join(map(str, (f.index for f in self['_foreach_stack'])))
        return '%s/%s[%s]' % (self.run_id, self.step_name, idxstr)

    def _save_object(self, transformable_obj, var, force_v4=False):
        if force_v4:
            blobtype = 'gzip+pickle-v4'
            if blobtype not in self._encodings:
                raise DataException("Artifact *%s* requires a serialization encoding that "
                                    "requires Python 3.4 or newer." % var)
            transformable_obj.transform(lambda x: pickle.dumps(x, protocol=4))
        else:
            try:
                # to ensure compatibility between python2 and python3, we use the
                # highest protocol that works with both the versions
                transformable_obj.transform(lambda x: pickle.dumps(x, protocol=2))
                blobtype = 'gzip+pickle-v2'
            except (SystemError, OverflowError):
                # this happens when you try to serialize an oversized
                # object (2GB/4GB+)
                blobtype = 'gzip+pickle-v4'
                if blobtype not in self._encodings:
                    raise DataException("Artifact *%s* is very large (over 2GB). "
                                        "You need to use Python 3.4 or newer if "
                                        "you want to serialize large objects."
                                        % var)
                transformable_obj.transform(lambda x: pickle.dumps(x, protocol=4))
        sha = sha1(transformable_obj.current()).hexdigest()
        sz = len(transformable_obj.current())
        self.save_data(sha, transformable_obj)
        return sha, sz, blobtype

    def _load_object(self, sha):
        if sha in self.artifact_cache:
            blob = self.artifact_cache[sha]
        else:
            blob = self.load_data(sha)
        return pickle.loads(blob)

    @only_if_not_done
    def clone(self, origin):
        self.save_metadata('data', {'datastore': self.TYPE,
                                    'version': '0.1',
                                    'origin': origin.pathspec,
                                    'python_version': sys.version,
                                    'objects': origin.objects,
                                    'info': origin.info})
        self._register_data_artifacts(origin.objects, origin.info)

    @only_if_not_done
    def passdown_partial(self, origin, vars):
        # Pass-down from datastore origin all information related to vars to
        # this datastore. In other words, this adds to the current datastore all
        # the variables in vars (obviously, it does not download them or anything but
        # records information about them). This is used to propagate parameters between
        # datastores without actually loading the parameters
        for var in vars:
            sha = origin.objects.get(var)
            if sha:
                self.objects[var] = sha
                self.info[var] = origin.info[var]

    @only_if_not_done
    def persist(self, flow):

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
                    yield var, TransformableObject(val), False

        # initialize with old values...
        if flow._datastore:
            self.objects.update(flow._datastore.objects)
            self.info.update(flow._datastore.info)

        # ...overwrite with new
        for var, obj, force_v4 in serializable_attributes():
            sha, size, encoding = self._save_object(obj, var, force_v4)
            self.objects[var] = sha
            self.info[var] = {'size': size,
                              'type': str(obj.original_type()),
                              'encoding': encoding}

        self.save_metadata('data', {'datastore': self.TYPE,
                                    'version': '1.0',
                                    'attempt': self.attempt,
                                    'python_version': sys.version,
                                    'objects': self.objects,
                                    'info': self.info})

        self._register_data_artifacts(self.objects, self.info)

    def _register_data_artifacts(self, objects, info):
        # register artifacts with the metadata service
        artifacts = [DataArtifact(name=var,
                                  ds_type=self.TYPE,
                                  url=self.object_path(sha),
                                  sha=sha,
                                  type=info[var]['encoding'])
                     for var, sha in objects.items()]

        self.metadata.register_data_artifacts(self.run_id,
                                              self.step_name,
                                              self.task_id,
                                              self.attempt,
                                              artifacts)

    def get(self, var, default=None):
        if self.objects:
            return self[var] if var in self.objects else default
        return default

    # Provides a fast-path to check if a given object is None.
    def is_none(self, var):
        if not self.info:
            return True
        info = self.info.get(var)
        if info:
            obj_type = info.get('type')
            # Conservatively check if the actual object is None, in case
            # the artifact is stored using a different python version.
            if obj_type == str(type(None)):
                return True
        # Slow path since this has to get the object from S3.
        return self.get(var) is None

    def __contains__(self, var):
        if self.objects:
            return var in self.objects
        return False

    def __getitem__(self, var):
        # backwards compatibility: we might not have info for all objects
        if not self.info:
            return None
        info = self.info.get(var)
        if info:
            encoding = info.get('encoding', 'gzip+pickle-v2')
        else:
            encoding = 'gzip+pickle-v2'
        if encoding in self._encodings:
            return self._load_object(self.objects[var])
        raise DataException("Artifact *%s* requires a newer version "
                            "of Python. Try with Python 3.4 or newer." %
                            var)

    def __iter__(self):
        if self.objects:
            return iter(self.objects)
        return iter([])

    def __str__(self):
        return self.format(show_private=True, max_value_size=1000)

    def items(self):
        if self.objects:
            return self.objects.items()
        return {}

    def to_dict(self, show_private=False, max_value_size=None, include=None):
        d = {}
        for k, v in self.items():
            if include and k not in include:
                continue
            if k[0] == '_' and not show_private:
                continue
            if max_value_size is not None and\
               self.info[k]['size'] > max_value_size:
                d[k] = ArtifactTooLarge()
            else:
                d[k] = self[k]
        return d

    def format(self, **kwargs):
        def lines():
            for k, v in self.to_dict(**kwargs).items():
                yield k, '*{key}* [size: {size} type: {type}] = {value}'\
                         .format(key=k, value=v, **self.info[k])
        return '\n'.join(line for k, line in sorted(lines()))

class ArtifactTooLarge(object):
    def __str__(self):
        return '< artifact too large >'
