from __future__ import print_function
import os
import time
from tempfile import NamedTemporaryFile
from hashlib import sha1

from metaflow.datastore import DATASTORES, FlowDataStore
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import CLIENT_CACHE_PATH, CLIENT_CACHE_MAX_SIZE

NEW_FILE_QUARANTINE = 10


class FileCacheException(MetaflowException):
    headline = 'File cache error'

class FileCache(object):
    def __init__(self, metadata=None, event_logger=None, monitor=None,
                 cache_dir=None, max_size=None):
        self._cache_dir = cache_dir
        self._max_size = max_size
        if self._cache_dir is None:
            self._cache_dir = CLIENT_CACHE_PATH
        if self._max_size is None:
            self._max_size = int(CLIENT_CACHE_MAX_SIZE)
        self._total = 0

        self._metadata = metadata
        self._event_logger = event_logger
        self._monitor = monitor

        self._objects = None
        # We will have a separate blob_cache per flow and datastore type
        # This is to respect the fact that the keys across datastore types
        # and/or flows may not be unique (across flows is less likely but we are
        # keeping in line with the fact that datsatores are flow based)
        self._blob_caches = {}

        # We also keep a cache for FlowDataStore objects that we created because
        # some of them may have persistent connections in their backends;
        # this is purely a performance optimization. We do *not* keep track of
        # task datastores to refresh them as needed. Caching FlowDataStore has
        # no adverse affect in terms of having to refresh the cache.
        self._store_caches = {}

    @property
    def cache_dir(self):
        return self._cache_dir

    def get_logs_stream(
            self, ds_type, ds_root, stream, attempt, flow_name, run_id,
            step_name, task_id):
        from metaflow.mflog import LOG_SOURCES

        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id,
            data_metadata={'objects': {}, 'info': {}})
        return task_ds.load_logs(LOG_SOURCES, stream, attempt_override=attempt)

    def get_log_legacy(
            self, ds_type, location, logtype, attempt, flow_name, run_id,
            step_name, task_id):

        ds_cls = self._get_backend_datastore_cls(ds_type)
        ds_root = ds_cls.path_join(*ds_cls.path_split(location)[:-5])
        cache_id = self._flow_ds_type(ds_type, ds_root, flow_name)

        token = '%s.cached' % sha1(os.path.join(
            run_id, step_name, task_id, '%s_log' % logtype).\
            encode('utf-8')).hexdigest()
        path = os.path.join(self._cache_dir, cache_id, token[:2], token)

        cached_log = self.read_file(path)
        if cached_log is not None:
            return cached_log

        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id,
            data_metadata={'objects': {}, 'info': {}})

        log = task_ds.load_log_legacy(logtype, attempt_override=attempt)
        # Store this in the file cache as well
        self.create_file(path, log)
        return log

    def get_data(self, ds_type, flow_name, location, key):
        ds_cls = self._get_backend_datastore_cls(ds_type)
        ds_root = ds_cls.get_datastore_root_from_location(location, flow_name)
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        return ds.load_data([key], force_raw=True)[0]

    def get_artifact_by_location(
            self, ds_type, location, data_metadata, flow_name, run_id,
            step_name, task_id, name):
        ds_cls = self._get_backend_datastore_cls(ds_type)
        ds_root = ds_cls.get_datastore_root_from_location(location, flow_name)
        return self.get_artifact(
            ds_type, ds_root, data_metadata, flow_name, run_id, step_name,
            task_id, name)

    def get_artifact(
            self, ds_type, ds_root, data_metadata, flow_name, run_id, step_name,
            task_id, name):
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        # We get the task datastore for this task
        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id, data_metadata=data_metadata)
        # This will reuse the blob cache if needed. We do not have an
        # artifact cache so the unpickling happens every time here.
        return task_ds[name]

    def create_file(self, path, value):
        if self._objects is None:
            # Index objects lazily (when we first need to write to it).
            # This can be an expensive operation
            self._index_objects()
        dirname = os.path.dirname(path)
        try:
            FileCache._makedirs(dirname)
        except:  # noqa E722
            raise FileCacheException(
                'Could not create directory: %s' % dirname)
        tmpfile = NamedTemporaryFile(
            dir=dirname, prefix='dlobj', delete=False)
        # Now write out the file
        try:
            tmpfile.write(value)
            tmpfile.flush()
            os.rename(tmpfile.name, path)
        except:  # noqa E722
            os.unlink(tmpfile.name)
            raise
        size = os.path.getsize(path)
        self._total += size
        self._objects.append((int(time.time()), size, path))
        self._garbage_collect()

    def read_file(self, path):
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    return f.read()
            except IOError:
                # It may have been concurrently garbage collected by another
                # process
                pass
        return None

    def _index_objects(self):
        objects = []
        if os.path.exists(self._cache_dir):
            for flow_ds_type in os.listdir(self._cache_dir):
                root = os.path.join(self._cache_dir, flow_ds_type)
                if not os.path.isdir(root):
                    continue
                for subdir in os.listdir(root):
                    root = os.path.join(self._cache_dir, flow_ds_type, subdir)
                    if not os.path.isdir(root):
                        continue
                    for obj in os.listdir(root):
                        sha, ext = os.path.splitext(obj)
                        if ext in ['cached', 'blob']:
                            path = os.path.join(root, obj)
                            objects.insert(0, (os.path.getctime(path),
                                               os.path.getsize(path),
                                               path))

        self._total = sum(size for _, size, _ in objects)
        self._objects = sorted(objects, reverse=False)

    @staticmethod
    def _flow_ds_type(ds_type, ds_root, flow_name):
        return '.'.join([ds_type, ds_root, flow_name])

    def _garbage_collect(self):
        now = time.time()
        while self._objects and self._total > self._max_size * 1024**2:
            if now - self._objects[0][0] < NEW_FILE_QUARANTINE:
                break
            ctime, size, path = self._objects.pop(0)
            self._total -= size
            try:
                os.remove(path)
            except OSError:
                # maybe another client had already GC'ed the file away
                pass

    @staticmethod
    def _makedirs(path):
        # this is for python2 compatibility.
        # Python3 has os.makedirs(exist_ok=True).
        try:
            os.makedirs(path)
        except OSError as x:
            if x.errno == 17:
                return
            else:
                raise

    @staticmethod
    def _get_backend_datastore_cls(ds_type):
        storage_impl = DATASTORES.get(ds_type, None)
        if storage_impl is None:
            raise FileCacheException('Datastore %s was not found' % ds_type)
        return storage_impl

    def _get_flow_datastore(self, ds_type, ds_root, flow_name):
        cache_id = self._flow_ds_type(ds_type, ds_root, flow_name)

        cached_flow_datastore = self._store_caches.get(cache_id)
        if cached_flow_datastore:
            return cached_flow_datastore

        storage_impl = self._get_backend_datastore_cls(ds_type)

        blob_cache = self._blob_caches.setdefault(
            cache_id, BlobCache(self, cache_id))

        cached_flow_datastore = FlowDataStore(
            flow_name,
            None, # TODO: Add environment here
            self._metadata,
            self._event_logger,
            self._monitor,
            blob_cache=blob_cache,
            backend_class=storage_impl,
            ds_root=ds_root)
        self._store_caches[cache_id] = cached_flow_datastore
        return cached_flow_datastore

class BlobCache(object):
    def __init__(self, filecache, cache_id):
        self._filecache = filecache
        self._cache_id = cache_id

    def load(self, key):
        key_dir = key[:2]
        path = os.path.join(
            self._filecache.cache_dir, self._cache_id, key_dir, '%s.blob' % key)
        return self._filecache.read_file(path)

    def register(self, key, value, write=False):
        # Here, we will store the value in a file (basically backing
        # our cache with files).

        # First derive the path we store it at
        key_dir = key[:2]
        path = os.path.join(
            self._filecache.cache_dir, self._cache_id, key_dir, '%s.blob' % key)
        self._filecache.create_file(path, value)
