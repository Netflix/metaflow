from __future__ import print_function
from collections import OrderedDict
import json
import os
import sys
import time
from tempfile import NamedTemporaryFile
from hashlib import sha1

from urllib.parse import urlparse

from metaflow.datastore import FlowDataStore
from metaflow.datastore.content_addressed_store import BlobCache
from metaflow.datastore.flow_datastore import MetadataCache
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    CLIENT_CACHE_PATH,
    CLIENT_CACHE_MAX_SIZE,
    CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT,
)
from metaflow.metaflow_profile import from_start

from metaflow.plugins import DATASTORES

NEW_FILE_QUARANTINE = 10

if sys.version_info[0] >= 3 and sys.version_info[1] >= 2:

    def od_move_to_end(od, key):
        od.move_to_end(key)

else:
    # Not very efficient but works and most people are on 3.2+
    def od_move_to_end(od, key):
        v = od.get(key)
        del od[key]
        od[key] = v


class FileCacheException(MetaflowException):
    headline = "File cache error"


class FileCache(object):
    def __init__(self, cache_dir=None, max_size=None):
        self._cache_dir = cache_dir
        self._max_size = max_size
        if self._cache_dir is None:
            self._cache_dir = CLIENT_CACHE_PATH
        if self._max_size is None:
            self._max_size = int(CLIENT_CACHE_MAX_SIZE)
        self._total = 0

        self._objects = None
        # We have a separate blob_cache per flow and datastore type.
        self._blob_caches = {}

        # We also keep a cache for FlowDataStore objects because some of them
        # may have long-lived persistent connections; this is purely a
        # performance optimization. Uses OrderedDict to implement a kind of LRU
        # cache and keep only a certain number of these caches around.
        self._store_caches = OrderedDict()

        # We also keep a cache of data_metadata for TaskDatastore. This is used
        # when querying for sizes of artifacts. Once we have queried for the size
        # of one artifact in a TaskDatastore, caching this means that any
        # queries on that same TaskDatastore will be quick (since we already
        # have all the metadata). We keep track of this in a file so it persists
        # across processes.

    @property
    def cache_dir(self):
        return self._cache_dir

    def get_logs_stream(
        self, ds_type, ds_root, stream, attempt, flow_name, run_id, step_name, task_id
    ):
        from metaflow.mflog import LOG_SOURCES

        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id, data_metadata={"objects": {}, "info": {}}
        )
        return task_ds.load_logs(LOG_SOURCES, stream, attempt_override=attempt)

    def get_log_legacy(
        self, ds_type, location, logtype, attempt, flow_name, run_id, step_name, task_id
    ):
        ds_cls = self._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.path_join(*ds_cls.path_split(location)[:-5])
        cache_id = self.flow_ds_id(ds_type, ds_root, flow_name)

        token = (
            "%s.cached"
            % sha1(
                os.path.join(run_id, step_name, task_id, "%s_log" % logtype).encode(
                    "utf-8"
                )
            ).hexdigest()
        )
        path = os.path.join(self._cache_dir, cache_id, token[:2], token)

        cached_log = self.read_file(path)
        if cached_log is not None:
            return cached_log

        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id, data_metadata={"objects": {}, "info": {}}
        )

        log = task_ds.load_log_legacy(logtype, attempt_override=attempt)
        # Store this in the file cache as well
        self.create_file(path, log)
        return log

    def get_legacy_log_size(
        self, ds_type, location, logtype, attempt, flow_name, run_id, step_name, task_id
    ):
        ds_cls = self._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.path_join(*ds_cls.path_split(location)[:-5])
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id,
            step_name,
            task_id,
            attempt=attempt,
            data_metadata={"objects": {}, "info": {}},
        )

        return task_ds.get_legacy_log_size(logtype)

    def get_log_size(
        self, ds_type, ds_root, logtype, attempt, flow_name, run_id, step_name, task_id
    ):
        from metaflow.mflog import LOG_SOURCES

        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        task_ds = ds.get_task_datastore(
            run_id,
            step_name,
            task_id,
            attempt=attempt,
            data_metadata={"objects": {}, "info": {}},
        )

        return task_ds.get_log_size(LOG_SOURCES, logtype)

    def get_data(self, ds_type, flow_name, location, key):
        ds_cls = self._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.get_datastore_root_from_location(location, flow_name)
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        return next(ds.load_data([key], force_raw=True))

    def get_artifact_size_by_location(
        self, ds_type, location, attempt, flow_name, run_id, step_name, task_id, name
    ):
        """Gets the size of the artifact content (in bytes) for the name at the location"""
        ds_cls = self._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.get_datastore_root_from_location(location, flow_name)

        return self.get_artifact_size(
            ds_type, ds_root, attempt, flow_name, run_id, step_name, task_id, name
        )

    def get_artifact_size(
        self, ds_type, ds_root, attempt, flow_name, run_id, step_name, task_id, name
    ):
        """Gets the size of the artifact content (in bytes) for the name"""
        task_ds = self._get_task_datastore(
            ds_type, ds_root, flow_name, run_id, step_name, task_id, attempt
        )

        _, size = next(task_ds.get_artifact_sizes([name]))
        return size

    def get_artifact_by_location(
        self,
        ds_type,
        location,
        data_metadata,
        flow_name,
        run_id,
        step_name,
        task_id,
        name,
    ):
        ds_cls = self._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.get_datastore_root_from_location(location, flow_name)
        return self.get_artifact(
            ds_type, ds_root, data_metadata, flow_name, run_id, step_name, task_id, name
        )

    def get_artifact(
        self,
        ds_type,
        ds_root,
        data_metadata,
        flow_name,
        run_id,
        step_name,
        task_id,
        name,
    ):
        _, obj = next(
            self.get_artifacts(
                ds_type,
                ds_root,
                data_metadata,
                flow_name,
                run_id,
                step_name,
                task_id,
                [name],
            )
        )
        return obj

    def get_all_artifacts(
        self, ds_type, ds_root, data_metadata, flow_name, run_id, step_name, task_id
    ):
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        # We get the task datastore for this task
        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id, data_metadata=data_metadata
        )
        # This will reuse the blob cache if needed. We do not have an
        # artifact cache so the unpickling happens every time here.
        return task_ds.load_artifacts([n for n, _ in task_ds.items()])

    def get_artifacts(
        self,
        ds_type,
        ds_root,
        data_metadata,
        flow_name,
        run_id,
        step_name,
        task_id,
        names,
    ):
        ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        # We get the task datastore for this task
        task_ds = ds.get_task_datastore(
            run_id, step_name, task_id, data_metadata=data_metadata
        )
        # note that load_artifacts uses flow_datastore.castore which goes
        # through one of the self._blob_cache
        return task_ds.load_artifacts(names)

    def create_file(self, path, value):
        if self._objects is None:
            # Index objects lazily (when we first need to write to it).
            # This can be an expensive operation
            self._index_objects()
        dirname = os.path.dirname(path)
        try:
            FileCache._makedirs(dirname)
        except:  # noqa E722
            raise FileCacheException("Could not create directory: %s" % dirname)
        tmpfile = NamedTemporaryFile(dir=dirname, prefix="dlobj", delete=False)
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
                with open(path, "rb") as f:
                    return f.read()
            except IOError:
                # It may have been concurrently garbage collected by another
                # process
                pass
        return None

    def _index_objects(self):
        objects = []
        if os.path.exists(self._cache_dir):
            for flow_ds_id in os.listdir(self._cache_dir):
                root = os.path.join(self._cache_dir, flow_ds_id)
                if not os.path.isdir(root):
                    continue
                for subdir in os.listdir(root):
                    root = os.path.join(self._cache_dir, flow_ds_id, subdir)
                    if not os.path.isdir(root):
                        continue
                    for obj in os.listdir(root):
                        sha, ext = os.path.splitext(obj)
                        if ext in ["cached", "blob"]:
                            path = os.path.join(root, obj)
                            objects.insert(
                                0, (os.path.getctime(path), os.path.getsize(path), path)
                            )

        self._total = sum(size for _, size, _ in objects)
        self._objects = sorted(objects, reverse=False)

    @staticmethod
    def flow_ds_id(ds_type, ds_root, flow_name):
        p = urlparse(ds_root)
        sanitized_root = (p.netloc + p.path).replace("/", "_")
        return ".".join([ds_type, sanitized_root, flow_name])

    @staticmethod
    def task_ds_id(ds_type, ds_root, flow_name, run_id, step_name, task_id, attempt):
        p = urlparse(ds_root)
        sanitized_root = (p.netloc + p.path).replace("/", "_")
        return ".".join(
            [
                ds_type,
                sanitized_root,
                flow_name,
                run_id,
                step_name,
                task_id,
                str(attempt),
            ]
        )

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
    def _get_datastore_storage_impl(ds_type):
        storage_impl = [d for d in DATASTORES if d.TYPE == ds_type]
        if len(storage_impl) == 0:
            raise FileCacheException("Datastore %s was not found" % ds_type)
        return storage_impl[0]

    def _get_flow_datastore(self, ds_type, ds_root, flow_name):
        cache_id = self.flow_ds_id(ds_type, ds_root, flow_name)
        cached_flow_datastore = self._store_caches.get(cache_id)

        if cached_flow_datastore:
            od_move_to_end(self._store_caches, cache_id)
            return cached_flow_datastore
        else:
            storage_impl = self._get_datastore_storage_impl(ds_type)
            cached_flow_datastore = FlowDataStore(
                flow_name=flow_name,
                environment=None,  # TODO: Add environment here
                storage_impl=storage_impl,
                ds_root=ds_root,
            )
            blob_cache = self._blob_caches.setdefault(
                cache_id,
                (
                    FileBlobCache(self, cache_id),
                    TaskMetadataCache(self, ds_type, ds_root, flow_name),
                ),
            )
            cached_flow_datastore.ca_store.set_blob_cache(blob_cache[0])
            cached_flow_datastore.set_metadata_cache(blob_cache[1])
            self._store_caches[cache_id] = cached_flow_datastore
            if len(self._store_caches) > CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT:
                cache_id_to_remove, _ = self._store_caches.popitem(last=False)
                del self._blob_caches[cache_id_to_remove]
            return cached_flow_datastore

    def _get_task_datastore(
        self, ds_type, ds_root, flow_name, run_id, step_name, task_id, attempt
    ):
        flow_ds = self._get_flow_datastore(ds_type, ds_root, flow_name)

        return flow_ds.get_task_datastore(run_id, step_name, task_id, attempt=attempt)


class TaskMetadataCache(MetadataCache):
    def __init__(self, filecache, ds_type, ds_root, flow_name):
        self._filecache = filecache
        self._ds_type = ds_type
        self._ds_root = ds_root
        self._flow_name = flow_name

    def _path(self, run_id, step_name, task_id, attempt):
        if attempt is None:
            raise MetaflowException(
                "Attempt number must be specified to use task metadata cache. Raise an issue "
                "on Metaflow GitHub if you see this message.",
            )
        cache_id = self._filecache.task_ds_id(
            self._ds_type,
            self._ds_root,
            self._flow_name,
            run_id,
            step_name,
            task_id,
            attempt,
        )
        token = (
            "%s.cached"
            % sha1(
                os.path.join(
                    run_id, step_name, task_id, str(attempt), "metadata"
                ).encode("utf-8")
            ).hexdigest()
        )
        return os.path.join(self._filecache.cache_dir, cache_id, token[:2], token)

    def load_metadata(self, run_id, step_name, task_id, attempt):
        d = self._filecache.read_file(self._path(run_id, step_name, task_id, attempt))
        if d:
            return json.loads(d)

    def store_metadata(self, run_id, step_name, task_id, attempt, metadata_dict):
        self._filecache.create_file(
            self._path(run_id, step_name, task_id, attempt),
            json.dumps(metadata_dict).encode("utf-8"),
        )


class FileBlobCache(BlobCache):
    def __init__(self, filecache, cache_id):
        self._filecache = filecache
        self._cache_id = cache_id

    def _path(self, key):
        key_dir = key[:2]
        return os.path.join(
            self._filecache.cache_dir, self._cache_id, key_dir, "%s.blob" % key
        )

    def load_key(self, key):
        return self._filecache.read_file(self._path(key))

    def store_key(self, key, blob):
        self._filecache.create_file(self._path(key), blob)
