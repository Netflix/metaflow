import json

from io import BytesIO

from .common import DataException

"""
TaskDataStoreSet allows you to prefetch multiple (read) datastores into a
cache and lets you access them.
As a performance optimization it also lets you prefetch select data artifacts
leveraging a shared cache.
"""
class TaskDataStoreSet(object):
    def __init__(self,
                 flow_datastore,
                 run_id,
                 steps=None,
                 pathspecs=None,
                 prefetch_data_artifacts=None):
        if flow_datastore.blob_cache is not None:
            raise DataException("FlowDataStore already has a blob cache")
        if flow_datastore.artifact_cache is not None:
            raise DataException("FlowDataStore already has an artifact cache")
        flow_datastore.blob_cache = KeepInitialCache()
        flow_datastore.artifact_cache = KeepSpecificKeysCache()

        task_datastores = flow_datastore.get_latest_task_datastores(
            run_id, steps=steps, pathspecs=pathspecs)

        if prefetch_data_artifacts:
            all_keys = []
            for ds in task_datastores:
                all_keys.extend(ds.keys_for_artifacts(prefetch_data_artifacts))
            keys_to_prefetch = set(all_keys)
            # Remove any non-existent artifacts
            try:
                keys_to_prefetch.remove(None)
            except KeyError:
                pass
            flow_datastore.artifact_cache.keys_to_cache = keys_to_prefetch
            # Load all keys; this will update blob_cache
            flow_datastore.load_data(keys_to_prefetch)
            # Stop caching for any future blob
            flow_datastore.blob_cache.flip()

        self.pathspec_index_cache = {}
        self.pathspec_cache = {}
        for ds in task_datastores:
            self.pathspec_index_cache[ds.pathspec_index] = ds
            self.pathspec_cache[ds.pathspec] = ds

    def get_with_pathspec(self, pathspec):
        return self.pathspec_cache.get(pathspec, None)

    def get_with_pathspec_index(self, pathspec_index):
        return self.pathspec_index_cache.get(pathspec_index, None)

    def __iter__(self):
        for v in self.pathspec_cache.values():
            yield v

class DictCache(object):
    def __init__(self):
        self._cache = {}

    def register(self, key, value, write=False):
        self._cache[key] = value

    def load(self, key):
        return self._cache.get(key, None)

class KeepInitialCache(DictCache):
    def __init__(self):
        self._is_caching = True
        super(KeepInitialCache, self).__init__()

    def register(self, key, value, write=False):
        if self._is_caching:
            super(KeepInitialCache, self).register(key, value, write)

    def flip(self, is_on=False):
        self._is_caching = is_on

class KeepSpecificKeysCache(DictCache):
    def __init__(self, keys=None):
        self._keys = keys
        if self._keys is None:
            self._keys = set()
        super(KeepSpecificKeysCache, self).__init__()

    def register(self, key, value, write=False):
        if key in self._keys:
            super(KeepSpecificKeysCache, self).register(key, value, write)

    @property
    def keys_to_cache(self):
        return self._keys

    @keys_to_cache.setter
    def keys_to_cache(self, s):
        if s is None:
            s = set()
        self._keys = s
