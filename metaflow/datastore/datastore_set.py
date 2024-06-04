import json

from io import BytesIO

from .exceptions import DataException
from .content_addressed_store import BlobCache

"""
TaskDataStoreSet allows you to prefetch multiple (read) datastores into a
cache and lets you access them. As a performance optimization it also lets you 
prefetch select data artifacts leveraging a shared cache.
"""


class TaskDataStoreSet(object):
    def __init__(
        self,
        flow_datastore,
        run_id,
        steps=None,
        pathspecs=None,
        prefetch_data_artifacts=None,
        allow_not_done=False,
    ):
        self.task_datastores = flow_datastore.get_task_datastores(
            run_id, steps=steps, pathspecs=pathspecs, allow_not_done=allow_not_done
        )

        if prefetch_data_artifacts:
            # produce a set of SHA keys to prefetch based on artifact names
            prefetch = set()
            for ds in self.task_datastores:
                prefetch.update(ds.keys_for_artifacts(prefetch_data_artifacts))
            # ignore missing keys
            prefetch.discard(None)

            # prefetch artifacts and share them with all datastores
            # in this DatastoreSet
            preloaded = dict(flow_datastore.ca_store.load_blobs(prefetch))
            cache = ImmutableBlobCache(preloaded)
            flow_datastore.ca_store.set_blob_cache(cache)

        self.pathspec_index_cache = {}
        self.pathspec_cache = {}
        if not allow_not_done:
            for ds in self.task_datastores:
                self.pathspec_index_cache[ds.pathspec_index] = ds
                self.pathspec_cache[ds.pathspec] = ds

    def get_with_pathspec(self, pathspec):
        return self.pathspec_cache.get(pathspec, None)

    def get_with_pathspec_index(self, pathspec_index):
        return self.pathspec_index_cache.get(pathspec_index, None)

    def __iter__(self):
        for v in self.task_datastores:
            yield v


"""
This class ensures that blobs that correspond to artifacts that
are common to all datastores in this set are only loaded once 
"""


class ImmutableBlobCache(BlobCache):
    def __init__(self, preloaded):
        self._preloaded = preloaded

    def load_key(self, key):
        return self._preloaded.get(key)

    def store_key(self, key, blob):
        # we cache only preloaded keys, so no need to store anything
        pass
