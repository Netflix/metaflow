import json

from metaflow.util import to_unicode

"""
MetaflowDatastoreSet allows you to prefetch multiple (read) datastores into a
cache and lets you access them.
As a performance optimization it also lets you prefetch select data artifacts
leveraging a shared cache.
"""
class MetaflowDatastoreSet(object):
    def __init__(self,
                 ds_class,
                 flow_name,
                 run_id,
                 steps=None,
                 pathspecs=None,
                 metadata=None,
                 event_logger=None,
                 monitor=None,
                 prefetch_data_artifacts=None):
        data_blobs = ds_class.get_latest_tasks(flow_name,
                                               run_id,
                                               steps=steps,
                                               pathspecs=pathspecs)
        artifact_cache = {}
        datastores = [ds_class(flow_name,
                               run_id=run_id,
                               step_name=step_name,
                               task_id=task_id,
                               metadata=metadata,
                               attempt=attempt,
                               event_logger=event_logger,
                               monitor=monitor,
                               data_obj=json.loads(to_unicode(data_blob)),
                               artifact_cache=artifact_cache)
                      for step_name, task_id, attempt, data_blob in data_blobs]
        if prefetch_data_artifacts:
            artifacts_to_prefetch = set(
                [ds.artifact_path(artifact_name)
                 for ds in datastores
                 for artifact_name in prefetch_data_artifacts
                 if artifact_name in ds])

            # Update (and not re-assign) the artifact_cache since each datastore
            # created above has a reference to this object.
            artifact_cache.update(ds_class.get_artifacts(artifacts_to_prefetch))
        self.pathspec_index_cache = {}
        self.pathspec_cache = {}
        for ds in datastores:
            self.pathspec_index_cache[ds.pathspec_index] = ds
            self.pathspec_cache[ds.pathspec] = ds

    def get_with_pathspec(self, pathspec):
        return self.pathspec_cache.get(pathspec, None)

    def get_with_pathspec_index(self, pathspec_index):
        return self.pathspec_index_cache.get(pathspec_index, None)

    def __iter__(self):
        for v in self.pathspec_cache.values():
            yield v
