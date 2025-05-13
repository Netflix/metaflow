"""
An implementation of a null metadata provider for Spin steps.
This provider does not store any metadata and is used when
we want to ensure that there are no side effects from the
metadata provider.

"""

import time

from metaflow.metadata_provider import MetadataProvider
from typing import List


class SpinMetadataProvider(MetadataProvider):
    TYPE = "spin"

    def __init__(self, environment, flow, event_logger, monitor):
        super(SpinMetadataProvider, self).__init__(
            environment, flow, event_logger, monitor
        )
        # No provider-specific initialization needed for a null provider

    def version(self):
        """Return provider type as version."""
        return self.TYPE

    def new_run_id(self, tags=None, sys_tags=None):
        # We currently just use the timestamp to create an ID. We can be reasonably certain
        # that it is unique and this makes it possible to do without coordination or
        # reliance on POSIX locks in the filesystem.
        run_id = "%d" % (time.time() * 1e6)
        return run_id

    def register_run_id(self, run_id, tags=None, sys_tags=None):
        """No-op register_run_id. Indicates no action taken."""
        return False

    def new_task_id(self, run_id, step_name, tags=None, sys_tags=None):
        self._task_id_seq += 1
        task_id = str(self._task_id_seq)
        return task_id

    def register_task_id(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        """No-op register_task_id. Indicates no action taken."""
        return False

    def register_data_artifacts(
        self, run_id, step_name, task_id, attempt_id, artifacts
    ):
        """No-op register_data_artifacts."""
        pass

    def register_metadata(self, run_id, step_name, task_id, metadata):
        """No-op register_metadata."""
        pass

    @classmethod
    def _mutate_user_tags_for_run(
        cls, flow_id, run_id, tags_to_add=None, tags_to_remove=None
    ):
        """No-op _mutate_user_tags_for_run. Returns an empty set of tags."""
        return frozenset()

    @classmethod
    def filter_tasks_by_metadata(
        cls,
        flow_name: str,
        run_id: str,
        step_name: str,
        field_name: str,
        pattern: str,
    ) -> List[str]:
        """No-op filter_tasks_by_metadata. Returns an empty list."""
        return []

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        """
        No-op _get_object_internal. Returns an empty list,
        which MetadataProvider.get_object will interpret as 'not found'.
        """
        return []
