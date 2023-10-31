import kubernetes
from kubernetes.client.models import V1Volume


class KubernetesVolume:
    """KubernetesVolume data passing method involves passing data by mounting a
    single multi-write Kubernetes volume to containers instead of using Argo's
    artifact passing method (which stores the data in an S3 blob store)."""

    def __init__(self, volume: V1Volume, path_prefix: str = 'artifact_data/'):
        if not isinstance(volume, (dict, V1Volume)):
            raise TypeError('volume must be either V1Volume or dict')
        self._volume = volume
        self._path_prefix = path_prefix

    def transform_workflow(self, workflow: dict) -> dict:
        from ..compiler._data_passing_using_volume import rewrite_data_passing_to_use_volumes
        if isinstance(self._volume, dict):
            volume_dict = self._volume
        else:
            volume_dict = kubernetes.kubernetes.client.ApiClient(
            ).sanitize_for_serialization(self._volume)
        return rewrite_data_passing_to_use_volumes(workflow, volume_dict,
                                                   self._path_prefix)

    def __call__(self, workflow: dict) -> dict:
        return self.transform_workflow(workflow)
