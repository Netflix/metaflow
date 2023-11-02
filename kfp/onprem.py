from typing import Dict, Optional
from kfp import dsl


def mount_pvc(pvc_name='pipeline-claim',
              volume_name='pipeline',
              volume_mount_path='/mnt/pipeline'):
    """Modifier function to apply to a Container Op to simplify volume, volume
    mount addition and enable better reuse of volumes, volume claims across
    container ops.

    Example:
        ::

            train = train_op(...)
            train.apply(mount_pvc('claim-name', 'pipeline', '/mnt/pipeline'))
    """

    def _mount_pvc(task):
        from kubernetes import client as k8s_client
        # there can be other ops in a pipeline (e.g. ResourceOp, VolumeOp)
        # refer to #3906
        if not hasattr(task, "add_volume") or not hasattr(
                task, "add_volume_mount"):
            return task
        local_pvc = k8s_client.V1PersistentVolumeClaimVolumeSource(
            claim_name=pvc_name)
        return (task.add_volume(
            k8s_client.V1Volume(
                name=volume_name,
                persistent_volume_claim=local_pvc)).add_volume_mount(
                    k8s_client.V1VolumeMount(
                        mount_path=volume_mount_path, name=volume_name)))

    return _mount_pvc


def use_k8s_secret(
    secret_name: str = 'k8s-secret',
    k8s_secret_key_to_env: Optional[Dict] = None,
):
    """An operator that configures the container to use k8s credentials.

    k8s_secret_key_to_env specifies a mapping from the name of the keys in the k8s secret to the name of the
    environment variables where the values will be added.

    The secret needs to be deployed manually a priori.

    Example:
        ::

            train = train_op(...)
            train.apply(use_k8s_secret(secret_name='s3-secret',
            k8s_secret_key_to_env={'secret_key': 'AWS_SECRET_ACCESS_KEY'}))

        This will load the value in secret 's3-secret' at key 'secret_key' and source it as the environment variable
        'AWS_SECRET_ACCESS_KEY'. I.e. it will produce the following section on the pod:
        env:
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: s3-secret
              key: secret_key
    """

    k8s_secret_key_to_env = k8s_secret_key_to_env or {}

    def _use_k8s_secret(task):
        from kubernetes import client as k8s_client
        for secret_key, env_var in k8s_secret_key_to_env.items():
            task.container \
                .add_env_variable(
                k8s_client.V1EnvVar(
                    name=env_var,
                    value_from=k8s_client.V1EnvVarSource(
                        secret_key_ref=k8s_client.V1SecretKeySelector(
                            name=secret_name,
                            key=secret_key
                        )
                    )
                )
            )
        return task

    return _use_k8s_secret


def add_default_resource_spec(
    memory_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    memory_request: Optional[str] = None,
    cpu_request: Optional[str] = None,
):
    """Add default resource requests and limits.

    For resource units, refer to https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes.

    Args:
      memory_limit: optional, memory limit. Format can be 512Mi, 2Gi etc.
      cpu_limit: optional, cpu limit. Format can be 0.5, 500m etc.
      memory_request: optional, defaults to memory limit.
      cpu_request: optional, defaults to cpu limit.
    """
    if not memory_request:
        memory_request = memory_limit
    if not cpu_request:
        cpu_request = cpu_limit

    def _add_default_resource_spec(task):
        # Skip tasks which are not container ops.
        if not isinstance(task, dsl.ContainerOp):
            return task
        _apply_default_resource(task, 'cpu', cpu_request, cpu_limit)
        _apply_default_resource(task, 'memory', memory_request, memory_limit)
        return task

    return _add_default_resource_spec


def _apply_default_resource(task: dsl.ContainerOp, resource_name: str,
                            default_request: Optional[str],
                            default_limit: Optional[str]):
    if task.container.get_resource_limit(resource_name):
        # Do nothing.
        # Limit is set, request will default to limit if not set (Kubernetes default behavior),
        # so we do not need to further apply defaults.
        return

    if default_limit:
        task.container.add_resource_limit(resource_name, default_limit)
    if default_request:
        if not task.container.get_resource_request(resource_name):
            task.container.add_resource_request(resource_name, default_request)
