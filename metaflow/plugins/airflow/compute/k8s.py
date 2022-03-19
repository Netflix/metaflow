from datetime import timedelta
import json
from metaflow.plugins.aws.eks.kubernetes import (
    Kubernetes,
    generate_rfc1123_name,
    sanitize_label_value,
)
from metaflow.metaflow_config import (
    BATCH_METADATA_SERVICE_URL,
    DATATOOLS_S3ROOT,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_S3,
    DEFAULT_METADATA,
    KUBERNETES_NAMESPACE,
    KUBERNETES_CONTAINER_IMAGE,
    BATCH_METADATA_SERVICE_HEADERS,
    DATASTORE_CARD_S3ROOT,
)


def create_k8s_args(
    datastore,
    metadata,
    environment,
    flow_name,
    run_id,
    step_name,
    task_id,
    attempt,
    code_package_url,
    code_package_sha,
    step_cli,
    docker_image,
    service_account=None,
    secrets=None,
    node_selector=None,
    namespace=None,
    cpu=None,
    gpu=None,
    disk=None,
    memory=None,
    run_time_limit=timedelta(days=5),
    retries=None,
    retry_delay=None,
    env={},
    user=None,
):

    k8s = Kubernetes(
        datastore, metadata, environment, flow_name, run_id, step_name, task_id, attempt
    )
    labels = {
        "app": "metaflow",
        "metaflow/flow_name": sanitize_label_value(flow_name),
        "metaflow/step_name": sanitize_label_value(step_name),
        "app.kubernetes.io/name": "metaflow-task",
        "app.kubernetes.io/part-of": "metaflow",
        "app.kubernetes.io/created-by": sanitize_label_value(user),
    }
    # Add Metaflow system tags as labels as well!
    for sys_tag in metadata.sticky_sys_tags:
        labels["metaflow/%s" % sys_tag[: sys_tag.index(":")]] = sanitize_label_value(
            sys_tag[sys_tag.index(":") + 1 :]
        )

    additional_mf_variables = {
        "METAFLOW_CODE_SHA": code_package_sha,
        "METAFLOW_CODE_URL": code_package_url,
        "METAFLOW_CODE_DS": datastore.TYPE,
        "METAFLOW_USER": user,
        "METAFLOW_SERVICE_URL": BATCH_METADATA_SERVICE_URL,
        "METAFLOW_SERVICE_HEADERS": json.dumps(BATCH_METADATA_SERVICE_HEADERS),
        "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
        "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
        "METAFLOW_DEFAULT_DATASTORE": "s3",
        "METAFLOW_DEFAULT_METADATA": "service",
        "METAFLOW_KUBERNETES_WORKLOAD": 1,
        "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
        "METAFLOW_CARD_S3ROOT": DATASTORE_CARD_S3ROOT,
    }
    env.update(additional_mf_variables)
    k8s_operator_args = dict(
        namespace=namespace,
        service_account=service_account,
        # todo : pass secrets from metaflow to Kubernetes via airflow
        secrets=secrets,
        node_selector=node_selector,
        cmds=k8s._command(
            code_package_url=code_package_url,
            step_cmds=[step_cli],
        ),
        in_cluster=True,
        image=docker_image,
        cpu=cpu,
        memory=memory,
        disk=disk,
        execution_timeout=dict(seconds=run_time_limit.total_seconds()),
        retry_delay=dict(seconds=retry_delay.total_seconds()) if retry_delay else None,
        retries=retries,
        env_vars=[dict(name=k, value=v) for k, v in env.items()],
        labels=labels,
        is_delete_operator_pod=True,
    )
    return k8s_operator_args


# todo : below code block only for reference. Remove it later.
# def type_func():
#     import typing
#     from kubernetes import kubernetes
#     import airflow

#     NoneType = type(None)
#     {
#         "namespace": typing.Union[str, NoneType],
#         "image": typing.Union[str, NoneType],
#         "name": typing.Union[str, NoneType],
#         "random_name_suffix": typing.Union[bool, NoneType],
#         "cmds": typing.Union[typing.List[str], NoneType],
#         "arguments": typing.Union[typing.List[str], NoneType],
#         "ports": typing.Union[
#             typing.List[kubernetes.client.models.v1_container_port.V1ContainerPort],
#             NoneType,
#         ],
#         "volume_mounts": typing.Union[
#             typing.List[kubernetes.client.models.v1_volume_mount.V1VolumeMount],
#             NoneType,
#         ],
#         "volumes": typing.Union[
#             typing.List[kubernetes.client.models.v1_volume.V1Volume], NoneType
#         ],
#         "env_vars": typing.Union[
#             typing.List[kubernetes.client.models.v1_env_var.V1EnvVar], NoneType
#         ],
#         "env_from": typing.Union[
#             typing.List[kubernetes.client.models.v1_env_from_source.V1EnvFromSource],
#             NoneType,
#         ],
#         "secrets": typing.Union[
#             typing.List[airflow.kubernetes.secret.Secret], NoneType
#         ],
#         "in_cluster": False,
#         "cluster_context": typing.Union[str, NoneType],
#         "labels": typing.Union[typing.Dict, NoneType],
#         "reattach_on_restart": False,
#         "startup_timeout_seconds": 0,
#         "get_logs": False,
#         "image_pull_policy": typing.Union[str, NoneType],
#         "annotations": typing.Union[typing.Dict, NoneType],
#         "resources": typing.Union[
#             kubernetes.client.models.v1_resource_requirements.V1ResourceRequirements,
#             NoneType,
#         ],
#         "affinity": typing.Union[
#             kubernetes.client.models.v1_affinity.V1Affinity, NoneType
#         ],
#         "config_file": typing.Union[str, NoneType],
#         "node_selectors": typing.Union[dict, NoneType],
#         "node_selector": typing.Union[dict, NoneType],
#         "image_pull_secrets": typing.Union[
#             typing.List[
#                 kubernetes.client.models.v1_local_object_reference.V1LocalObjectReference
#             ],
#             NoneType,
#         ],
#         "service_account_name": typing.Union[str, NoneType],
#         "is_delete_operator_pod": False,
#         "hostnetwork": False,
#         "tolerations": typing.Union[
#             typing.List[kubernetes.client.models.v1_toleration.V1Toleration], NoneType
#         ],
#         "security_context": typing.Union[typing.Dict, NoneType],
#         "dnspolicy": typing.Union[str, NoneType],
#         "schedulername": typing.Union[str, NoneType],
#         "full_pod_spec": typing.Union[kubernetes.client.models.v1_pod.V1Pod, NoneType],
#         "init_containers": typing.Union[
#             typing.List[kubernetes.client.models.v1_container.V1Container], NoneType
#         ],
#         "log_events_on_failure": False,
#         "do_xcom_push": False,
#         "pod_template_file": typing.Union[str, NoneType],
#         "priority_class_name": typing.Union[str, NoneType],
#         "pod_runtime_info_envs": typing.Union[
#             typing.List[kubernetes.client.models.v1_env_var.V1EnvVar], NoneType
#         ],
#         "termination_grace_period": typing.Union[int, NoneType],
#         "configmaps": typing.Union[typing.List[str], NoneType],
#         "return": None,
#     }
