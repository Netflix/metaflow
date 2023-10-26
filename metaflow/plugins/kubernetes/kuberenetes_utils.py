import json
import math
import random
import time

from metaflow.exception import MetaflowException

from metaflow.metaflow_config import KUBERNETES_SECRETS


def compute_resource_limits(args):
    limits_dict = dict()
    if args.get("resource_limits_memory", None):
        limits_dict["memory"] = "%sM" % str(args["resource_limits_memory"])
    if args.get("resource_limits_cpu", None):
        limits_dict["cpu"] = args["resource_limits_cpu"]
    if args["gpu"] is not None:
        limits_dict["%s.com/gpu".lower() % args["gpu_vendor"]] = str(args["gpu"])
    return limits_dict


def get_list_from_untyped(possible_list):
    return (
        []
        if not possible_list
        else [possible_list]
        if isinstance(possible_list, str)
        else possible_list
    )


def compute_tempfs_enabled(args):
    use_tmpfs = args["use_tmpfs"]
    tmpfs_size = args["tmpfs_size"]
    return use_tmpfs or (tmpfs_size and not use_tmpfs)


def make_kubernetes_container(
    client, name, commands, args, envs, additional_secrets=[]
):
    from kubernetes.client import V1SecurityContext, ApiClient

    class KubernetesClientDataObj(object):
        def __init__(self, data_dict, class_name):
            self.data = json.dumps(data_dict) if data_dict is not None else None
            self.class_name = class_name

        def get_deserialized_object(self):
            if self.data is not None:
                return ApiClient().deserialize(self, self.class_name)
            else:
                return None

    security_context = KubernetesClientDataObj(
        args["security_context"], V1SecurityContext
    ).get_deserialized_object()

    # tmpfs variables
    tmpfs_enabled = compute_tempfs_enabled(args)

    return client.V1Container(
        name=name,
        command=commands,
        env=[client.V1EnvVar(name=k, value=str(v)) for k, v in envs.items()]
        # And some downward API magic. Add (key, value)
        # pairs below to make pod metadata available
        # within Kubernetes container.
        + [
            client.V1EnvVar(
                name=k,
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path=str(v))
                ),
            )
            for k, v in {
                "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
            }.items()
        ],
        env_from=[
            client.V1EnvFromSource(
                secret_ref=client.V1SecretEnvSource(
                    name=str(k),
                    # optional=True
                )
            )
            for k in get_list_from_untyped(args.get("secrets")) + additional_secrets
            if k
        ],
        image=args["image"],
        security_context=security_context,
        image_pull_policy=args["image_pull_policy"],
        resources=client.V1ResourceRequirements(
            requests={
                "cpu": str(args["cpu"]),
                "memory": "%sM" % str(args["memory"]),
                "ephemeral-storage": "%sM" % str(args["disk"]),
            },
            limits=compute_resource_limits(args),
        ),
        volume_mounts=(
            [
                client.V1VolumeMount(
                    mount_path=args.get("tmpfs_path"),
                    name="tmpfs-ephemeral-volume",
                )
            ]
            if tmpfs_enabled
            else []
        )
        + (
            [
                client.V1VolumeMount(mount_path=path, name=claim)
                for claim, path in args["persistent_volume_claims"].items()
            ]
            if args["persistent_volume_claims"] is not None
            else []
        ),
    )
