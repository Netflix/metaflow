from concurrent.futures import ThreadPoolExecutor
import os
import sys
import time

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
from .kube_utils import hashed_label

from .kubernetes_job import KubernetesJob, KubernetesJobSet, RunningJob

CLIENT_REFRESH_INTERVAL_SECONDS = 300


class KubernetesClientException(MetaflowException):
    headline = "Kubernetes client error"


class KubernetesClient(object):
    def __init__(self):
        try:
            # Kubernetes is a soft dependency.
            from kubernetes import client, config
        except (NameError, ImportError):
            raise KubernetesClientException(
                "Could not import module 'kubernetes'.\n\nInstall Kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install kubernetes\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )
        self._refresh_client()
        self._namespace = KUBERNETES_NAMESPACE

    def _refresh_client(self):
        from kubernetes import client, config

        if os.getenv("KUBECONFIG"):
            # There are cases where we're running inside a pod, but can't use
            # the kubernetes client for that pod's cluster: for example when
            # running in Bitbucket Cloud or other CI system.
            # In this scenario, the user can set a KUBECONFIG environment variable
            # to load the kubeconfig, regardless of whether we're in a pod or not.
            config.load_kube_config()
        elif os.getenv("KUBERNETES_SERVICE_HOST"):
            # We are inside a pod, authenticate via ServiceAccount assigned to us
            config.load_incluster_config()
        else:
            # Default to using kubeconfig, likely $HOME/.kube/config
            # TODO (savin):
            #  1. Support generating kubeconfig on the fly using boto3
            #  2. Support auth via OIDC - https://docs.aws.amazon.com/eks/latest/userguide/authenticate-oidc-identity-provider.html
            config.load_kube_config()
        self._client = client
        self._client_refresh_timestamp = time.time()

    def get(self):
        if (
            time.time() - self._client_refresh_timestamp
            > CLIENT_REFRESH_INTERVAL_SECONDS
        ):
            self._refresh_client()

        return self._client

    def _find_active_pods(self, flow_name, run_id=None, user=None):
        flow_hash = hashed_label(flow_name)
        results = self._client.CoreV1Api().list_namespaced_pod(
            namespace=self._namespace,
            label_selector="metaflow.org/flow-hash=%s" % flow_hash,
            # limited selector support for K8S api. We want to cover multiple statuses: Running / Pending / Unknown
            field_selector="status.phase!=Succeeded,status.phase!=Failed",
        )
        if run_id is not None:
            # handle argo prefixes in run_id
            run_id = run_id[run_id.startswith("argo-") and len("argo-") :]
        for pod in results.items:
            match = (
                run_id is None
                or (pod.metadata.annotations.get("metaflow/run_id") == run_id)
                # we want to also match pods launched by argo-workflows
                or (pod.metadata.labels.get("workflows.argoproj.io/workflow") == run_id)
            ) and (
                user is None or pod.metadata.annotations.get("metaflow/user") == user
            )
            if match:
                yield pod

    def list(self, flow_name, run_id, user):
        results = self._find_active_pods(flow_name, run_id, user)

        return list(results)

    def kill_pods(self, flow_name, run_id, user, echo):
        from kubernetes.stream import stream

        api_instance = self._client.CoreV1Api()
        pods = self._find_active_pods(flow_name, run_id, user)

        def _kill_pod(pod):
            echo(
                "Attempting to kill pod %s in namespace %s"
                % (pod.metadata.name, pod.metadata.namespace)
            )
            try:
                stream(
                    api_instance.connect_get_namespaced_pod_exec,
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    container="main",  # required for argo-workflows due to multiple containers in a pod
                    command=[
                        "/bin/sh",
                        "-c",
                        "/sbin/killall5",
                    ],
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                )
                echo("killed pod %s" % pod.metadata.name)
            except Exception as ex:
                # best effort kill for pod can fail.
                echo("failed to kill pod: %s" % str(ex))

        with ThreadPoolExecutor() as executor:
            executor.map(_kill_pod, list(pods))

    def jobset(self, **kwargs):
        return KubernetesJobSet(self, **kwargs)

    def job(self, **kwargs):
        return KubernetesJob(self, **kwargs)
