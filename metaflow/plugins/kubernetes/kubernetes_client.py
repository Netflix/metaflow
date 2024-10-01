from concurrent.futures import ThreadPoolExecutor
import os
import sys
import time

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import KUBERNETES_NAMESPACE

from .kubernetes_job import KubernetesJob, KubernetesJobSet

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
        def _request(_continue=None):
            # handle paginated responses
            return self._client.CoreV1Api().list_namespaced_pod(
                namespace=self._namespace,
                # limited selector support for K8S api. We want to cover multiple statuses: Running / Pending / Unknown
                field_selector="status.phase!=Succeeded,status.phase!=Failed",
                limit=1000,
                _continue=_continue,
            )

        results = _request()

        if run_id is not None:
            # handle argo prefixes in run_id
            run_id = run_id[run_id.startswith("argo-") and len("argo-") :]

        while results.metadata._continue or results.items:
            for pod in results.items:
                match = (
                    # arbitrary pods might have no annotations at all.
                    pod.metadata.annotations
                    and pod.metadata.labels
                    and (
                        run_id is None
                        or (pod.metadata.annotations.get("metaflow/run_id") == run_id)
                        # we want to also match pods launched by argo-workflows
                        or (
                            pod.metadata.labels.get("workflows.argoproj.io/workflow")
                            == run_id
                        )
                    )
                    and (
                        user is None
                        or pod.metadata.annotations.get("metaflow/user") == user
                    )
                    and (
                        pod.metadata.annotations.get("metaflow/flow_name") == flow_name
                    )
                )
                if match:
                    yield pod
            if not results.metadata._continue:
                break
            results = _request(results.metadata._continue)

    def list(self, flow_name, run_id, user):
        results = self._find_active_pods(flow_name, run_id, user)

        return list(results)

    def kill_pods(self, flow_name, run_id, user, echo):
        from kubernetes.stream import stream

        api_instance = self._client.CoreV1Api()
        job_api = self._client.BatchV1Api()
        pods = self._find_active_pods(flow_name, run_id, user)

        def _kill_pod(pod):
            echo("Killing Kubernetes pod %s\n" % pod.metadata.name)
            try:
                stream(
                    api_instance.connect_get_namespaced_pod_exec,
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
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
            except Exception:
                # best effort kill for pod can fail.
                try:
                    job_name = pod.metadata.labels.get("job-name", None)
                    if job_name is None:
                        raise Exception("Could not determine job name")

                    job_api.patch_namespaced_job(
                        name=job_name,
                        namespace=pod.metadata.namespace,
                        field_manager="metaflow",
                        body={"spec": {"parallelism": 0}},
                    )
                except Exception as e:
                    echo("failed to kill pod %s - %s" % (pod.metadata.name, str(e)))

        with ThreadPoolExecutor() as executor:
            operated_pods = list(executor.map(_kill_pod, pods))

            if not operated_pods:
                echo("No active Kubernetes pods found for run *%s*" % run_id)

    def jobset(self, **kwargs):
        return KubernetesJobSet(self, **kwargs)

    def job(self, **kwargs):
        return KubernetesJob(self, **kwargs)
