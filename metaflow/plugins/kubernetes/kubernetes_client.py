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

    def list(self, flow_name):
        flow_hash = hashed_label(flow_name)
        results = self._client.BatchV1Api().list_namespaced_job(
            namespace=self._namespace,
            label_selector="metaflow.org/flow-hash=%s" % flow_hash,
        )
        seen = set()

        def _process_results(result):
            if result.metadata.annotations["metaflow/run_id"] in seen:
                return
            seen.add(result.metadata.annotations["metaflow/run_id"])
            return result

        return list(filter(_process_results, results.items))

    def kill(self, flow_name, run_id):
        flow_hash = hashed_label(flow_name)
        results = self._client.BatchV1Api().list_namespaced_job(
            namespace=self._namespace,
            label_selector="metaflow.org/flow-hash=%s" % flow_hash,
            field_selector="status.successful==0",
        )
        # Filter based on run_id in memory in order to not have to introduce yet another label on Kubernetes
        jobs = [
            job
            for job in results.items
            if job.metadata.annotations["metaflow/run_id"] == run_id
        ]
        for job in jobs:
            r = RunningJob(
                self,
                name=job.metadata.name,
                uid=job.metadata.uid,
                namespace=job.metadata.namespace,
            )
            print(r.status)
            r.kill()
        return True if jobs else False

    def jobset(self, **kwargs):
        return KubernetesJobSet(self, **kwargs)

    def job(self, **kwargs):
        return KubernetesJob(self, **kwargs)
