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

    def _find_active_jobs(self, flow_name, run_id=None, user=None, field_selector=None):
        flow_hash = hashed_label(flow_name)
        results = self._client.BatchV1Api().list_namespaced_job(
            namespace=self._namespace,
            label_selector="metaflow.org/flow-hash=%s" % flow_hash,
            field_selector=field_selector,
        )
        for job in results.items:
            match = (
                job.status.active
                and (
                    run_id is None
                    or job.metadata.annotations["metaflow/run_id"] == run_id
                )
                and (user is None or job.metadata.annotations["metaflow/user"] == user)
            )
            if match:
                yield job

    def list(self, flow_name, run_id, user):
        results = self._find_active_jobs(flow_name, run_id, user)

        return list(results)

    def kill_jobs(self, flow_name, run_id, user, echo):
        jobs = self._find_active_jobs(
            flow_name, run_id, user, field_selector="status.successful==0"
        )

        def _kill_job(job):
            r = RunningJob(
                self,
                name=job.metadata.name,
                uid=job.metadata.uid,
                namespace=job.metadata.namespace,
            )
            r.kill()
            echo("killed %s" % r.id if not r.is_running else "failed to kill %s" % r.id)

        with ThreadPoolExecutor() as executor:
            executor.map(_kill_job, jobs)

    def jobset(self, **kwargs):
        return KubernetesJobSet(self, **kwargs)

    def job(self, **kwargs):
        return KubernetesJob(self, **kwargs)
