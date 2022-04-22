import os
import sys
import time

from metaflow.exception import MetaflowException


class KubernetesCredentialsException(MetaflowException):
    headline = "Kubernetes credentials error"


CLIENT_REFRESH_INTERVAL_SECONDS = 300


class KubernetesCredentialsProvider(object):
    def __init__(self):
        try:
            # Kubernetes is a soft dependency.
            from kubernetes import client, config
        except (NameError, ImportError):
            raise KubernetesCredentialsException(
                "Could not import module 'kubernetes'.\n\nInstall Kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install kubernetes\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )
        self._refresh_client()

    def _refresh_client(self):
        from kubernetes import client, config

        if os.getenv("KUBERNETES_SERVICE_HOST"):
            # We are inside a pod, authenticate via ServiceAccount assigned to us
            config.load_incluster_config()
        else:
            # Use kubeconfig, likely $HOME/.kube/config
            # TODO (savin):
            #     1. Support generating kubeconfig on the fly using boto3
            #     2. Support auth via OIDC - https://docs.aws.amazon.com/eks/latest/userguide/authenticate-oidc-identity-provider.html
            config.load_kube_config()
        self._client = client
        self._client_refresh_timestamp = time.time()

    def get_client(self):
        """Returns K8S client object from kuberenetes SDK"""
        if (
            time.time() - self._client_refresh_timestamp
            > CLIENT_REFRESH_INTERVAL_SECONDS
        ):
            self._refresh_client()

        return self._client
