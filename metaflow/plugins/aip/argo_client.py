# talebz copied from https://github.com/Netflix/metaflow/blob/master/metaflow/plugins/argo/argo_client.py

import json
from typing import Any, Dict, Optional, List

from metaflow.exception import MetaflowException
from metaflow.plugins.aws.eks.kubernetes_client import KubernetesClient


class ArgoClientException(MetaflowException):
    headline = "Argo Client error"


class ArgoClient(object):
    def __init__(self, namespace=None):
        self._client = KubernetesClient()
        self._namespace = namespace or "default"
        self._group = "argoproj.io"
        self._version = "v1alpha1"

    def get_workflow_template(self, name):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=name,
            )
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def get_workflow_run_status(self, name):
        client = self._client.get()
        try:
            workflow: Dict[
                str, Any
            ] = client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                name=name,
            )

            if "metadata" in workflow and "labels" in workflow["metadata"]:
                return workflow["metadata"]["labels"]["workflows.argoproj.io/phase"]
            else:
                return None
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def create_workflow_config_map(self, name: str, config_map: Dict[str, Any]):
        client = self._client.get()
        try:
            return client.CoreV1Api().create_namespaced_config_map(
                namespace=self._namespace,
                body=config_map,
            )
        except client.rest.ApiException as e:
            if e.status == 409:  # Conflict status code
                try:
                    return client.CoreV1Api().replace_namespaced_config_map(
                        name=name, namespace=self._namespace, body=config_map
                    )
                except client.rest.ApiException as e:
                    raise ArgoClientException(
                        json.loads(e.body)["message"]
                        if e.body is not None
                        else e.reason
                    )
            else:
                raise ArgoClientException(
                    json.loads(e.body)["message"] if e.body is not None else e.reason
                )

    def list_workflow_template(self, namespace: Optional[str] = None):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().list_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=namespace or self._namespace,
                plural="workflowtemplates",
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def trigger_workflow_template(self, name: str, parameters: Optional[Dict] = None):
        client = self._client.get()
        body = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {
                "generateName": name + "-",
            },
            "spec": {
                "workflowTemplateRef": {"name": name},
                "arguments": {
                    "parameters": [dict(name=k, value=v) for k, v in parameters.items()]
                }
                if parameters
                else None,
            },
        }
        try:
            return client.CustomObjectsApi().create_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                body=body,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def run_workflow(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        client = self._client.get()
        try:
            return client.CustomObjectsApi().create_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                body=workflow,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def patch_argo_object(self, name: str, plural: str, body: List):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().patch_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural=plural,
                name=name,
                body=body,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def delete_workflow_template(self, name: str):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=name,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )
