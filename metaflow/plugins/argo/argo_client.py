import json
import os
import sys

from metaflow.exception import MetaflowException


class ArgoClientException(MetaflowException):
    headline = "Argo Client error"


class ArgoClient(object):
    def __init__(self, namespace=None):

        try:
            from kubernetes import client, config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'kubernetes'.\n\nInstall kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install kubernetes\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

        if os.getenv("KUBERNETES_SERVICE_HOST"):
            # We are inside a pod, authenticate via ServiceAccount assigned
            # to us
            config.load_incluster_config()
        else:
            # Use kubeconfig, likely $HOME/.kube/config
            # TODO (savin):
            #     1. Support generating kubeconfig on the fly using boto3
            #     2. Support auth via OIDC -
            #        https://docs.aws.amazon.com/eks/latest/userguide/authenticate-oidc-identity-provider.html
            config.load_kube_config()

        self._client = client
        self._namespace = namespace or "default"
        self._group = "argoproj.io"
        self._version = "v1alpha1"

    def get_workflow_template(self, name):
        try:
            return self._client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=name,
            )
        except self._client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def register_workflow_template(self, name, workflow_template):
        # Unfortunately, Kubernetes client does not handle optimistic
        # concurrency control by itself unlike kubectl
        try:
            workflow_template["metadata"][
                "resourceVersion"
            ] = self._client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=name,
            )[
                "metadata"
            ][
                "resourceVersion"
            ]
        except self._client.rest.ApiException as e:
            if e.status == 404:
                try:
                    return (
                        self._client.CustomObjectsApi().create_namespaced_custom_object(
                            group=self._group,
                            version=self._version,
                            namespace=self._namespace,
                            plural="workflowtemplates",
                            body=workflow_template,
                        )
                    )
                except self._client.rest.ApiException as e:
                    raise ArgoClientException(
                        json.loads(e.body)["message"]
                        if e.body is not None
                        else e.reason
                    )
            else:
                raise ArgoClientException(
                    json.loads(e.body)["message"] if e.body is not None else e.reason
                )
        try:
            return self._client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                body=workflow_template,
                name=name,
            )
        except self._client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def trigger_workflow_template(self, name, parameters={}):
        body = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {"generateName": name + "-"},
            "spec": {
                "workflowTemplateRef": {"name": name},
                "arguments": {
                    "parameters": [
                        {"name": k, "value": json.dumps(v)}
                        for k, v in parameters.items()
                    ]
                },
            },
        }
        try:
            return self._client.CustomObjectsApi().create_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                body=body,
            )
        except self._client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def schedule_workflow_template(self, name, schedule=None):
        # Unfortunately, Kubernetes client does not handle optimistic
        # concurrency control by itself unlike kubectl
        body = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "CronWorkflow",
            "metadata": {"name": name},
            "spec": {
                "suspend": schedule is None,
                "schedule": schedule,
                "workflowSpec": {"workflowTemplateRef": {"name": name}},
            },
        }
        try:
            body["metadata"][
                "resourceVersion"
            ] = self._client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="cronworkflows",
                name=name,
            )[
                "metadata"
            ][
                "resourceVersion"
            ]
        except self._client.rest.ApiException as e:
            # Scheduled workflow does not exist and we want to schedule a workflow
            if e.status == 404:
                if schedule is None:
                    return
                try:
                    return (
                        self._client.CustomObjectsApi().create_namespaced_custom_object(
                            group=self._group,
                            version=self._version,
                            namespace=self._namespace,
                            plural="cronworkflows",
                            body=body,
                        )
                    )
                except self._client.rest.ApiException as e:
                    raise ArgoClientException(
                        json.loads(e.body)["message"]
                        if e.body is not None
                        else e.reason
                    )
            else:
                raise ArgoClientException(
                    json.loads(e.body)["message"] if e.body is not None else e.reason
                )
        try:
            return self._client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="cronworkflows",
                body=body,
                name=name,
            )
        except self._client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def register_trigger_on_template(self, name, sensor_template):
        try:
            sensor_template["metadata"][
                "resourceVersion"
            ] = self._client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                name=name,
            )[
                "metadata"
            ][
                "resourceVersion"
            ]
        except self._client.rest.ApiException as e:
            if e.status == 404:
                try:
                    return (
                        self._client.CustomObjectsApi().create_namespaced_custom_object(
                            group=self._group,
                            version=self._version,
                            namespace=self._namespace,
                            plural="sensors",
                            body=sensor_template,
                        )
                    )
                except self._client.rest.ApiException as e:
                    raise ArgoClientException(
                        json.loads(e.body)["message"]
                        if e.body is not None
                        else e.reason
                    )
            else:
                raise ArgoClientException(
                    json.loads(e.body)["message"] if e.body is not None else e.reason
                )
        try:
            return self._client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                name=name,
                body=sensor_template,
            )
        except self._client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def remove_existing_trigger_on_template(self, name, dry_run=False):
        if dry_run:
            return self._describe_remove_existing_trigger_on_template(name)
        else:
            return self._remove_existing_trigger_on_template(name)

    def _remove_existing_trigger_on_template(self, name):
        try:
            self._client.CustomObjectsApi().delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                name=name,
                grace_period_seconds=0,
            )
            return (True,)
        except self._client.rest.ApiException as e:
            if e.status == 404:
                return (False,)

            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def _describe_remove_existing_trigger_on_template(self, name):
        try:
            results = self._client.CustomObjectsApi().list_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                watch=False,
            )
            templates = results["items"]
            for t in templates:
                if t["metadata"]["name"] == name:
                    annotations = t["metadata"]["annotations"]
                    triggered_by = annotations["metaflow/triggered_by"]
                    trigger_status = annotations["metaflow/trigger_status"]
                    return (True, triggered_by, trigger_status)
            return (False,)
        except self._client.rest.ApiException as e:
            if e.status == 404:
                return (False,)

            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def remove_existing_workflow_template(self, flow_name, dry_run=False):
        if dry_run:
            return self._describe_remove_existing_workflow_template(flow_name)
        else:
            return self._remove_existing_workflow_template(flow_name)

    def _describe_remove_existing_workflow_template(self, name):
        try:
            self._client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=name,
            )
            return True
        except self._client.rest.ApiException as e:
            if e.status == 404:
                return False

            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def _remove_existing_workflow_template(self, flow_name):
        try:
            self._client.CustomObjectsApi().delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                name=flow_name,
            )
            return True
        except self._client.rest.ApiException as e:
            if e.status == 404:
                return False

            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )
