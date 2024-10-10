import json
import os
import sys

from metaflow.exception import MetaflowException
from metaflow.plugins.kubernetes.kubernetes_client import KubernetesClient


class ArgoClientException(MetaflowException):
    headline = "Argo Client error"


class ArgoResourceNotFound(MetaflowException):
    headline = "Resource not found"


class ArgoNotPermitted(MetaflowException):
    headline = "Operation not permitted"


class ArgoClient(object):
    def __init__(self, namespace=None):
        self._client = KubernetesClient()
        self._namespace = namespace or "default"
        self._group = "argoproj.io"
        self._version = "v1alpha1"

    def get_workflow(self, name):
        client = self._client.get()
        try:
            workflow = client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                name=name,
            )
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )
        return workflow

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

    def get_workflow_templates(self):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().list_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
            )["items"]
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def register_workflow_template(self, name, workflow_template):
        # Unfortunately, Kubernetes client does not handle optimistic
        # concurrency control by itself unlike kubectl
        client = self._client.get()
        try:
            workflow_template["metadata"][
                "resourceVersion"
            ] = client.CustomObjectsApi().get_namespaced_custom_object(
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
        except client.rest.ApiException as e:
            if e.status == 404:
                try:
                    return client.CustomObjectsApi().create_namespaced_custom_object(
                        group=self._group,
                        version=self._version,
                        namespace=self._namespace,
                        plural="workflowtemplates",
                        body=workflow_template,
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
        try:
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflowtemplates",
                body=workflow_template,
                name=name,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def delete_cronworkflow(self, name):
        """
        Issues an API call for deleting a cronworkflow

        Returns either the successful API response, or None in case the resource was not found.
        """
        client = self._client.get()

        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="cronworkflows",
                name=name,
            )
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            else:
                raise wrap_api_error(e)

    def delete_workflow_template(self, name):
        """
        Issues an API call for deleting a cronworkflow

        Returns either the successful API response, or None in case the resource was not found.
        """
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
            if e.status == 404:
                return None
            else:
                raise wrap_api_error(e)

    def terminate_workflow(self, name):
        client = self._client.get()
        try:
            workflow = client.CustomObjectsApi().get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                name=name,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

        if workflow["status"]["finishedAt"] is not None:
            raise ArgoClientException(
                "Cannot terminate an execution that has already finished."
            )
        if workflow["spec"].get("shutdown") == "Terminate":
            raise ArgoClientException("Execution has already been terminated.")

        try:
            body = {"spec": workflow["spec"]}
            body["spec"]["shutdown"] = "Terminate"
            return client.CustomObjectsApi().patch_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                name=name,
                body=body,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def suspend_workflow(self, name):
        workflow = self.get_workflow(name)
        if workflow is None:
            raise ArgoClientException("Execution argo-%s was not found" % name)

        if workflow["status"]["finishedAt"] is not None:
            raise ArgoClientException(
                "Cannot suspend an execution that has already finished."
            )
        if workflow["spec"].get("suspend") is True:
            raise ArgoClientException("Execution has already been suspended.")

        body = {"spec": workflow["spec"]}
        body["spec"]["suspend"] = True
        return self._patch_workflow(name, body)

    def unsuspend_workflow(self, name):
        workflow = self.get_workflow(name)
        if workflow is None:
            raise ArgoClientException("Execution argo-%s was not found" % name)

        if workflow["status"]["finishedAt"] is not None:
            raise ArgoClientException(
                "Cannot unsuspend an execution that has already finished."
            )
        if not workflow["spec"].get("suspend", False):
            raise ArgoClientException("Execution is already proceeding.")

        body = {"spec": workflow["spec"]}
        body["spec"]["suspend"] = False
        return self._patch_workflow(name, body)

    def _patch_workflow(self, name, body):
        client = self._client.get()
        try:
            return client.CustomObjectsApi().patch_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="workflows",
                name=name,
                body=body,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def trigger_workflow_template(self, name, parameters={}):
        client = self._client.get()
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

    def schedule_workflow_template(self, name, schedule=None, timezone=None):
        # Unfortunately, Kubernetes client does not handle optimistic
        # concurrency control by itself unlike kubectl
        client = self._client.get()
        body = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "CronWorkflow",
            "metadata": {"name": name},
            "spec": {
                "suspend": schedule is None,
                "schedule": schedule,
                "timezone": timezone,
                "failedJobsHistoryLimit": 10000,  # default is unfortunately 1
                "successfulJobsHistoryLimit": 10000,  # default is unfortunately 3
                "workflowSpec": {"workflowTemplateRef": {"name": name}},
            },
        }
        try:
            body["metadata"][
                "resourceVersion"
            ] = client.CustomObjectsApi().get_namespaced_custom_object(
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
        except client.rest.ApiException as e:
            # Scheduled workflow does not exist and we want to schedule a workflow
            if e.status == 404:
                if schedule is None:
                    return
                try:
                    return client.CustomObjectsApi().create_namespaced_custom_object(
                        group=self._group,
                        version=self._version,
                        namespace=self._namespace,
                        plural="cronworkflows",
                        body=body,
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
        try:
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="cronworkflows",
                body=body,
                name=name,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def register_sensor(self, name, sensor=None):
        if sensor is None:
            sensor = {}
        # Unfortunately, Kubernetes client does not handle optimistic
        # concurrency control by itself unlike kubectl
        client = self._client.get()
        if not sensor:
            sensor["metadata"] = {}

        try:
            sensor["metadata"][
                "resourceVersion"
            ] = client.CustomObjectsApi().get_namespaced_custom_object(
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
        except client.rest.ApiException as e:
            # Sensor does not exist and we want to add one
            if e.status == 404:
                try:
                    return client.CustomObjectsApi().create_namespaced_custom_object(
                        group=self._group,
                        version=self._version,
                        namespace=self._namespace,
                        plural="sensors",
                        body=sensor,
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
        try:
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                body=sensor,
                name=name,
            )
        except client.rest.ApiException as e:
            raise ArgoClientException(
                json.loads(e.body)["message"] if e.body is not None else e.reason
            )

    def delete_sensor(self, name):
        """
        Issues an API call for deleting a sensor

        Returns either the successful API response, or None in case the resource was not found.
        """
        client = self._client.get()

        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=self._namespace,
                plural="sensors",
                name=name,
            )
        except client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise wrap_api_error(e)


def wrap_api_error(error):
    message = (
        json.loads(error.body)["message"] if error.body is not None else error.reason
    )
    # catch all
    ex = ArgoClientException(message)
    if error.status == 404:
        # usually handled outside this function as most cases want to return None instead.
        ex = ArgoResourceNotFound(message)
    if error.status == 403:
        ex = ArgoNotPermitted(message)
    return ex
