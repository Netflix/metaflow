import json

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import from_conf


class ArgoException(MetaflowException):
    headline = 'Argo Workflows error'

class ArgoClient(object):
    """Works with Argo Workflows' resources using kubernetes"""

    def __init__(self, k8s_namespace):
        try:
            from kubernetes import client, config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'kubernetes'. Install kubernetes "
                "Python package (https://pypi.org/project/kubernetes/) first."
            )
        config.load_kube_config()
        self.client = client

        if k8s_namespace is None:
            k8s_namespace = 'default'
        self.k8s_namespace = from_conf('METAFLOW_K8S_NAMESPACE', default=k8s_namespace)
        try:
            core_api_instance = self.client.CoreV1Api()
            core_api_instance.read_namespace(self.k8s_namespace)
        except self.client.rest.ApiException as e:
            msg = json.loads(e.body)["message"] if e.body is not None else e.reason
            raise ArgoException(msg)

        self.api_instance = self.client.CustomObjectsApi()
        self.kwargs = {
            'version': 'v1alpha1',
            'group': 'argoproj.io',
            'namespace': self.k8s_namespace
        }

    def create_wf(self, name, body, plural):
        """
        Deploys the Argo Cron Workflow Template.
        Overwrites the existing one with the same name
        """
        self.delete_wf(name, plural)
        return self.register_wf(body, plural)

    def submit(self, workflow):
        """
        Submits an Argo Workflow from the WorkflowTemplate
        """
        return self.register_wf(workflow, 'workflows')

    def list_workflows(self, name, phases):
        """
        Lists Argo Workflows spawned from the WorkflowTemplate "name"
        """
        selectors = ['metaflow/workflow_template=%s' % name]
        if phases:
            selectors.append('workflows.argoproj.io/phase in (%s)' % ','.join(phases))
        label_selector = ', '.join(selectors)

        try:
            api_response = self.api_instance.list_namespaced_custom_object(label_selector=label_selector,
                                                                           plural='workflows', **self.kwargs)
        except self.client.rest.ApiException as e:
            msg = json.loads(e.body)["message"] if e.body is not None else e.reason
            raise ArgoException(msg)

        return api_response['items']

    def get_template(self, name):
        """
        Returns a WorkflowTemplate spec
        """
        try:
            return self.api_instance.get_namespaced_custom_object(name=name, plural='workflowtemplates',
                                                                          **self.kwargs)
        except self.client.rest.ApiException as e:
            if e.status == 404:
                return None
            msg = json.loads(e.body)["message"] if e.body is not None else e.reason
            raise ArgoException(msg)

    def delete_wf(self, name, plural):
        try:
            return self.api_instance.delete_namespaced_custom_object(name=name, plural=plural, **self.kwargs)
        except self.client.rest.ApiException as e:
            if e.status != 404:
                msg = json.loads(e.body)["message"] if e.body is not None else e.reason
                raise ArgoException(msg)

    def register_wf(self, body, plural):
        try:
            return self.api_instance.create_namespaced_custom_object(body=body, plural=plural,
                                                                         **self.kwargs)
        except self.client.rest.ApiException as e:
            msg = json.loads(e.body)["message"] if e.body is not None else e.reason
            raise ArgoException(msg)
