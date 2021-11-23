from metaflow.exception import MetaflowException
from metaflow.metaflow_config import from_conf


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
        self.api_instance = self.client.CustomObjectsApi()

        if k8s_namespace is None:
            k8s_namespace = 'default'
        self.k8s_namespace = from_conf('METAFLOW_K8S_NAMESPACE', default=k8s_namespace)
        self.kwargs = {
            'version': 'v1alpha1',
            'group': 'argoproj.io',
            'namespace': self.k8s_namespace
        }

    def create_template(self, name, body):
        """
        Deploys the Argo WorkflowTemplate.  Overwrites the
        existing one with the same name
        """
        try:
            self.api_instance.delete_namespaced_custom_object(name=name, plural='workflowtemplates', **self.kwargs)
        except self.client.rest.ApiException as e:
            if e.status != 404:
                raise
        api_response = self.api_instance.create_namespaced_custom_object(body=body, plural='workflowtemplates',
                                                                         **self.kwargs)
        return api_response

    def submit(self, workflow):
        """
        Submits an Argo Workflow from the WorkflowTemplate
        """
        api_response = self.api_instance.create_namespaced_custom_object(body=workflow, plural='workflows',
                                                                         **self.kwargs)
        return api_response

    def list_workflows(self, name, phases):
        """
        Lists Argo Workflows spawned from the WorkflowTemplate "name"
        """
        selectors = ['metaflow.workflow_template=%s' % name]
        if phases:
            selectors.append('workflows.argoproj.io/phase in (%s)' % ','.join(phases))
        label_selector = ', '.join(selectors)

        api_response = self.api_instance.list_namespaced_custom_object(label_selector=label_selector,
                                                                       plural='workflows', **self.kwargs)

        return api_response['items']

    def get_template(self, name):
        """
        Returns a WorkflowTemplate spec
        """
        try:
            api_response = self.api_instance.get_namespaced_custom_object(name=name, plural='workflowtemplates',
                                                                          **self.kwargs)
        except self.client.rest.ApiException as e:
            if e.status == 404:
                return None
            raise

        return api_response

    def create_cron_wf(self, name, body):
        """
        Deploys the Argo Cron Workflow Template.
        Overwrites the existing one with the same name
        """
        self.delete_cron_wf(name)
        api_response = self.api_instance.create_namespaced_custom_object(body=body, plural='cronworkflows',
                                                                         **self.kwargs)
        return api_response

    def delete_cron_wf(self, name):
        try:
            self.api_instance.delete_namespaced_custom_object(name=name, plural='cronworkflows', **self.kwargs)
        except self.client.rest.ApiException as e:
            if e.status != 404:
                raise
