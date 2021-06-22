import posixpath
import requests

from metaflow.metaflow_config import from_conf
from .argo_exception import ArgoException


class ArgoClient(object):
    """Works with Argo Workflows' resources using the REST Api Server"""

    def __init__(self, auth, k8s_namespace):
        self.server = from_conf('METAFLOW_ARGO_SERVER')
        if self.server is None:
            raise ArgoException("The METAFLOW_ARGO_SERVER is needed to support "
                                "the create, trigger or list-runs command")
        self.auth = auth
        if k8s_namespace is None:
            k8s_namespace = 'default'
        self.k8s_namespace = from_conf('METAFLOW_K8S_NAMESPACE', default=k8s_namespace)

    def create_template(self, name, definition):
        """
        Deploys the Argo WorkflowTemplate.  Overwrites the
        existing one with the same name
        """
        template = self.get_template(name)
        h = {'Authorization': self.auth}
        if template:
            # overwrite the WorkflowTemplate but had to keep metadata
            template['spec'] = definition['spec']
            template['metadata']['labels'] = definition['metadata'].get('labels', {})
            template['metadata']['annotations'] = definition['metadata'].get('annotations', {})
            url = posixpath.join(self.server,
                                 'api/v1/workflow-templates',
                                 self.k8s_namespace,
                                 name)
            r = requests.put(url, headers=h, json={'template': template})
        else:
            url = posixpath.join(self.server,
                                 'api/v1/workflow-templates',
                                 self.k8s_namespace)
            r = requests.post(url, headers=h, json={'template': definition})
        r.raise_for_status()
        return r.json()

    def submit(self, workflow):
        """
        Submits an Argo Workflow from the WorkflowTemplate
        """
        url = posixpath.join(self.server,
                             'api/v1/workflows',
                             self.k8s_namespace)
        r = requests.post(url,
                          headers={'Authorization': self.auth},
                          json={'workflow': workflow})
        r.raise_for_status()
        return r.json()

    def list_workflows(self, name, phases):
        """
        Lists Argo Workflows spawned from the WorkflowTemplate "name"
        """
        url = posixpath.join(self.server,
                             'api/v1/workflows',
                             self.k8s_namespace)
        params = {
            'fields': 'items.metadata.name,items.status.phase,'
                      'items.status.startedAt,items.status.finishedAt'
        }
        selectors = ['metaflow.workflow_template=%s' % name]
        if phases:
            selectors.append('workflows.argoproj.io/phase in (%s)' % ','.join(phases))
        params['listOptions.labelSelector'] = ', '.join(selectors)
        r = requests.get(url, headers={'Authorization': self.auth}, params=params)
        r.raise_for_status()
        return r.json()['items']

    def get_template(self, name):
        """
        Returns a WorkflowTemplate spec
        """
        url = posixpath.join(self.server,
                             'api/v1/workflow-templates',
                             self.k8s_namespace,
                             name)
        r = requests.get(url, headers={'Authorization': self.auth})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
