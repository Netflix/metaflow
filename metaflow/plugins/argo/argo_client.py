import requests
import posixpath

from metaflow.metaflow_config import from_conf


class ArgoClient(object):
    """Works with Argo resources using the REST Api Server"""

    def __init__(self, auth, namespace):
        self.server = from_conf('METAFLOW_ARGO_SERVER')
        self.auth = auth
        self.namespace = namespace

    def create_template(self, name, definition):
        """
        Deploys the Argo WorkflowTemplate.  Overwrites the
        existing one with the same name
        """
        template = self.get_template(name)
        h = {'Authorization': self.auth}
        if template:
            # overwrite the WorkflowTemplate
            template['spec'] = definition['spec']
            url = posixpath.join(self.server,
                                 'api/v1/workflow-templates',
                                 self.namespace,
                                 name)
            r = requests.put(url, headers=h, json={'template': template})
        else:
            url = posixpath.join(self.server,
                                 'api/v1/workflow-templates',
                                 self.namespace)
            r = requests.post(url, headers=h, json={'template': definition})
        r.raise_for_status()
        return r.json()

    def submit(self, workflow):
        """
        Submits an Argo Workflow from the WorkflowTemplate
        """
        url = posixpath.join(self.server,
                             'api/v1/workflows',
                             self.namespace)
        r = requests.post(url,
                          headers={'Authorization': self.auth},
                          json={'workflow': workflow})
        r.raise_for_status()
        return r.json()

    def list_workflows(self, prefix, phases):
        """
        Lists Argo Workflows starting with 'prefix'
        """
        url = posixpath.join(self.server,
                             'api/v1/workflows',
                             self.namespace)
        params = {
            'fields': 'items.metadata.name,items.status.phase,items.status.startedAt,items.status.finishedAt'
        }
        if phases:
            params['listOptions.labelSelector'] = 'workflows.argoproj.io/phase in (%s)' % ','.join(phases)
        r = requests.get(url, headers={'Authorization': self.auth}, params=params)
        r.raise_for_status()
        workflows = r.json()['items']
        if workflows is None:
            return []
        return [w for w in workflows if w['metadata']['name'].startswith(prefix)]

    def get_template(self, name):
        """
        Returns a WorkflowTemplate spec
        """
        url = posixpath.join(self.server,
                             'api/v1/workflow-templates',
                             self.namespace,
                             name)
        r = requests.get(url, headers={'Authorization': self.auth})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
