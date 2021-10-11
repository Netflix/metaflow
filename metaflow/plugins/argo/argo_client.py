import posixpath
import requests
from requests.packages.urllib3.util.retry import Retry

from metaflow.metaflow_config import from_conf
from .argo_exception import ArgoException


class ArgoClient(object):
    """Works with Argo Workflows' resources using the REST Api Server"""

    def __init__(self, auth, k8s_namespace):
        server = from_conf('METAFLOW_ARGO_SERVER')
        if server is None:
            raise ArgoException("The METAFLOW_ARGO_SERVER is needed to support "
                                "the create, trigger or list-runs command")
        self.api = posixpath.join(server, 'api/v1')

        if k8s_namespace is None:
            k8s_namespace = 'default'
        self.k8s_namespace = from_conf('METAFLOW_K8S_NAMESPACE', default=k8s_namespace)

        self.sess = requests.Session()
        self.sess.headers.update({'Authorization': auth})
        self.sess.hooks = {'response': lambda r, *args, **kwargs: r.raise_for_status()}

        # Sometimes POST fails because DELETE doesn't remove a resource immediately
        retry = Retry(total=3, status_forcelist=[409], method_whitelist=['POST'], backoff_factor=1)
        self.sess.mount(self.api, requests.adapters.HTTPAdapter(max_retries=retry))

    def create_template(self, name, definition):
        """
        Deploys the Argo WorkflowTemplate.  Overwrites the
        existing one with the same name
        """
        try:
            # Try to delete the WorkflowTemplate if exists
            url = posixpath.join(self.api, 'workflow-templates', self.k8s_namespace, name)
            self.sess.delete(url)
        except requests.HTTPError as err:
            if err.response.status_code != 404:
                raise

        url = posixpath.join(self.api, 'workflow-templates', self.k8s_namespace)
        r = self.sess.post(url, json={'template': definition})
        return r.json()

    def submit(self, workflow):
        """
        Submits an Argo Workflow from the WorkflowTemplate
        """
        url = posixpath.join(self.api, 'workflows', self.k8s_namespace)
        r = requests.post(url, json={'workflow': workflow})
        return r.json()

    def list_workflows(self, name, phases):
        """
        Lists Argo Workflows spawned from the WorkflowTemplate "name"
        """
        params = {
            'fields': 'items.metadata.name,items.status.phase,'
                      'items.status.startedAt,items.status.finishedAt'
        }
        selectors = ['metaflow.workflow_template=%s' % name]
        if phases:
            selectors.append('workflows.argoproj.io/phase in (%s)' % ','.join(phases))
        params['listOptions.labelSelector'] = ', '.join(selectors)

        url = posixpath.join(self.api, 'workflows', self.k8s_namespace)
        r = self.sess.get(url, params=params)
        return r.json()['items']

    def get_template(self, name):
        """
        Returns a WorkflowTemplate spec
        """
        try:
            url = posixpath.join(self.api, 'workflow-templates', self.k8s_namespace, name)
            r = self.sess.get(url)
            return r.json()
        except requests.HTTPError as err:
            if err.response.status_code == 404:
                return None
            raise

    def create_cron_wf(self, name, definition):
        """
        Deploys the Argo Cron Workflow Template.
        Overwrites the existing one with the same name
        """
        self.delete_cron_wf(name)
        url = posixpath.join(self.api, 'cron-workflows', self.k8s_namespace)
        r = self.sess.post(url, json={'cronWorkflow': definition})
        return r.json()

    def delete_cron_wf(self, name):
        try:
            # Try to delete the cron wf if exists
            url = posixpath.join(self.api, 'cron-workflows', self.k8s_namespace, name)
            params = {
                'deleteOptions.propagationPolicy': 'Orphan' # preserve existing workflows
            }
            self.sess.delete(url, params=params)
        except requests.HTTPError as err:
            if err.response.status_code != 404:
                raise
