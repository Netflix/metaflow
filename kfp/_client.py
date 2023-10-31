# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import logging
import json
import os
import re
import tarfile
import tempfile
import warnings
import yaml
import zipfile
import datetime
import copy
from typing import Mapping, Callable, Optional

import kfp_server_api

from kfp import dsl
from kfp.compiler import compiler
from kfp.compiler._k8s_helper import sanitize_k8s_name

from kfp._auth import get_auth_token, get_gcp_access_token
from kfp_server_api import ApiException

# Operators on scalar values. Only applies to one of |int_value|,
# |long_value|, |string_value| or |timestamp_value|.
_FILTER_OPERATIONS = {
    "UNKNOWN": 0,
    "EQUALS": 1,
    "NOT_EQUALS": 2,
    "GREATER_THAN": 3,
    "GREATER_THAN_EQUALS": 5,
    "LESS_THAN": 6,
    "LESS_THAN_EQUALS": 7
}


def _add_generated_apis(target_struct, api_module, api_client):
    """Initializes a hierarchical API object based on the generated API module.

    PipelineServiceApi.create_pipeline becomes
    target_struct.pipelines.create_pipeline
    """
    Struct = type('Struct', (), {})

    def camel_case_to_snake_case(name):
        import re
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

    for api_name in dir(api_module):
        if not api_name.endswith('ServiceApi'):
            continue

        short_api_name = camel_case_to_snake_case(
            api_name[0:-len('ServiceApi')]) + 's'
        api_struct = Struct()
        setattr(target_struct, short_api_name, api_struct)
        service_api = getattr(api_module.api, api_name)
        initialized_service_api = service_api(api_client)
        for member_name in dir(initialized_service_api):
            if member_name.startswith('_') or member_name.endswith(
                    '_with_http_info'):
                continue

            bound_member = getattr(initialized_service_api, member_name)
            setattr(api_struct, member_name, bound_member)
    models_struct = Struct()
    for member_name in dir(api_module.models):
        if not member_name[0].islower():
            setattr(models_struct, member_name,
                    getattr(api_module.models, member_name))
    target_struct.api_models = models_struct


KF_PIPELINES_ENDPOINT_ENV = 'KF_PIPELINES_ENDPOINT'
KF_PIPELINES_UI_ENDPOINT_ENV = 'KF_PIPELINES_UI_ENDPOINT'
KF_PIPELINES_DEFAULT_EXPERIMENT_NAME = 'KF_PIPELINES_DEFAULT_EXPERIMENT_NAME'
KF_PIPELINES_OVERRIDE_EXPERIMENT_NAME = 'KF_PIPELINES_OVERRIDE_EXPERIMENT_NAME'
KF_PIPELINES_IAP_OAUTH2_CLIENT_ID_ENV = 'KF_PIPELINES_IAP_OAUTH2_CLIENT_ID'
KF_PIPELINES_APP_OAUTH2_CLIENT_ID_ENV = 'KF_PIPELINES_APP_OAUTH2_CLIENT_ID'
KF_PIPELINES_APP_OAUTH2_CLIENT_SECRET_ENV = 'KF_PIPELINES_APP_OAUTH2_CLIENT_SECRET'


class Client(object):
    """API Client for KubeFlow Pipeline.

    Args:
      host: The host name to use to talk to Kubeflow Pipelines. If not set, the in-cluster
          service DNS name will be used, which only works if the current environment is a pod
          in the same cluster (such as a Jupyter instance spawned by Kubeflow's
          JupyterHub). If you have a different connection to cluster, such as a kubectl
          proxy connection, then set it to something like "127.0.0.1:8080/pipeline.
          If you connect to an IAP enabled cluster, set it to
          https://<your-deployment>.endpoints.<your-project>.cloud.goog/pipeline".
      client_id: The client ID used by Identity-Aware Proxy.
      namespace: The namespace where the kubeflow pipeline system is run.
      other_client_id: The client ID used to obtain the auth codes and refresh tokens.
          Reference: https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_desktop_app.
      other_client_secret: The client secret used to obtain the auth codes and refresh tokens.
      existing_token: Pass in token directly, it's used for cases better get token outside of SDK, e.x. GCP Cloud Functions
          or caller already has a token
      cookies: CookieJar object containing cookies that will be passed to the pipelines API.
      proxy: HTTP or HTTPS proxy server
      ssl_ca_cert: Cert for proxy
      kube_context: String name of context within kubeconfig to use, defaults to the current-context set within kubeconfig.
      credentials: A TokenCredentialsBase object which provides the logic to
          populate the requests with credentials to authenticate against the API
          server.
      ui_host: Base url to use to open the Kubeflow Pipelines UI. This is used when running the client from a notebook to generate and
          print links.
      userid: The ID of the user creating the client.
    """

    # in-cluster DNS name of the pipeline service
    IN_CLUSTER_DNS_NAME = 'ml-pipeline.{}.svc.cluster.local:8888'
    KUBE_PROXY_PATH = 'api/v1/namespaces/{}/services/ml-pipeline:http/proxy/'

    # Auto populated path in pods
    # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod
    # https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#serviceaccount-admission-controller
    NAMESPACE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

    LOCAL_KFP_CONTEXT = os.path.expanduser('~/.config/kfp/context.json')
    KUBEFLOW_USERID_HEADER = 'kubeflow-userid'

    # TODO: Wrap the configurations for different authentication methods.
    def __init__(self,
                 host=None,
                 client_id=None,
                 namespace='kubeflow',
                 other_client_id=None,
                 other_client_secret=None,
                 existing_token=None,
                 cookies=None,
                 proxy=None,
                 ssl_ca_cert=None,
                 kube_context=None,
                 credentials=None,
                 ui_host=None,
                 userid=None):
        """Create a new instance of kfp client."""
        host = host or os.environ.get(KF_PIPELINES_ENDPOINT_ENV)
        self._uihost = os.environ.get(KF_PIPELINES_UI_ENDPOINT_ENV, ui_host or
                                      host)
        client_id = client_id or os.environ.get(
            KF_PIPELINES_IAP_OAUTH2_CLIENT_ID_ENV)
        other_client_id = other_client_id or os.environ.get(
            KF_PIPELINES_APP_OAUTH2_CLIENT_ID_ENV)
        other_client_secret = other_client_secret or os.environ.get(
            KF_PIPELINES_APP_OAUTH2_CLIENT_SECRET_ENV)

        config = self._load_config(host, client_id, namespace, other_client_id,
                                   other_client_secret, existing_token, proxy,
                                   ssl_ca_cert, kube_context, credentials)
        # Save the loaded API client configuration, as a reference if update is
        # needed.
        self._load_context_setting_or_default()
        self._existing_config = config
        if cookies is None:
            cookies = self._context_setting.get('client_authentication_cookie')
        api_client = kfp_server_api.api_client.ApiClient(
            config,
            cookie=cookies,
            header_name=self._context_setting.get(
                'client_authentication_header_name'),
            header_value=self._context_setting.get(
                'client_authentication_header_value'))
        if userid:
            api_client.set_default_header(Client.KUBEFLOW_USERID_HEADER, userid)
        _add_generated_apis(self, kfp_server_api, api_client)
        self._job_api = kfp_server_api.api.job_service_api.JobServiceApi(
            api_client)
        self._run_api = kfp_server_api.api.run_service_api.RunServiceApi(
            api_client)
        self._experiment_api = kfp_server_api.api.experiment_service_api.ExperimentServiceApi(
            api_client)
        self._pipelines_api = kfp_server_api.api.pipeline_service_api.PipelineServiceApi(
            api_client)
        self._upload_api = kfp_server_api.api.PipelineUploadServiceApi(
            api_client)
        self._healthz_api = kfp_server_api.api.healthz_service_api.HealthzServiceApi(
            api_client)
        if not self._context_setting['namespace'] and self.get_kfp_healthz(
        ).multi_user is True:
            try:
                with open(Client.NAMESPACE_PATH, 'r') as f:
                    current_namespace = f.read()
                    self.set_user_namespace(current_namespace)
            except FileNotFoundError:
                logging.info(
                    'Failed to automatically set namespace.', exc_info=False)

    def _load_config(self, host, client_id, namespace, other_client_id,
                     other_client_secret, existing_token, proxy, ssl_ca_cert,
                     kube_context, credentials):
        config = kfp_server_api.configuration.Configuration()

        if proxy:
            # https://github.com/kubeflow/pipelines/blob/c6ac5e0b1fd991e19e96419f0f508ec0a4217c29/backend/api/python_http_client/kfp_server_api/rest.py#L100
            config.proxy = proxy

        if ssl_ca_cert:
            config.ssl_ca_cert = ssl_ca_cert

        host = host or ''

        # Defaults to 'https' if host does not contain 'http' or 'https' protocol.
        if host and not host.startswith('http'):
            warnings.warn(
                'The host %s does not contain the "http" or "https" protocol.'
                ' Defaults to "https".' % host)
            host = 'https://' + host

        # Preprocess the host endpoint to prevent some common user mistakes.
        if not client_id:
            # always preserving the protocol (http://localhost requires it)
            host = host.rstrip('/')

        if host:
            config.host = host

        token = None

        # "existing_token" is designed to accept token generated outside of SDK. Here is an example.
        #
        # https://cloud.google.com/functions/docs/securing/function-identity
        # https://cloud.google.com/endpoints/docs/grpc/service-account-authentication
        #
        # import requests
        # import kfp
        #
        # def get_access_token():
        #     url = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token'
        #     r = requests.get(url, headers={'Metadata-Flavor': 'Google'})
        #     r.raise_for_status()
        #     access_token = r.json()['access_token']
        #     return access_token
        #
        # client = kfp.Client(host='<KFPHost>', existing_token=get_access_token())
        #
        if existing_token:
            token = existing_token
            self._is_refresh_token = False
        elif client_id:
            token = get_auth_token(client_id, other_client_id,
                                   other_client_secret)
            self._is_refresh_token = True
        elif self._is_inverse_proxy_host(host):
            token = get_gcp_access_token()
            self._is_refresh_token = False
        elif credentials:
            config.api_key['authorization'] = 'placeholder'
            config.api_key_prefix['authorization'] = 'Bearer'
            config.refresh_api_key_hook = credentials.refresh_api_key_hook

        if token:
            config.api_key['authorization'] = token
            config.api_key_prefix['authorization'] = 'Bearer'
            return config

        if host:
            # if host is explicitly set with auth token, it's probably a port forward address.
            return config

        import kubernetes as k8s
        in_cluster = True
        try:
            k8s.config.load_incluster_config()
        except:
            in_cluster = False
            pass

        if in_cluster:
            config.host = Client.IN_CLUSTER_DNS_NAME.format(namespace)
            config = self._get_config_with_default_credentials(config)
            return config

        try:
            k8s.config.load_kube_config(
                client_configuration=config, context=kube_context)
        except:
            print('Failed to load kube config.')
            return config

        if config.host:
            config.host = config.host + '/' + Client.KUBE_PROXY_PATH.format(
                namespace)
        return config

    def _is_inverse_proxy_host(self, host):
        if host:
            return re.match(r'\S+.googleusercontent.com/{0,1}$', host)
        if re.match(r'\w+', host):
            warnings.warn(
                'The received host is %s, please include the full endpoint address '
                '(with ".(pipelines/notebooks).googleusercontent.com")' % host)
        return False

    def _is_ipython(self):
        """Returns whether we are running in notebook."""
        try:
            import IPython
            ipy = IPython.get_ipython()
            if ipy is None:
                return False
        except ImportError:
            return False

        return True

    def _get_url_prefix(self):
        if self._uihost:
            # User's own connection.
            if self._uihost.startswith('http://') or self._uihost.startswith(
                    'https://'):
                return self._uihost
            else:
                return 'http://' + self._uihost

        # In-cluster pod. We could use relative URL.
        return '/pipeline'

    def _load_context_setting_or_default(self):
        if os.path.exists(Client.LOCAL_KFP_CONTEXT):
            with open(Client.LOCAL_KFP_CONTEXT, 'r') as f:
                self._context_setting = json.load(f)
        else:
            self._context_setting = {
                'namespace': '',
            }

    def _refresh_api_client_token(self):
        """Refreshes the existing token associated with the kfp_api_client."""
        if getattr(self, '_is_refresh_token', None):
            return

        new_token = get_gcp_access_token()
        self._existing_config.api_key['authorization'] = new_token

    def _get_config_with_default_credentials(self, config):
        """Apply default credentials to the configuration object.

        This method accepts a Configuration object and extends it with
        some default credentials interface.
        """
        # XXX: The default credentials are audience-based service account tokens
        # projected by the kubelet (ServiceAccountTokenVolumeCredentials). As we
        # implement more and more credentials, we can have some heuristic and
        # choose from a number of options.
        # See https://github.com/kubeflow/pipelines/pull/5287#issuecomment-805654121
        from kfp import auth
        credentials = auth.ServiceAccountTokenVolumeCredentials()
        config_copy = copy.deepcopy(config)

        try:
            credentials.refresh_api_key_hook(config_copy)
        except Exception:
            logging.warning("Failed to set up default credentials. Proceeding"
                            " without credentials...")
            return config

        config.refresh_api_key_hook = credentials.refresh_api_key_hook
        config.api_key_prefix['authorization'] = 'Bearer'
        config.refresh_api_key_hook(config)
        return config

    def set_user_namespace(self, namespace: str):
        """Set user namespace into local context setting file.

        This function should only be used when Kubeflow Pipelines is in the multi-user mode.

        Args:
          namespace: kubernetes namespace the user has access to.
        """
        self._context_setting['namespace'] = namespace
        if not os.path.exists(os.path.dirname(Client.LOCAL_KFP_CONTEXT)):
            os.makedirs(os.path.dirname(Client.LOCAL_KFP_CONTEXT))
        with open(Client.LOCAL_KFP_CONTEXT, 'w') as f:
            json.dump(self._context_setting, f)

    def get_kfp_healthz(self) -> kfp_server_api.ApiGetHealthzResponse:
        """Gets healthz info of KFP deployment.

        Returns:
          response: json formatted response from the healtz endpoint.
        """
        count = 0
        response = None
        max_attempts = 5
        while not response:
            count += 1
            if count > max_attempts:
                raise TimeoutError(
                    'Failed getting healthz endpoint after {} attempts.'.format(
                        max_attempts))
            try:
                response = self._healthz_api.get_healthz()
                return response
            # ApiException, including network errors, is the only type that may
            # recover after retry.
            except kfp_server_api.ApiException:
                # logging.exception also logs detailed info about the ApiException
                logging.exception(
                    'Failed to get healthz info attempt {} of 5.'.format(count))
                time.sleep(5)

    def get_user_namespace(self) -> str:
        """Get user namespace in context config.

        Returns:
          namespace: kubernetes namespace from the local context file or empty if it wasn't set.
        """
        return self._context_setting['namespace']

    def create_experiment(
            self,
            name: str,
            description: str = None,
            namespace: str = None) -> kfp_server_api.ApiExperiment:
        """Create a new experiment.

        Args:
          name: The name of the experiment.
          description: Description of the experiment.
          namespace: Kubernetes namespace where the experiment should be created.
            For single user deployment, leave it as None;
            For multi user, input a namespace where the user is authorized.

        Returns:
          An Experiment object. Most important field is id.
        """
        namespace = namespace or self.get_user_namespace()
        experiment = None
        try:
            experiment = self.get_experiment(
                experiment_name=name, namespace=namespace)
        except ValueError as error:
            # Ignore error if the experiment does not exist.
            if not str(error).startswith('No experiment is found with name'):
                raise error

        if not experiment:
            logging.info('Creating experiment {}.'.format(name))

            resource_references = []
            if namespace:
                key = kfp_server_api.models.ApiResourceKey(
                    id=namespace,
                    type=kfp_server_api.models.ApiResourceType.NAMESPACE)
                reference = kfp_server_api.models.ApiResourceReference(
                    key=key,
                    relationship=kfp_server_api.models.ApiRelationship.OWNER)
                resource_references.append(reference)

            experiment = kfp_server_api.models.ApiExperiment(
                name=name,
                description=description,
                resource_references=resource_references)
            experiment = self._experiment_api.create_experiment(body=experiment)

        if self._is_ipython():
            import IPython
            html = \
                ('<a href="%s/#/experiments/details/%s" target="_blank" >Experiment details</a>.'
                % (self._get_url_prefix(), experiment.id))
            IPython.display.display(IPython.display.HTML(html))
        return experiment

    def get_pipeline_id(self, name) -> Optional[str]:
        """Find the id of a pipeline by name.

        Args:
          name: Pipeline name.

        Returns:
          Returns the pipeline id if a pipeline with the name exists.
        """
        pipeline_filter = json.dumps({
            "predicates": [{
                "op": _FILTER_OPERATIONS["EQUALS"],
                "key": "name",
                "stringValue": name,
            }]
        })
        result = self._pipelines_api.list_pipelines(filter=pipeline_filter)
        if result.pipelines is None:
            return None
        if len(result.pipelines) == 1:
            return result.pipelines[0].id
        elif len(result.pipelines) > 1:
            raise ValueError(
                "Multiple pipelines with the name: {} found, the name needs to be unique"
                .format(name))
        return None

    def list_experiments(
            self,
            page_token='',
            page_size=10,
            sort_by='',
            namespace=None,
            filter=None) -> kfp_server_api.ApiListExperimentsResponse:
        """List experiments.

        Args:
          page_token: Token for starting of the page.
          page_size: Size of the page.
          sort_by: Can be '[field_name]', '[field_name] desc'. For example, 'name desc'.
          namespace: Kubernetes namespace where the experiment was created.
            For single user deployment, leave it as None;
            For multi user, input a namespace where the user is authorized.
          filter: A url-encoded, JSON-serialized Filter protocol buffer
            (see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto)).

        Returns:
          A response object including a list of experiments and next page token.
        """
        namespace = namespace or self.get_user_namespace()
        response = self._experiment_api.list_experiment(
            page_token=page_token,
            page_size=page_size,
            sort_by=sort_by,
            resource_reference_key_type=kfp_server_api.models.api_resource_type
            .ApiResourceType.NAMESPACE,
            resource_reference_key_id=namespace,
            filter=filter)
        return response

    def get_experiment(self,
                       experiment_id=None,
                       experiment_name=None,
                       namespace=None) -> kfp_server_api.ApiExperiment:
        """Get details of an experiment.

        Either experiment_id or experiment_name is required

        Args:
          experiment_id: Id of the experiment. (Optional)
          experiment_name: Name of the experiment. (Optional)
          namespace: Kubernetes namespace where the experiment was created.
            For single user deployment, leave it as None;
            For multi user, input the namespace where the user is authorized.

        Returns:
          A response object including details of a experiment.

        Raises:
          kfp_server_api.ApiException: If experiment is not found or None of the arguments is provided
        """
        namespace = namespace or self.get_user_namespace()
        if experiment_id is None and experiment_name is None:
            raise ValueError(
                'Either experiment_id or experiment_name is required')
        if experiment_id is not None:
            return self._experiment_api.get_experiment(id=experiment_id)
        experiment_filter = json.dumps({
            "predicates": [{
                "op": _FILTER_OPERATIONS["EQUALS"],
                "key": "name",
                "stringValue": experiment_name,
            }]
        })
        if namespace:
            result = self._experiment_api.list_experiment(
                filter=experiment_filter,
                resource_reference_key_type=kfp_server_api.models
                .api_resource_type.ApiResourceType.NAMESPACE,
                resource_reference_key_id=namespace)
        else:
            result = self._experiment_api.list_experiment(
                filter=experiment_filter)
        if not result.experiments:
            raise ValueError(
                'No experiment is found with name {}.'.format(experiment_name))
        if len(result.experiments) > 1:
            raise ValueError(
                'Multiple experiments is found with name {}.'.format(
                    experiment_name))
        return result.experiments[0]

    def archive_experiment(self, experiment_id: str):
        """Archive experiment.

        Args:
          experiment_id: id of the experiment.

        Raises:
          kfp_server_api.ApiException: If experiment is not found.
        """
        self._experiment_api.archive_experiment(experiment_id)

    def delete_experiment(self, experiment_id):
        """Delete experiment.

        Args:
          experiment_id: id of the experiment.

        Returns:
          Object. If the method is called asynchronously, returns the request thread.

        Raises:
          kfp_server_api.ApiException: If experiment is not found.
        """
        return self._experiment_api.delete_experiment(id=experiment_id)

    def _extract_pipeline_yaml(self, package_file):

        def _choose_pipeline_yaml_file(file_list) -> str:
            yaml_files = [file for file in file_list if file.endswith('.yaml')]
            if len(yaml_files) == 0:
                raise ValueError(
                    'Invalid package. Missing pipeline yaml file in the package.'
                )

            if 'pipeline.yaml' in yaml_files:
                return 'pipeline.yaml'
            else:
                if len(yaml_files) == 1:
                    return yaml_files[0]
                raise ValueError(
                    'Invalid package. There is no pipeline.yaml file and there are multiple yaml files.'
                )

        if package_file.endswith('.tar.gz') or package_file.endswith('.tgz'):
            with tarfile.open(package_file, "r:gz") as tar:
                file_names = [member.name for member in tar if member.isfile()]
                pipeline_yaml_file = _choose_pipeline_yaml_file(file_names)
                with tar.extractfile(tar.getmember(pipeline_yaml_file)) as f:
                    return yaml.safe_load(f)
        elif package_file.endswith('.zip'):
            with zipfile.ZipFile(package_file, 'r') as zip:
                pipeline_yaml_file = _choose_pipeline_yaml_file(zip.namelist())
                with zip.open(pipeline_yaml_file) as f:
                    return yaml.safe_load(f)
        elif package_file.endswith('.yaml') or package_file.endswith('.yml'):
            with open(package_file, 'r') as f:
                return yaml.safe_load(f)
        else:
            raise ValueError(
                'The package_file ' + package_file +
                ' should end with one of the following formats: [.tar.gz, .tgz, .zip, .yaml, .yml]'
            )

    def _override_caching_options(self, workflow: dict, enable_caching: bool):
        templates = workflow['spec']['templates']
        for template in templates:
            if 'metadata' in template \
               and 'labels' in template['metadata'] \
               and 'pipelines.kubeflow.org/enable_caching' in template['metadata']['labels']:
                template['metadata']['labels'][
                    'pipelines.kubeflow.org/enable_caching'] = str(
                        enable_caching).lower()

    def list_pipelines(self,
                       page_token='',
                       page_size=10,
                       sort_by='',
                       filter=None) -> kfp_server_api.ApiListPipelinesResponse:
        """List pipelines.

        Args:
          page_token: Token for starting of the page.
          page_size: Size of the page.
          sort_by: one of 'field_name', 'field_name desc'. For example, 'name desc'.
          filter: A url-encoded, JSON-serialized Filter protocol buffer
            (see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto)).

        Returns:
          A response object including a list of pipelines and next page token.
        """
        return self._pipelines_api.list_pipelines(
            page_token=page_token,
            page_size=page_size,
            sort_by=sort_by,
            filter=filter)

    # TODO: provide default namespace, similar to kubectl default namespaces.
    def run_pipeline(
        self,
        experiment_id: str,
        job_name: str,
        pipeline_package_path: Optional[str] = None,
        params: Optional[dict] = None,
        pipeline_id: Optional[str] = None,
        version_id: Optional[str] = None,
        pipeline_root: Optional[str] = None,
        enable_caching: Optional[str] = None,
        service_account: Optional[str] = None,
    ) -> kfp_server_api.ApiRun:
        """Run a specified pipeline.

        Args:
          experiment_id: The id of an experiment.
          job_name: Name of the job.
          pipeline_package_path: Local path of the pipeline package(the filename should end with one of the following .tar.gz, .tgz, .zip, .yaml, .yml).
          params: A dictionary with key (string) as param name and value (string) as as param value.
          pipeline_id: The id of a pipeline.
          version_id: The id of a pipeline version.
            If both pipeline_id and version_id are specified, version_id will take precendence.
            If only pipeline_id is specified, the default version of this pipeline is used to create the run.
          pipeline_root: The root path of the pipeline outputs. This argument should
            be used only for pipeline compiled with
            dsl.PipelineExecutionMode.V2_COMPATIBLE or
            dsl.PipelineExecutionMode.V2_ENGINGE mode.
          enable_caching: Optional. Whether or not to enable caching for the run.
            This setting affects v2 compatible mode and v2 mode only.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          service_account: Optional. Specifies which Kubernetes service account this
            run uses.

        Returns:
          A run object. Most important field is id.
        """
        if params is None:
            params = {}

        if pipeline_root is not None:
            params[dsl.ROOT_PARAMETER_NAME] = pipeline_root

        job_config = self._create_job_config(
            experiment_id=experiment_id,
            params=params,
            pipeline_package_path=pipeline_package_path,
            pipeline_id=pipeline_id,
            version_id=version_id,
            enable_caching=enable_caching,
        )
        run_body = kfp_server_api.models.ApiRun(
            pipeline_spec=job_config.spec,
            resource_references=job_config.resource_references,
            name=job_name,
            service_account=service_account)

        response = self._run_api.create_run(body=run_body)

        if self._is_ipython():
            import IPython
            html = (
                '<a href="%s/#/runs/details/%s" target="_blank" >Run details</a>.'
                % (self._get_url_prefix(), response.run.id))
            IPython.display.display(IPython.display.HTML(html))
        return response.run

    def create_recurring_run(
        self,
        experiment_id: str,
        job_name: str,
        description: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval_second: Optional[int] = None,
        cron_expression: Optional[str] = None,
        max_concurrency: Optional[int] = 1,
        no_catchup: Optional[bool] = None,
        params: Optional[dict] = None,
        pipeline_package_path: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        version_id: Optional[str] = None,
        enabled: bool = True,
        enable_caching: Optional[bool] = None,
        service_account: Optional[str] = None,
    ) -> kfp_server_api.ApiJob:
        """Create a recurring run.

        Args:
          experiment_id: The string id of an experiment.
          job_name: Name of the job.
          description: An optional job description.
          start_time: The RFC3339 time string of the time when to start the job.
          end_time: The RFC3339 time string of the time when to end the job.
          interval_second: Integer indicating the seconds between two recurring runs in for a periodic schedule.
          cron_expression: A cron expression representing a set of times, using 6 space-separated fields, e.g. "0 0 9 ? * 2-6".
            See `here <https://pkg.go.dev/github.com/robfig/cron#hdr-CRON_Expression_Format>`_ for details of the cron expression format.
          max_concurrency: Integer indicating how many jobs can be run in parallel.
          no_catchup: Whether the recurring run should catch up if behind schedule.
            For example, if the recurring run is paused for a while and re-enabled
            afterwards. If no_catchup=False, the scheduler will catch up on (backfill) each
            missed interval. Otherwise, it only schedules the latest interval if more than one interval
            is ready to be scheduled.
            Usually, if your pipeline handles backfill internally, you should turn catchup
            off to avoid duplicate backfill. (default: {False})
          pipeline_package_path: Local path of the pipeline package(the filename should end with one of the following .tar.gz, .tgz, .zip, .yaml, .yml).
          params: A dictionary with key (string) as param name and value (string) as param value.
          pipeline_id: The id of a pipeline.
          version_id: The id of a pipeline version.
            If both pipeline_id and version_id are specified, version_id will take precendence.
            If only pipeline_id is specified, the default version of this pipeline is used to create the run.
          enabled: A bool indicating whether the recurring run is enabled or disabled.
          enable_caching: Optional. Whether or not to enable caching for the run.
            This setting affects v2 compatible mode and v2 mode only.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          service_account: Optional. Specifies which Kubernetes service account this
            recurring run uses.

        Returns:
          A Job object. Most important field is id.

        Raises:
          ValueError: If required parameters are not supplied.
        """

        job_config = self._create_job_config(
            experiment_id=experiment_id,
            params=params,
            pipeline_package_path=pipeline_package_path,
            pipeline_id=pipeline_id,
            version_id=version_id,
            enable_caching=enable_caching,
        )

        if all([interval_second, cron_expression
               ]) or not any([interval_second, cron_expression]):
            raise ValueError(
                'Either interval_second or cron_expression is required')
        if interval_second is not None:
            trigger = kfp_server_api.models.ApiTrigger(
                periodic_schedule=kfp_server_api.models.ApiPeriodicSchedule(
                    start_time=start_time,
                    end_time=end_time,
                    interval_second=interval_second))
        if cron_expression is not None:
            trigger = kfp_server_api.models.ApiTrigger(
                cron_schedule=kfp_server_api.models.ApiCronSchedule(
                    start_time=start_time,
                    end_time=end_time,
                    cron=cron_expression))

        job_body = kfp_server_api.models.ApiJob(
            enabled=enabled,
            pipeline_spec=job_config.spec,
            resource_references=job_config.resource_references,
            name=job_name,
            description=description,
            no_catchup=no_catchup,
            trigger=trigger,
            max_concurrency=max_concurrency,
            service_account=service_account)
        return self._job_api.create_job(body=job_body)

    def _create_job_config(
        self,
        experiment_id: str,
        params: Optional[dict],
        pipeline_package_path: Optional[str],
        pipeline_id: Optional[str],
        version_id: Optional[str],
        enable_caching: Optional[bool],
    ):
        """Create a JobConfig with spec and resource_references.

        Args:
          experiment_id: The id of an experiment.
          pipeline_package_path: Local path of the pipeline package(the filename should end with one of the following .tar.gz, .tgz, .zip, .yaml, .yml).
          params: A dictionary with key (string) as param name and value (string) as param value.
          pipeline_id: The id of a pipeline.
          version_id: The id of a pipeline version.
            If both pipeline_id and version_id are specified, version_id will take precendence.
            If only pipeline_id is specified, the default version of this pipeline is used to create the run.
          enable_caching: Whether or not to enable caching for the run.
            This setting affects v2 compatible mode and v2 mode only.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.

        Returns:
          A JobConfig object with attributes spec and resource_reference.
        """

        class JobConfig:

            def __init__(self, spec, resource_references):
                self.spec = spec
                self.resource_references = resource_references

        params = params or {}
        pipeline_json_string = None
        if pipeline_package_path:
            pipeline_obj = self._extract_pipeline_yaml(pipeline_package_path)

            # Caching option set at submission time overrides the compile time settings.
            if enable_caching is not None:
                self._override_caching_options(pipeline_obj, enable_caching)

            pipeline_json_string = json.dumps(pipeline_obj)
        api_params = [
            kfp_server_api.ApiParameter(
                name=sanitize_k8s_name(name=k, allow_capital_underscore=True),
                value=str(v) if type(v) not in (list, dict) else json.dumps(v))
            for k, v in params.items()
        ]
        resource_references = []
        key = kfp_server_api.models.ApiResourceKey(
            id=experiment_id,
            type=kfp_server_api.models.ApiResourceType.EXPERIMENT)
        reference = kfp_server_api.models.ApiResourceReference(
            key=key, relationship=kfp_server_api.models.ApiRelationship.OWNER)
        resource_references.append(reference)

        if version_id:
            key = kfp_server_api.models.ApiResourceKey(
                id=version_id,
                type=kfp_server_api.models.ApiResourceType.PIPELINE_VERSION)
            reference = kfp_server_api.models.ApiResourceReference(
                key=key,
                relationship=kfp_server_api.models.ApiRelationship.CREATOR)
            resource_references.append(reference)

        spec = kfp_server_api.models.ApiPipelineSpec(
            pipeline_id=pipeline_id,
            workflow_manifest=pipeline_json_string,
            parameters=api_params)
        return JobConfig(spec=spec, resource_references=resource_references)

    def create_run_from_pipeline_func(
        self,
        pipeline_func: Callable,
        arguments: Mapping[str, str],
        run_name: Optional[str] = None,
        experiment_name: Optional[str] = None,
        pipeline_conf: Optional[dsl.PipelineConf] = None,
        namespace: Optional[str] = None,
        mode: dsl.PipelineExecutionMode = dsl.PipelineExecutionMode.V1_LEGACY,
        launcher_image: Optional[str] = None,
        pipeline_root: Optional[str] = None,
        enable_caching: Optional[bool] = None,
        service_account: Optional[str] = None,
    ):
        """Runs pipeline on KFP-enabled Kubernetes cluster.

        This command compiles the pipeline function, creates or gets an experiment and submits the pipeline for execution.

        Args:
          pipeline_func: A function that describes a pipeline by calling components and composing them into execution graph.
          arguments: Arguments to the pipeline function provided as a dict.
          run_name: Optional. Name of the run to be shown in the UI.
          experiment_name: Optional. Name of the experiment to add the run to.
          pipeline_conf: Optional. Pipeline configuration ops that will be applied
            to all the ops in the pipeline func.
          namespace: Kubernetes namespace where the pipeline runs are created.
            For single user deployment, leave it as None;
            For multi user, input a namespace where the user is authorized
          mode: The PipelineExecutionMode to use when compiling and running
            pipeline_func.
          launcher_image: The launcher image to use if the mode is specified as
            PipelineExecutionMode.V2_COMPATIBLE. Should only be needed for tests
            or custom deployments right now.
          pipeline_root: The root path of the pipeline outputs. This argument should
            be used only for pipeline compiled with
            dsl.PipelineExecutionMode.V2_COMPATIBLE or
            dsl.PipelineExecutionMode.V2_ENGINGE mode.
          enable_caching: Optional. Whether or not to enable caching for the run.
            This setting affects v2 compatible mode and v2 mode only.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          service_account: Optional. Specifies which Kubernetes service account this
            run uses.
        """
        if pipeline_root is not None and mode == dsl.PipelineExecutionMode.V1_LEGACY:
            raise ValueError('`pipeline_root` should not be used with '
                             'dsl.PipelineExecutionMode.V1_LEGACY mode.')

        #TODO: Check arguments against the pipeline function
        pipeline_name = pipeline_func.__name__
        run_name = run_name or pipeline_name + ' ' + datetime.datetime.now(
        ).strftime('%Y-%m-%d %H-%M-%S')
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline_package_path = os.path.join(tmpdir, 'pipeline.yaml')
            compiler.Compiler(
                mode=mode, launcher_image=launcher_image).compile(
                    pipeline_func=pipeline_func,
                    package_path=pipeline_package_path,
                    pipeline_conf=pipeline_conf)

            return self.create_run_from_pipeline_package(
                pipeline_file=pipeline_package_path,
                arguments=arguments,
                run_name=run_name,
                experiment_name=experiment_name,
                namespace=namespace,
                pipeline_root=pipeline_root,
                enable_caching=enable_caching,
                service_account=service_account,
            )

    def create_run_from_pipeline_package(
        self,
        pipeline_file: str,
        arguments: Mapping[str, str],
        run_name: Optional[str] = None,
        experiment_name: Optional[str] = None,
        namespace: Optional[str] = None,
        pipeline_root: Optional[str] = None,
        enable_caching: Optional[bool] = None,
        service_account: Optional[str] = None,
    ):
        """Runs pipeline on KFP-enabled Kubernetes cluster.

        This command takes a local pipeline package, creates or gets an experiment
        and submits the pipeline for execution.

        Args:
          pipeline_file: A compiled pipeline package file.
          arguments: Arguments to the pipeline function provided as a dict.
          run_name: Optional. Name of the run to be shown in the UI.
          experiment_name: Optional. Name of the experiment to add the run to.
          namespace: Kubernetes namespace where the pipeline runs are created.
            For single user deployment, leave it as None;
            For multi user, input a namespace where the user is authorized
          pipeline_root: The root path of the pipeline outputs. This argument should
            be used only for pipeline compiled with
            dsl.PipelineExecutionMode.V2_COMPATIBLE or
            dsl.PipelineExecutionMode.V2_ENGINGE mode.
          enable_caching: Optional. Whether or not to enable caching for the run.
            This setting affects v2 compatible mode and v2 mode only.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          service_account: Optional. Specifies which Kubernetes service account this
            run uses.
        """

        class RunPipelineResult:

            def __init__(self, client, run_info):
                self._client = client
                self.run_info = run_info
                self.run_id = run_info.id

            def wait_for_run_completion(self, timeout=None):
                timeout = timeout or datetime.timedelta.max
                return self._client.wait_for_run_completion(
                    self.run_id, timeout)

            def __repr__(self):
                return 'RunPipelineResult(run_id={})'.format(self.run_id)

        #TODO: Check arguments against the pipeline function
        pipeline_name = os.path.basename(pipeline_file)
        experiment_name = experiment_name or os.environ.get(
            KF_PIPELINES_DEFAULT_EXPERIMENT_NAME, None)
        overridden_experiment_name = os.environ.get(
            KF_PIPELINES_OVERRIDE_EXPERIMENT_NAME, experiment_name)
        if overridden_experiment_name != experiment_name:
            import warnings
            warnings.warn('Changing experiment name from "{}" to "{}".'.format(
                experiment_name, overridden_experiment_name))
        experiment_name = overridden_experiment_name or 'Default'
        run_name = run_name or (
            pipeline_name + ' ' +
            datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
        experiment = self.create_experiment(
            name=experiment_name, namespace=namespace)
        run_info = self.run_pipeline(
            experiment_id=experiment.id,
            job_name=run_name,
            pipeline_package_path=pipeline_file,
            params=arguments,
            pipeline_root=pipeline_root,
            enable_caching=enable_caching,
            service_account=service_account,
        )
        return RunPipelineResult(self, run_info)

    def delete_job(self, job_id: str):
        """Deletes a job.

        Args:
          job_id: id of the job.

        Returns:
          Object. If the method is called asynchronously, returns the request thread.

        Raises:
          kfp_server_api.ApiException: If the job is not found.
        """
        return self._job_api.delete_job(id=job_id)

    def disable_job(self, job_id: str):
        """Disables a job.

        Args:
          job_id: id of the job.

        Returns:
          Object. If the method is called asynchronously, returns the request thread.

        Raises:
          ApiException: If the job is not found.
        """
        return self._job_api.disable_job(id=job_id)

    def list_runs(self,
                  page_token='',
                  page_size=10,
                  sort_by='',
                  experiment_id=None,
                  namespace=None,
                  filter=None) -> kfp_server_api.ApiListRunsResponse:
        """List runs, optionally can be filtered by experiment or namespace.

        Args:
          page_token: Token for starting of the page.
          page_size: Size of the page.
          sort_by: One of 'field_name', 'field_name desc'. For example, 'name desc'.
          experiment_id: Experiment id to filter upon
          namespace: Kubernetes namespace to filter upon.
            For single user deployment, leave it as None;
            For multi user, input a namespace where the user is authorized.
          filter: A url-encoded, JSON-serialized Filter protocol buffer
            (see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto)).

        Returns:
          A response object including a list of experiments and next page token.
        """
        namespace = namespace or self.get_user_namespace()
        if experiment_id is not None:
            response = self._run_api.list_runs(
                page_token=page_token,
                page_size=page_size,
                sort_by=sort_by,
                resource_reference_key_type=kfp_server_api.models
                .api_resource_type.ApiResourceType.EXPERIMENT,
                resource_reference_key_id=experiment_id,
                filter=filter)
        elif namespace:
            response = self._run_api.list_runs(
                page_token=page_token,
                page_size=page_size,
                sort_by=sort_by,
                resource_reference_key_type=kfp_server_api.models
                .api_resource_type.ApiResourceType.NAMESPACE,
                resource_reference_key_id=namespace,
                filter=filter)
        else:
            response = self._run_api.list_runs(
                page_token=page_token,
                page_size=page_size,
                sort_by=sort_by,
                filter=filter)
        return response

    def list_recurring_runs(self,
                            page_token='',
                            page_size=10,
                            sort_by='',
                            experiment_id=None,
                            filter=None) -> kfp_server_api.ApiListJobsResponse:
        """List recurring runs.

        Args:
          page_token: Token for starting of the page.
          page_size: Size of the page.
          sort_by: One of 'field_name', 'field_name desc'. For example, 'name desc'.
          experiment_id: Experiment id to filter upon.
          filter: A url-encoded, JSON-serialized Filter protocol buffer
            (see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto)).

        Returns:
          A response object including a list of recurring_runs and next page token.
        """
        if experiment_id is not None:
            response = self._job_api.list_jobs(
                page_token=page_token,
                page_size=page_size,
                sort_by=sort_by,
                resource_reference_key_type=kfp_server_api.models
                .api_resource_type.ApiResourceType.EXPERIMENT,
                resource_reference_key_id=experiment_id,
                filter=filter)
        else:
            response = self._job_api.list_jobs(
                page_token=page_token,
                page_size=page_size,
                sort_by=sort_by,
                filter=filter)
        return response

    def get_recurring_run(self, job_id: str) -> kfp_server_api.ApiJob:
        """Get recurring_run details.

        Args:
          job_id: id of the recurring_run.

        Returns:
          A response object including details of a recurring_run.

        Raises:
          kfp_server_api.ApiException: If recurring_run is not found.
        """
        return self._job_api.get_job(id=job_id)

    def get_run(self, run_id: str) -> kfp_server_api.ApiRun:
        """Get run details.

        Args:
          run_id: id of the run.

        Returns:
          A response object including details of a run.

        Raises:
          kfp_server_api.ApiException: If run is not found.
        """
        return self._run_api.get_run(run_id=run_id)

    def wait_for_run_completion(self, run_id: str, timeout: int):
        """Waits for a run to complete.

        Args:
          run_id: Run id, returned from run_pipeline.
          timeout: Timeout in seconds.

        Returns:
          A run detail object: Most important fields are run and pipeline_runtime.

        Raises:
          TimeoutError: if the pipeline run failed to finish before the specified timeout.
        """
        status = 'Running:'
        start_time = datetime.datetime.now()
        if isinstance(timeout, datetime.timedelta):
            timeout = timeout.total_seconds()
        is_valid_token = False
        while (status is None or status.lower()
               not in ['succeeded', 'failed', 'skipped', 'error']):
            try:
                get_run_response = self._run_api.get_run(run_id=run_id)
                is_valid_token = True
            except ApiException as api_ex:
                # if the token is valid but receiving 401 Unauthorized error
                # then refresh the token
                if is_valid_token and api_ex.status == 401:
                    logging.info('Access token has expired !!! Refreshing ...')
                    self._refresh_api_client_token()
                    continue
                else:
                    raise api_ex
            status = get_run_response.run.status
            elapsed_time = (datetime.datetime.now() -
                            start_time).total_seconds()
            logging.info('Waiting for the job to complete...')
            if elapsed_time > timeout:
                raise TimeoutError('Run timeout')
            time.sleep(5)
        return get_run_response

    def _get_workflow_json(self, run_id):
        """Get the workflow json.

        Args:
          run_id: run id, returned from run_pipeline.

        Returns:
          workflow: Json workflow
        """
        get_run_response = self._run_api.get_run(run_id=run_id)
        workflow = get_run_response.pipeline_runtime.workflow_manifest
        workflow_json = json.loads(workflow)
        return workflow_json

    def upload_pipeline(
        self,
        pipeline_package_path: str = None,
        pipeline_name: str = None,
        description: str = None,
    ) -> kfp_server_api.ApiPipeline:
        """Uploads the pipeline to the Kubeflow Pipelines cluster.

        Args:
          pipeline_package_path: Local path to the pipeline package.
          pipeline_name: Optional. Name of the pipeline to be shown in the UI.
          description: Optional. Description of the pipeline to be shown in the UI.

        Returns:
          Server response object containing pipleine id and other information.
        """

        response = self._upload_api.upload_pipeline(
            pipeline_package_path, name=pipeline_name, description=description)
        if self._is_ipython():
            import IPython
            html = '<a href=%s/#/pipelines/details/%s>Pipeline details</a>.' % (
                self._get_url_prefix(), response.id)
            IPython.display.display(IPython.display.HTML(html))
        return response

    def upload_pipeline_version(
        self,
        pipeline_package_path,
        pipeline_version_name: str,
        pipeline_id: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> kfp_server_api.ApiPipelineVersion:
        """Uploads a new version of the pipeline to the Kubeflow Pipelines cluster.

        Args:
          pipeline_package_path: Local path to the pipeline package.
          pipeline_version_name:  Name of the pipeline version to be shown in the UI.
          pipeline_id: Optional. Id of the pipeline.
          pipeline_name: Optional. Name of the pipeline.
          description: Optional. Description of the pipeline version to be shown in the UI.

        Returns:
          Server response object containing pipleine id and other information.

        Raises:
          ValueError when none or both of pipeline_id or pipeline_name are specified
          kfp_server_api.ApiException: If pipeline id is not found.
        """

        if all([pipeline_id, pipeline_name
               ]) or not any([pipeline_id, pipeline_name]):
            raise ValueError('Either pipeline_id or pipeline_name is required')

        if pipeline_name:
            pipeline_id = self.get_pipeline_id(pipeline_name)
        kwargs = dict(
            name=pipeline_version_name,
            pipelineid=pipeline_id,
        )

        if description:
            kwargs['description'] = description
        try:
            response = self._upload_api.upload_pipeline_version(
                pipeline_package_path, **kwargs)
        except kfp_server_api.exceptions.ApiTypeError as e:
            # ToDo: Remove this once we drop support for kfp_server_api < 1.7.1
            if 'description' in e.message and 'unexpected keyword argument' in e.message:
                raise NotImplementedError(
                    'Pipeline version description is not supported in current kfp-server-api pypi package. Upgrade to 1.7.1 or above'
                )
            else:
                raise e

        if self._is_ipython():
            import IPython
            html = '<a href=%s/#/pipelines/details/%s>Pipeline details</a>.' % (
                self._get_url_prefix(), response.id)
            IPython.display.display(IPython.display.HTML(html))
        return response

    def get_pipeline(self, pipeline_id: str) -> kfp_server_api.ApiPipeline:
        """Get pipeline details.

        Args:
          pipeline_id: id of the pipeline.

        Returns:
          A response object including details of a pipeline.

        Raises:
          kfp_server_api.ApiException: If pipeline is not found.
        """
        return self._pipelines_api.get_pipeline(id=pipeline_id)

    def delete_pipeline(self, pipeline_id):
        """Delete pipeline.

        Args:
          pipeline_id: id of the pipeline.

        Returns:
          Object. If the method is called asynchronously, returns the request thread.

        Raises:
          kfp_server_api.ApiException: If pipeline is not found.
        """
        return self._pipelines_api.delete_pipeline(id=pipeline_id)

    def list_pipeline_versions(
            self,
            pipeline_id: str,
            page_token: str = '',
            page_size: int = 10,
            sort_by: str = '',
            filter: str = None
    ) -> kfp_server_api.ApiListPipelineVersionsResponse:
        """Lists pipeline versions.

        Args:
          pipeline_id: Id of the pipeline to list versions
          page_token: Token for starting of the page.
          page_size: Size of the page.
          sort_by: One of 'field_name', 'field_name desc'. For example, 'name desc'.
          filter: A url-encoded, JSON-serialized Filter protocol buffer
            (see [filter.proto](https://github.com/kubeflow/pipelines/blob/master/backend/api/filter.proto)).

        Returns:
          A response object including a list of versions and next page token.

        Raises:
          kfp_server_api.ApiException: If pipeline is not found.
        """

        return self._pipelines_api.list_pipeline_versions(
            page_token=page_token,
            page_size=page_size,
            sort_by=sort_by,
            resource_key_type=kfp_server_api.models.api_resource_type
            .ApiResourceType.PIPELINE,
            resource_key_id=pipeline_id,
            filter=filter)

    def delete_pipeline_version(self, version_id: str):
        """Delete pipeline version.

        Args:
          version_id: id of the pipeline version.

        Returns:
          Object. If the method is called asynchronously, returns the request thread.

        Raises:
          Exception if pipeline version is not found.
        """
        return self._pipelines_api.delete_pipeline_version(
            version_id=version_id)
