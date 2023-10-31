# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module for AIPlatformPipelines API client."""

import datetime
import json
import pkg_resources
import re
import subprocess
import warnings
from typing import Any, Dict, List, Mapping, Optional

from absl import logging
from google import auth
from google.auth import exceptions
from google.oauth2 import credentials
from google.protobuf import json_format
from googleapiclient import discovery

from kfp.v2.google.client import client_utils
from kfp.v2.google.client import runtime_config_builder
from kfp.v2.google.client.schedule import _create_from_pipeline_dict

# AIPlatformPipelines API endpoint.
DEFAULT_ENDPOINT_FORMAT = '{region}-aiplatform.googleapis.com'

# AIPlatformPipelines API version.
DEFAULT_API_VERSION = 'v1beta1'

# If application default credential does not exist, we fall back to whatever
# previously provided to `gcloud auth`. One can run `gcloud auth login` to
# provide identity for this token.
_AUTH_ARGS = ('gcloud', 'auth', 'print-access-token')

# AIPlatformPipelines service API parent pattern.
_PARENT_PATTERN = 'projects/{}/locations/{}'

# AIPlatformPipelines service API job name relative name prefix pattern.
_JOB_NAME_PATTERN = '{parent}/pipelineJobs/{job_id}'

# Pattern for valid names used as a uCAIP resource name.
_VALID_NAME_PATTERN = re.compile('^[a-z][-a-z0-9]{0,127}$')

# Cloud Console UI link of a pipeline run.
UI_PIPELINE_JOB_LINK_FORMAT = (
    'https://console.cloud.google.com/vertex-ai/locations/{region}/pipelines/'
    'runs/{job_id}?project={project_id}')

# Display UI link HTML snippet
UI_LINK_HTML_FORMAT = (
    'See the Pipeline job <a href="{}" target="_blank" >here</a>.')


class _AccessTokenCredentials(credentials.Credentials):
    """Credential class to provide token-based authentication."""

    def __init__(self, token):
        super().__init__(token=token)

    def refresh(self, request):
        # Overrides refresh method in the base class because by default token is
        # not refreshable.
        pass


def _get_gcp_credential() -> Optional[credentials.Credentials]:
    """Returns the GCP OAuth2 credential.

    Returns:
      The credential. By default the function returns the current Application
      Default Credentials, which is located at $GOOGLE_APPLICATION_CREDENTIALS. If
      not set, this function returns the current login credential, whose token is
      created by running 'gcloud auth login'.
      For more information, see
      https://cloud.google.com/sdk/gcloud/reference/auth/application-default/print-access-token
    """
    result = None
    try:
        result, _ = auth.default()
    except exceptions.DefaultCredentialsError as e:
        logging.warning(
            'Failed to get GCP access token for application '
            'default credential: (%s). Using end user credential.', e)
        try:
            token = subprocess.check_output(_AUTH_ARGS).rstrip().decode('utf-8')
            result = _AccessTokenCredentials(token=token)
        except subprocess.CalledProcessError as e:
            # _AccessTokenCredentials won't throw CalledProcessError, so this
            # exception implies that the subprocess call is failed.
            logging.warning('Failed to get GCP access token: %s', e)
    else:
        return result


def _get_current_time() -> datetime.datetime:
    """Gets the current timestamp."""
    return datetime.datetime.now()


def _is_ipython() -> bool:
    """Returns whether we are running in notebook."""
    try:
        import IPython  # pylint: disable=g-import-not-at-top
        ipy = IPython.get_ipython()
        if ipy is None:
            return False
    except ImportError:
        return False

    return True


def _extract_job_id(job_name: str) -> Optional[str]:
    """Extracts job id from job name.

    Args:
     job_name: The full job name.

    Returns:
     The job id or None if no match found.
    """
    p = re.compile(
        'projects/(?P<project_id>.*)/locations/(?P<region>.*)/pipelineJobs/(?P<job_id>.*)'
    )
    result = p.search(job_name)
    return result.group('job_id') if result else None


def _set_enable_caching_value(pipeline_spec: Dict[str, Any],
                              enable_caching: bool) -> None:
    """Sets pipeline tasks caching options.

    Args:
     pipeline_spec: The dictionary of pipeline spec.
     enable_caching: Whether to enable caching.
    """
    for component in [pipeline_spec['root']] + list(
            pipeline_spec['components'].values()):
        if 'dag' in component:
            for task in component['dag']['tasks'].values():
                task['cachingOptions'] = {'enableCache': enable_caching}


class AIPlatformClient(object):
    """AIPlatformPipelines Unified API Client."""

    def __init__(
        self,
        project_id: str,
        region: str,
    ):
        """Constructs an AIPlatformPipelines API client.

        Args:
          project_id: GCP project ID.
          region: GCP project region.
        """
        warnings.warn(
            'AIPlatformClient will be deprecated in v2.0.0. Please use PipelineJob'
            ' https://googleapis.dev/python/aiplatform/latest/_modules/google/cloud/aiplatform/pipeline_jobs.html'
            ' in Vertex SDK. Install the SDK using "pip install google-cloud-aiplatform"',
            category=FutureWarning,
        )

        if not project_id:
            raise ValueError(
                'A valid GCP project ID is required to run a pipeline.')
        if not region:
            raise ValueError(
                'A valid GCP region is required to run a pipeline.')

        self._endpoint = DEFAULT_ENDPOINT_FORMAT.format(region=region)
        self._api_version = DEFAULT_API_VERSION

        self._project_id = project_id
        self._region = region
        self._parent = _PARENT_PATTERN.format(project_id, region)

        discovery_doc_path = pkg_resources.resource_filename(
            'kfp.v2.google.client',
            'discovery/aiplatform_public_google_rest_v1beta1.json')

        with open(discovery_doc_path) as f:
            discovery_doc = f.read()

        self._api_client = discovery.build_from_document(
            service=discovery_doc,
            http=None,
            credentials=_get_gcp_credential(),
        )
        self._api_client._baseUrl = 'https://{}'.format(self._endpoint)  # pylint:disable=protected-access

    def _display_job_link(self, job_id: str):
        """Display an link to UI."""
        url = UI_PIPELINE_JOB_LINK_FORMAT.format(
            region=self._region, job_id=job_id, project_id=self._project_id)
        if _is_ipython():
            import IPython  # pylint: disable=g-import-not-at-top
            html = UI_LINK_HTML_FORMAT.format(url)
            IPython.display.display(IPython.display.HTML(html))
        else:
            print('See the Pipeline job here:', url)

    def _submit_job(
        self,
        job_spec: Mapping[str, Any],
        job_id: Optional[str] = None,
    ) -> dict:
        """Submits a pipeline job to run on AIPlatformPipelines service.

        Args:
          job_spec: AIPlatformPipelines pipelineJob spec.
          job_id: Optional user-specified ID of this pipelineJob. If not provided,
            pipeline service will automatically generate a random numeric ID.

        Returns:
          The service returned PipelineJob instance.

        Raises:
          RuntimeError: If AIPlatformPipelines service returns unexpected response
          or empty job name.
        """
        request = self._api_client.projects().locations().pipelineJobs().create(
            body=job_spec, pipelineJobId=job_id, parent=self._parent)
        response = request.execute()
        job_id = _extract_job_id(response['name'])
        if job_id:
            self._display_job_link(job_id)
        return response

    def get_job(self, job_id: str) -> dict:
        """Gets an existing pipeline job on AIPlatformPipelines service.

        Args:
          job_id: The relative ID of this pipelineJob. The full qualified name will
            generated according to the project ID and region specified for the
            client.

        Returns:
          The JSON-formatted response from service representing the pipeline job.
        """
        full_name = _JOB_NAME_PATTERN.format(parent=self._parent, job_id=job_id)
        return self._api_client.projects().locations().pipelineJobs().get(
            name=full_name).execute()

    def list_jobs(self) -> dict:
        """Lists existing pipeline jobs on AIPlatformPipelines service.

        Returns:
          The JSON-formatted response from service representing all pipeline jobs.
        """
        return self._api_client.projects().locations().pipelineJobs().list(
            parent=self._parent).execute()

    def create_run_from_job_spec(
            self,
            job_spec_path: str,
            job_id: Optional[str] = None,
            pipeline_root: Optional[str] = None,
            parameter_values: Optional[Mapping[str, Any]] = None,
            enable_caching: Optional[bool] = None,
            cmek: Optional[str] = None,
            service_account: Optional[str] = None,
            network: Optional[str] = None,
            labels: Optional[Mapping[str, str]] = None) -> dict:
        """Runs a pre-compiled pipeline job on AIPlatformPipelines service.

        Args:
          job_spec_path: The path of PipelineJob JSON file. It can be a local path
            or a GS URI.
          job_id: Optionally, the user can provide the unique ID of the job run. If
            not specified, pipeline name + timestamp will be used.
          pipeline_root: Optionally the user can override the pipeline root
            specified during the compile time.
          parameter_values: The mapping from runtime parameter names to its values.
          enable_caching: Whether or not to enable caching for the run.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          cmek: The customer-managed encryption key for a pipelineJob. If set, the
            pipeline job and all of its sub-resources will be secured by this key.
          service_account: The service account that the pipeline workload runs as.
          network: The network configuration applied for pipeline jobs. If left
            unspecified, the workload is not peered with any network.
          labels: The user defined metadata to organize PipelineJob.

        Returns:
          Full AIPlatformPipelines job name.

        Raises:
          ParseError: On JSON parsing problems.
          RuntimeError: If AIPlatformPipelines service returns unexpected response
          or empty job name.
        """
        job_spec = client_utils.load_json(job_spec_path)
        pipeline_name = job_spec['pipelineSpec']['pipelineInfo']['name']
        job_id = job_id or '{pipeline_name}-{timestamp}'.format(
            pipeline_name=re.sub('[^-0-9a-z]+', '-',
                                 pipeline_name.lower()).lstrip('-').rstrip('-'),
            timestamp=_get_current_time().strftime('%Y%m%d%H%M%S'))
        if not _VALID_NAME_PATTERN.match(job_id):
            raise ValueError(
                'Generated job ID: {} is illegal as a uCAIP pipelines job ID. '
                'Expecting an ID following the regex pattern '
                '"[a-z][-a-z0-9]{{0,127}}"'.format(job_id))

        job_name = _JOB_NAME_PATTERN.format(parent=self._parent, job_id=job_id)

        job_spec['name'] = job_name
        job_spec['displayName'] = job_id

        builder = runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
            job_spec)
        builder.update_pipeline_root(pipeline_root)
        builder.update_runtime_parameters(parameter_values)

        runtime_config = builder.build()
        job_spec['runtimeConfig'] = runtime_config

        if enable_caching is not None:
            _set_enable_caching_value(job_spec['pipelineSpec'], enable_caching)

        if cmek is not None:
            job_spec['encryptionSpec'] = {'kmsKeyName': cmek}
        if service_account is not None:
            job_spec['serviceAccount'] = service_account
        if network is not None:
            job_spec['network'] = network

        if labels:
            if not isinstance(labels, Mapping):
                raise ValueError(
                    'Expect labels to be a mapping of string key value pairs. '
                    'Got "{}" of type "{}"'.format(labels, type(labels)))
            for k, v in labels.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise ValueError(
                        'Expect labels to be a mapping of string key value pairs. '
                        'Got "{}".'.format(labels))

            job_spec['labels'] = labels

        return self._submit_job(
            job_spec=job_spec,
            job_id=job_id,
        )

    def create_schedule_from_job_spec(
        self,
        job_spec_path: str,
        schedule: str,
        time_zone: str = 'US/Pacific',
        pipeline_root: Optional[str] = None,
        parameter_values: Optional[Mapping[str, Any]] = None,
        service_account: Optional[str] = None,
        enable_caching: Optional[bool] = None,
        app_engine_region: Optional[str] = None,
        cloud_scheduler_service_account: Optional[str] = None,
    ) -> dict:
        """Creates schedule for compiled pipeline file.

        This function creates scheduled job which will run the provided pipeline on
        schedule. This is implemented by creating a Google Cloud Scheduler Job.
        The job will be visible in https://console.google.com/cloudscheduler and can
        be paused/resumed and deleted.

        To make the system work, this function also creates a Google Cloud Function
        which acts as an intermediary between the Scheduler and Pipelines. A single
        function is shared between all scheduled jobs.
        The following APIs will be activated automatically:
        * cloudfunctions.googleapis.com
        * cloudscheduler.googleapis.com
        * appengine.googleapis.com

        Args:
          job_spec_path: Path of the compiled pipeline file.
          schedule: Schedule in cron format. Example: "45 * * * *"
          time_zone: Schedule time zone. Default is 'US/Pacific'
          parameter_values: Arguments for the pipeline parameters
          pipeline_root: Optionally the user can override the pipeline root
            specified during the compile time.
          service_account: The service account that the pipeline workload runs as.
          enable_caching: Whether or not to enable caching for the run.
            If not set, defaults to the compile time settings, which are True for all
            tasks by default, while users may specify different caching options for
            individual tasks.
            If set, the setting applies to all tasks in the pipeline -- overrides
            the compile time settings.
          app_engine_region: The region that cloud scheduler job is created in.
          cloud_scheduler_service_account: The service account that Cloud Scheduler job and the proxy cloud function use.
            this should have permission to call AI Platform API and the proxy function.
            If not specified, the functions uses the App Engine default service account.

        Returns:
          Created Google Cloud Scheduler Job object dictionary.
        """
        job_spec = client_utils.load_json(job_spec_path)

        if enable_caching is not None:
            _set_enable_caching_value(job_spec['pipelineSpec'], enable_caching)

        return _create_from_pipeline_dict(
            pipeline_dict=job_spec,
            schedule=schedule,
            project_id=self._project_id,
            region=self._region,
            time_zone=time_zone,
            parameter_values=parameter_values,
            pipeline_root=pipeline_root,
            service_account=service_account,
            app_engine_region=app_engine_region,
            cloud_scheduler_service_account=cloud_scheduler_service_account)
