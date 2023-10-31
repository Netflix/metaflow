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
"""Module for scheduling pipeline runs."""

import base64
import hashlib
import json
import logging
import pathlib
import re
import tempfile
from typing import Any, Mapping, Optional
import zipfile

import googleapiclient
from googleapiclient import discovery
import requests

from kfp.v2.google.client import client_utils
from kfp.v2.google.client import runtime_config_builder

_PROXY_FUNCTION_NAME = 'templated_http_request-v1'
_PROXY_FUNCTION_FILENAME = '_cloud_function_templated_http_request.py'

_CAIPP_ENDPOINT_WITHOUT_REGION = 'aiplatform.googleapis.com'
_CAIPP_API_VERSION = 'v1beta1'


def create_from_pipeline_file(
    pipeline_path: str,
    schedule: str,
    project_id: str,
    region: str = 'us-central1',
    time_zone: str = 'US/Pacific',
    parameter_values: Optional[Mapping[str, Any]] = None,
    pipeline_root: Optional[str] = None,
    service_account: Optional[str] = None,
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
      pipeline_path: Path of the compiled pipeline file.
      schedule: Schedule in cron format. Example: "45 * * * *"
      project_id: Google Cloud project ID
      region: Google Cloud compute region. Default is 'us-central1'
      time_zone: Schedule time zone. Default is 'US/Pacific'
      parameter_values: Arguments for the pipeline parameters
      pipeline_root: Optionally the user can override the pipeline root
        specified during the compile time.
      service_account: The service account that the pipeline workload runs as.
      app_engine_region: The region that cloud scheduler job is created in.
      cloud_scheduler_service_account: The service account that Cloud Scheduler job and the proxy cloud function use.
        this should have permission to call AI Platform API and the proxy function.
        If not specified, the functions uses the App Engine default service account.

    Returns:
      Created Google Cloud Scheduler Job object dictionary.
    """
    pipeline_dict = client_utils.load_json(pipeline_path)

    return _create_from_pipeline_dict(
        pipeline_dict=pipeline_dict,
        schedule=schedule,
        project_id=project_id,
        region=region,
        time_zone=time_zone,
        parameter_values=parameter_values,
        pipeline_root=pipeline_root,
        service_account=service_account,
        app_engine_region=app_engine_region,
        cloud_scheduler_service_account=cloud_scheduler_service_account,
    )


def _create_from_pipeline_dict(
    pipeline_dict: dict,
    schedule: str,
    project_id: str,
    region: str = 'us-central1',
    time_zone: str = 'US/Pacific',
    parameter_values: Optional[Mapping[str, Any]] = None,
    pipeline_root: Optional[str] = None,
    service_account: Optional[str] = None,
    app_engine_region: Optional[str] = None,
    cloud_scheduler_service_account: Optional[str] = None,
) -> dict:
    """Creates schedule for compiled pipeline dictionary."""

    _enable_required_apis(project_id=project_id)

    # If appengine region is not provided, use the pipeline region.
    app_engine_region = app_engine_region or region

    proxy_function_url = _get_proxy_cloud_function_endpoint(
        project_id=project_id,
        region=region,
        cloud_scheduler_service_account=cloud_scheduler_service_account,
    )

    if parameter_values or pipeline_root:
        config_builder = runtime_config_builder.RuntimeConfigBuilder.from_job_spec_json(
            pipeline_dict)
        config_builder.update_runtime_parameters(
            parameter_values=parameter_values)
        config_builder.update_pipeline_root(pipeline_root=pipeline_root)
        updated_runtime_config = config_builder.build()
        pipeline_dict['runtimeConfig'] = updated_runtime_config

    # Creating job creation request to get the final request URL
    pipeline_jobs_api_url = f'https://{region}-{_CAIPP_ENDPOINT_WITHOUT_REGION}/{_CAIPP_API_VERSION}/projects/{project_id}/locations/{region}/pipelineJobs'

    # Preparing the request body for the Cloud Function processing
    pipeline_name = pipeline_dict['pipelineSpec']['pipelineInfo']['name']
    full_pipeline_name = 'projects/{}/pipelineJobs/{}'.format(project_id, pipeline_name)
    pipeline_display_name = pipeline_dict.get('displayName')
    time_format_suffix = "-{{$.scheduledTime.strftime('%Y-%m-%d-%H-%M-%S')}}"
    if 'name' in pipeline_dict:
        pipeline_dict['name'] += time_format_suffix
    if 'displayName' in pipeline_dict:
        pipeline_dict['displayName'] += time_format_suffix

    pipeline_dict['_url'] = pipeline_jobs_api_url
    pipeline_dict['_method'] = 'POST'

    if service_account is not None:
        pipeline_dict['serviceAccount'] = service_account

    pipeline_text = json.dumps(pipeline_dict)
    pipeline_data = pipeline_text.encode('utf-8')

    # Generating human-readable schedule name.
    schedule_name = _build_schedule_name(
        job_body_data=pipeline_data,
        schedule=schedule,
        pipeline_name=full_pipeline_name,
        display_name=pipeline_display_name,
    )

    project_location_path = 'projects/{}/locations/{}'.format(
        project_id, app_engine_region)
    scheduled_job_full_name = '{}/jobs/{}'.format(project_location_path,
                                                  schedule_name)
    service_account_email = cloud_scheduler_service_account or '{}@appspot.gserviceaccount.com'.format(
        project_id)

    scheduled_job = dict(
        name=scheduled_job_full_name,  # Optional. Only used for readable names.
        schedule=schedule,
        time_zone=time_zone,
        http_target=dict(
            http_method='POST',
            uri=proxy_function_url,
            # Warning: when using google.cloud.scheduler_v1, the type of body is
            # bytes or string. But when using the API through discovery, the body
            # needs to be base64-encoded.
            body=base64.b64encode(pipeline_data).decode('utf-8'),
            oidc_token=dict(service_account_email=service_account_email,),
        ),
        # TODO(avolkov): Add labels once Cloud Scheduler supports them
        # labels={
        #     'google.cloud.ai-platform.pipelines.scheduling': 'v1alpha1',
        # },
    )

    try:
        response = _create_scheduler_job(
            project_location_path=project_location_path,
            job_body=scheduled_job,
        )
        return response
    except googleapiclient.errors.HttpError as err:
        # Handling the case where the exact schedule already exists.
        if err.resp.get('status') == '409':
            raise RuntimeError(
                'The exact same schedule already exists') from err
        raise err


def _create_scheduler_job(project_location_path: str,
                          job_body: Mapping[str, Any]) -> str:
    """Creates a scheduler job.

    Args:
      project_location_path: The project location path.
      job_body: The scheduled job dictionary object.

    Returns:
      The response from scheduler service.
    """
    # We cannot use google.cloud.scheduler_v1.CloudSchedulerClient since
    # it's not available internally.
    scheduler_service = discovery.build(
        'cloudscheduler', 'v1', cache_discovery=False)
    scheduler_jobs_api = scheduler_service.projects().locations().jobs()
    response = scheduler_jobs_api.create(
        parent=project_location_path,
        body=job_body,
    ).execute()
    return response


def _build_schedule_name(
    job_body_data: bytes,
    schedule: str,
    pipeline_name: str,
    display_name: str,
) -> str:
    """Generates the name for the schedule.

    Args:
      job_body_data: The serialized pipeline job.
      schedule: Schedule in cron format.
      pipeline_name: Full resource name of the pipeline in
        projects/<project>/pipelineJobs/<pipeline_id> format.
      display_name: Pipeline display name.
    Returns:
      Suggested schedule resource name.
    """
    pipeline_name_part = 'pipeline'
    if pipeline_name is not None:
        # pipeline_name format: projects/<project>/pipelineJobs/<pipeline_id>
        pipeline_id = pipeline_name.split('/')[-1]
        # Limiting the length of the pipeline name part.
        pipeline_name_part = pipeline_id[0:200]
    elif display_name is not None:
        pipeline_name_part = display_name
    pipeline_hash_part = hashlib.sha256(job_body_data).hexdigest()[0:8]
    schedule_part = (
        schedule.replace('*/', 'div').replace('*', 'a').replace(' ', '-'))
    job_name = '_'.join([
        'pipeline',
        pipeline_name_part,
        pipeline_hash_part,
        schedule_part,
    ])
    job_name = re.sub('[^-_a-z0-9]', '_', job_name)
    return job_name


# For mocking
def _get_cloud_functions_api():
    functions_service = discovery.build(
        'cloudfunctions', 'v1', cache_discovery=False)
    functions_api = functions_service.projects().locations().functions()
    return functions_api


def _create_or_get_cloud_function(
    name: str,
    entry_point: str,
    file_data: Mapping[str, str],
    project_id: str,
    region: str,
    runtime: str = 'python37',
    cloud_scheduler_service_account: Optional[str] = None,
):
    """Creates Google Cloud Function."""
    functions_api = _get_cloud_functions_api()

    project_location_path = 'projects/{}/locations/{}'.format(
        project_id, region)
    function_full_name = project_location_path + '/functions/' + name

    # Returning early if the function already exists.
    try:
        function_get_response = functions_api.get(
            name=function_full_name).execute()
        return function_get_response
    except googleapiclient.errors.HttpError as err:
        raise_error = True
        if err.resp['status'] == '404':
            # The function does not exist, which is expected.
            raise_error = False
        if raise_error:
            raise err

    get_upload_url_response = functions_api.generateUploadUrl(
        parent=project_location_path,
        body={},
    ).execute()
    upload_url = get_upload_url_response['uploadUrl']

    # Preparing the payload archive
    with tempfile.TemporaryFile() as archive_file:
        with zipfile.ZipFile(archive_file, 'w',
                             zipfile.ZIP_DEFLATED) as archive:
            for path, data in file_data.items():
                archive.writestr(path, data)

        archive_file.seek(0)
        headers = {
            'content-type': 'application/zip',
            'x-goog-content-length-range': '0,104857600',
        }
        upload_response = requests.put(
            url=upload_url,
            headers=headers,
            data=archive_file,
        )
        upload_response.raise_for_status()

    # Prepare Request Body
    # https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#resource-cloudfunction

    request_body = {
        'name': function_full_name,
        'entryPoint': entry_point,
        'sourceUploadUrl': upload_url,
        'httpsTrigger': {},
        'runtime': runtime,
    }
    if cloud_scheduler_service_account is not None:
        request_body["serviceAccountEmail"] = cloud_scheduler_service_account
    try:
        functions_api.create(
            location=project_location_path,
            body=request_body,
        ).execute()
        # Response is an operation object dict
        # TODO(avolkov): Wait for the operation to succeed
        # 'status' can be 'ACTIVE', 'DEPLOY_IN_PROGRESS', etc
    except googleapiclient.errors.HttpError as err:
        # Handling the case where the function already exists
        raise_error = True
        if err.resp['status'] == '409':
            err_content_dict = json.loads(err.content)
            err_error_dict = err_content_dict.get('error')
            if err_error_dict and err_error_dict.get(
                    'status') == 'ALREADY_EXISTS':
                # This should not usually happen
                logging.warning(
                    'Cloud Function already exists: name=%s',
                    function_full_name,
                )
                raise_error = False
        if raise_error:
            raise err

    function_get_response = functions_api.get(name=function_full_name).execute()
    logging.info('Created Cloud Function: name=%s', function_full_name)

    return function_get_response


def _enable_required_apis(project_id: str,):
    """Enables necessary APIs."""
    serviceusage_service = discovery.build(
        'serviceusage', 'v1', cache_discovery=False)
    services_api = serviceusage_service.services()

    required_services = [
        'cloudfunctions.googleapis.com',
        'cloudscheduler.googleapis.com',
        'appengine.googleapis.com',  # Required by the Cloud Scheduler.
    ]
    project_path = 'projects/' + project_id
    for service_name in required_services:
        service_path = project_path + '/services/' + service_name
        services_api.enable(name=service_path).execute()


def _get_proxy_cloud_function_endpoint(
    project_id: str,
    region: str = 'us-central1',
    cloud_scheduler_service_account: Optional[str] = None,
):
    """Sets up a proxy Cloud Function."""
    function_source_path = (
        pathlib.Path(__file__).parent / _PROXY_FUNCTION_FILENAME)
    function_source = function_source_path.read_text()
    function_entry_point = '_process_request'

    function_dict = _create_or_get_cloud_function(
        name=_PROXY_FUNCTION_NAME,
        entry_point=function_entry_point,
        project_id=project_id,
        region=region,
        runtime='python37',
        file_data={
            'main.py': function_source,
            'requirements.txt': 'google-api-python-client>=1.7.8,<2',
        },
        cloud_scheduler_service_account=cloud_scheduler_service_account,
    )
    endpoint_url = function_dict['httpsTrigger']['url']
    return endpoint_url
