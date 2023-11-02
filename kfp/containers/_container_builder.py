# Copyright 2019 The Kubeflow Authors
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

__all__ = [
    'ContainerBuilder',
]

import logging
import tarfile
import tempfile
import os
import uuid

SERVICEACCOUNT_NAMESPACE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
GCS_STAGING_BLOB_DEFAULT_PREFIX = 'kfp_container_build_staging'
GCR_DEFAULT_IMAGE_SUFFIX = 'kfp_container'
KANIKO_EXECUTOR_IMAGE_DEFAULT = 'gcr.io/kaniko-project/executor@sha256:78d44ec4e9cb5545d7f85c1924695c89503ded86a59f92c7ae658afa3cff5400'


def _get_project_id():
    import requests
    URL = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    headers = {'Metadata-Flavor': 'Google'}
    r = requests.get(url=URL, headers=headers)
    if not r.ok:
        raise RuntimeError(
            'ContainerBuilder failed to retrieve the project id.')
    return r.text


def _get_instance_id():
    import requests
    URL = "http://metadata.google.internal/computeMetadata/v1/instance/id"
    headers = {'Metadata-Flavor': 'Google'}
    r = requests.get(url=URL, headers=headers)
    if not r.ok:
        raise RuntimeError(
            'ContainerBuilder failed to retrieve the instance id.')
    return r.text


class ContainerBuilder(object):
    """ContainerBuilder helps build a container image."""

    def __init__(self,
                 gcs_staging=None,
                 default_image_name=None,
                 namespace=None,
                 service_account='kubeflow-pipelines-container-builder',
                 kaniko_executor_image=KANIKO_EXECUTOR_IMAGE_DEFAULT,
                 k8s_client_configuration=None):
        """
    Args:
      gcs_staging (str): GCS bucket/blob that can store temporary build files,
          default is gs://PROJECT_ID/kfp_container_build_staging. You have to
          specify this when it doesn't run in cluster.
      default_image_name (str): Target container image name that will be used by the build method if the target_image argument is not specified.
      namespace (str): Kubernetes namespace where the container builder pod is launched,
          default is the same namespace as the notebook service account in cluster
          or 'kubeflow' if not in cluster. If using the full Kubeflow
          deployment and not in cluster, you should specify your own user namespace.
      service_account (str): Kubernetes service account the pod uses for container building,
          The default value is "kubeflow-pipelines-container-builder". It works with Kubeflow Pipelines clusters installed using Google Cloud Marketplace or Standalone with version > 0.4.0.
          The service account should have permission to read and write from staging gcs path and upload built images to gcr.io.
      kaniko_executor_image (str): Docker image used to run kaniko executor. Defaults to gcr.io/kaniko-project/executor:v0.10.0.
      k8s_client_configuration (kubernetes.Configuration): Kubernetes client configuration object to be used when talking with Kubernetes API.
        This is optional. If not specified, it will use the default configuration. This can be used to personalize the client used to talk to the Kubernetes server and change authentication parameters.
    """
        self._gcs_staging = gcs_staging
        self._gcs_staging_checked = False
        self._default_image_name = default_image_name
        self._namespace = namespace
        self._service_account = service_account
        self._kaniko_image = kaniko_executor_image
        self._k8s_client_configuration = k8s_client_configuration

    def _get_namespace(self):
        if self._namespace is None:
            # Configure the namespace
            if os.path.exists(SERVICEACCOUNT_NAMESPACE):
                with open(SERVICEACCOUNT_NAMESPACE, 'r') as f:
                    self._namespace = f.read()
            else:
                self._namespace = 'kubeflow'
        return self._namespace

    def _get_staging_location(self):
        if self._gcs_staging_checked:
            return self._gcs_staging

        # Configure the GCS staging bucket
        if self._gcs_staging is None:
            try:
                gcs_bucket = _get_project_id()
            except:
                raise ValueError(
                    'Cannot get the Google Cloud project ID, please specify the gcs_staging argument.'
                )
            self._gcs_staging = 'gs://' + gcs_bucket + '/' + GCS_STAGING_BLOB_DEFAULT_PREFIX
        else:
            from pathlib import PurePath
            path = PurePath(self._gcs_staging).parts
            if len(path) < 2 or not path[0].startswith('gs'):
                raise ValueError('Error: {} should be a GCS path.'.format(
                    self._gcs_staging))
            gcs_bucket = path[1]
        from ._gcs_helper import GCSHelper
        GCSHelper.create_gcs_bucket_if_not_exist(gcs_bucket)
        self._gcs_staging_checked = True
        return self._gcs_staging

    def _get_default_image_name(self):
        if self._default_image_name is None:
            # KubeFlow Jupyter notebooks have environment variable with the notebook ID
            try:
                nb_id = os.environ.get('NB_PREFIX', _get_instance_id())
            except:
                raise ValueError('Please provide the default_image_name.')
            nb_id = nb_id.replace('/', '-').strip('-')
            self._default_image_name = os.path.join('gcr.io', _get_project_id(),
                                                    nb_id,
                                                    GCR_DEFAULT_IMAGE_SUFFIX)
        return self._default_image_name

    def _generate_kaniko_spec(self, context, docker_filename, target_image):
        """_generate_kaniko_yaml generates kaniko job yaml based on a template
        yaml."""
        content = {
            'apiVersion': 'v1',
            'metadata': {
                'generateName': 'kaniko-',
                'namespace': self._get_namespace(),
                'annotations': {
                    'sidecar.istio.io/inject': 'false'
                },
            },
            'kind': 'Pod',
            'spec': {
                'restartPolicy': 'Never',
                'containers': [{
                    'name': 'kaniko',
                    'args': [
                        '--cache=true',
                        '--dockerfile=' + docker_filename,
                        '--context=' + context,
                        '--destination=' + target_image,
                        '--digest-file=/dev/termination-log',  # This is suggested by the Kaniko devs as a way to return the image digest from Kaniko Pod. See https://github.com/GoogleContainerTools/kaniko#--digest-file
                    ],
                    'image': self._kaniko_image,
                }],
                'serviceAccountName': self._service_account
            }
        }
        return content

    def _wrap_dir_in_tarball(self, tarball_path, dir_name):
        """_wrap_files_in_tarball creates a tarball for all the files in the
        directory."""
        if not tarball_path.endswith('.tar.gz'):
            raise ValueError('the tarball path should end with .tar.gz')
        with tarfile.open(tarball_path, 'w:gz') as tarball:
            tarball.add(dir_name, arcname='')

    def build(self,
              local_dir,
              docker_filename: str = 'Dockerfile',
              target_image=None,
              timeout=1000):
        """
    Args:
      local_dir (str): local directory that stores all the necessary build files
      docker_filename (str): the path of the Dockerfile relative to the local_dir
      target_image (str): The container image name where the data will be pushed. Can include tag. If not specified, the function will use the default_image_name specified when creating ContainerBuilder.
      timeout (int): time out in seconds. Default: 1000
    """
        target_image = target_image or self._get_default_image_name()
        # Prepare build context
        with tempfile.TemporaryDirectory() as local_build_dir:
            from ._gcs_helper import GCSHelper
            logging.info('Generate build files.')
            local_tarball_path = os.path.join(local_build_dir,
                                              'docker.tmp.tar.gz')
            self._wrap_dir_in_tarball(local_tarball_path, local_dir)
            # Upload to the context
            context = os.path.join(self._get_staging_location(),
                                   str(uuid.uuid4()) + '.tar.gz')
            GCSHelper.upload_gcs_file(local_tarball_path, context)

            # Run kaniko job
            kaniko_spec = self._generate_kaniko_spec(
                context=context,
                docker_filename=docker_filename,
                target_image=target_image)
            logging.info('Start a kaniko job for build.')
            from ._k8s_job_helper import K8sJobHelper
            k8s_helper = K8sJobHelper(self._k8s_client_configuration)
            result_pod_obj = k8s_helper.run_job(kaniko_spec, timeout)
            logging.info('Kaniko job complete.')

            # Clean up
            GCSHelper.remove_gcs_blob(context)

            # Returning image name with digest
            (image_repo, _, image_tag) = target_image.partition(':')
            # When Kaniko build completes successfully, the termination message is the hash digest of the newly built image. Otherwise it's empty. See https://github.com/GoogleContainerTools/kaniko#--digest-file https://kubernetes.io/docs/tasks/debug-application-cluster/determine-reason-pod-failure/#customizing-the-termination-message
            termination_message = [
                status.state.terminated.message
                for status in result_pod_obj.status.container_statuses
                if status.name == 'kaniko'
            ][0]  # Note: Using status.state instead of status.last_state since last_state entries can still be None
            image_digest = termination_message
            if not image_digest.startswith('sha256:'):
                raise RuntimeError(
                    "Kaniko returned invalid image digest: {}".format(
                        image_digest))
            strict_image_name = image_repo + '@' + image_digest
            logging.info(
                'Built and pushed image: {}.'.format(strict_image_name))
            return strict_image_name
