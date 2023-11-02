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

from datetime import datetime
from kubernetes import client as k8s_client
from kubernetes import config
import time
import logging
import os


class K8sJobHelper(object):
    """Kubernetes Helper."""

    def __init__(self, k8s_client_configuration=None):
        if not self._configure_k8s(k8s_client_configuration):
            raise Exception('K8sHelper __init__ failure')

    def _configure_k8s(self, k8s_client_configuration=None):
        k8s_config_file = os.environ.get('KUBECONFIG')
        if k8s_config_file:
            try:
                logging.info('Loading kubernetes config from the file %s',
                             k8s_config_file)
                config.load_kube_config(
                    config_file=k8s_config_file,
                    client_configuration=k8s_client_configuration)
            except Exception as e:
                raise RuntimeError(
                    'Can not load kube config from the file %s, error: %s',
                    k8s_config_file, e)
        else:
            try:
                config.load_incluster_config()
                logging.info('Initialized with in-cluster config.')
            except:
                logging.info(
                    'Cannot find in-cluster config, trying the local kubernetes config. '
                )
                try:
                    config.load_kube_config(
                        client_configuration=k8s_client_configuration)
                    logging.info(
                        'Found local kubernetes config. Initialized with kube_config.'
                    )
                except:
                    raise RuntimeError(
                        'Forgot to run the gcloud command? Check out the link: \
          https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl for more information'
                    )
        self._api_client = k8s_client.ApiClient()
        self._corev1 = k8s_client.CoreV1Api(self._api_client)
        return True

    def _create_k8s_job(self, yaml_spec):
        """_create_k8s_job creates a kubernetes job based on the yaml spec."""
        pod = k8s_client.V1Pod(
            metadata=k8s_client.V1ObjectMeta(
                generate_name=yaml_spec['metadata']['generateName'],
                annotations=yaml_spec['metadata']['annotations']))
        container = k8s_client.V1Container(
            name=yaml_spec['spec']['containers'][0]['name'],
            image=yaml_spec['spec']['containers'][0]['image'],
            args=yaml_spec['spec']['containers'][0]['args'])
        pod.spec = k8s_client.V1PodSpec(
            restart_policy=yaml_spec['spec']['restartPolicy'],
            containers=[container],
            service_account_name=yaml_spec['spec']['serviceAccountName'])
        try:
            api_response = self._corev1.create_namespaced_pod(
                yaml_spec['metadata']['namespace'], pod)
            return api_response.metadata.name, True
        except k8s_client.rest.ApiException as e:
            logging.exception(
                "Exception when calling CoreV1Api->create_namespaced_pod: {}\n"
                .format(str(e)))
            return '', False

    def _wait_for_k8s_job(self, pod_name, yaml_spec, timeout):
        """_wait_for_k8s_job waits for the job to complete."""
        status = 'running'
        start_time = datetime.now()
        while status in ['pending', 'running']:
            # Pod pending values: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodStatus.md
            try:
                api_response = self._corev1.read_namespaced_pod(
                    pod_name, yaml_spec['metadata']['namespace'])
                status = api_response.status.phase.lower()
                time.sleep(5)
                elapsed_time = (datetime.now() - start_time).seconds
                logging.info('{} seconds: waiting for job to complete'.format(
                    elapsed_time))
                if elapsed_time > timeout:
                    logging.info('Kubernetes job timeout')
                    return False
            except k8s_client.rest.ApiException as e:
                logging.exception(
                    'Exception when calling CoreV1Api->read_namespaced_pod: {}\n'
                    .format(str(e)))
                return False
        return status == 'succeeded'

    def _delete_k8s_job(self, pod_name, yaml_spec):
        """_delete_k8s_job deletes a pod."""
        try:
            api_response = self._corev1.delete_namespaced_pod(
                pod_name,
                yaml_spec['metadata']['namespace'],
                body=k8s_client.V1DeleteOptions())
        except k8s_client.rest.ApiException as e:
            logging.exception(
                'Exception when calling CoreV1Api->delete_namespaced_pod: {}\n'
                .format(str(e)))

    def _read_pod_log(self, pod_name, yaml_spec):
        try:
            api_response = self._corev1.read_namespaced_pod_log(
                pod_name, yaml_spec['metadata']['namespace'])
        except k8s_client.rest.ApiException as e:
            logging.exception(
                'Exception when calling CoreV1Api->read_namespaced_pod_log: {}\n'
                .format(str(e)))
            return False
        return api_response

    def _read_pod_status(self, pod_name, namespace):
        try:
            # Using read_namespaced_pod due to the following error: "pods \"kaniko-p2phh\" is forbidden: User \"system:serviceaccount:kubeflow:jupyter-notebook\" cannot get pods/status in the namespace \"kubeflow\""
            #api_response = self._corev1.read_namespaced_pod_status(pod_name, namespace)
            api_response = self._corev1.read_namespaced_pod(pod_name, namespace)
        except k8s_client.rest.ApiException as e:
            logging.exception(
                'Exception when calling CoreV1Api->read_namespaced_pod_status: {}\n'
                .format(str(e)))
            return False
        return api_response

    def run_job(self, yaml_spec, timeout=600):
        """run_job runs a kubernetes job and clean up afterwards."""
        pod_name, succ = self._create_k8s_job(yaml_spec)
        namespace = yaml_spec['metadata']['namespace']
        if not succ:
            raise RuntimeError('Kubernetes job creation failed.')
        # timeout in seconds
        succ = self._wait_for_k8s_job(pod_name, yaml_spec, timeout)
        if not succ:
            logging.info('Kubernetes job failed.')
            print(self._read_pod_log(pod_name, yaml_spec))
            raise RuntimeError('Kubernetes job failed.')
        status_obj = self._read_pod_status(pod_name, namespace)
        self._delete_k8s_job(pod_name, yaml_spec)
        return status_obj
