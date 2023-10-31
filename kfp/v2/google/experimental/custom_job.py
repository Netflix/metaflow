# Copyright 2021 The Kubeflow Authors
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
"""Module for supporting Google Cloud AI Platform Custom Job."""

import copy
import warnings
from typing import Any, List, Mapping, Optional

from kfp import dsl
from kfp.dsl import dsl_utils

_DEFAULT_CUSTOM_JOB_MACHINE_TYPE = 'n1-standard-4'


def run_as_aiplatform_custom_job(
    op: dsl.ContainerOp,
    display_name: Optional[str] = None,
    replica_count: Optional[int] = None,
    machine_type: Optional[str] = None,
    accelerator_type: Optional[str] = None,
    accelerator_count: Optional[int] = None,
    boot_disk_type: Optional[str] = None,
    boot_disk_size_gb: Optional[int] = None,
    timeout: Optional[str] = None,
    restart_job_on_worker_restart: Optional[bool] = None,
    service_account: Optional[str] = None,
    network: Optional[str] = None,
    output_uri_prefix: Optional[str] = None,
    worker_pool_specs: Optional[List[Mapping[str, Any]]] = None,
) -> None:
    """Run a pipeline task using AI Platform (Unified) custom training job.

    For detailed doc of the service, please refer to
    https://cloud.google.com/ai-platform-unified/docs/training/create-custom-job

    Args:
      op: The task (ContainerOp) object to run as aiplatform custom job.
      display_name: Optional. The name of the custom job.
      replica_count: Optional. The number of replicas to be split between master
        workerPoolSpec and worker workerPoolSpec. (master always has 1 replica).
      machine_type: Optional. The type of the machine to run the custom job. The
        default value is "n1-standard-4".
      accelerator_type: Optional. The type of accelerator(s) that may be attached
        to the machine as per acceleratorCount. Optional.
      accelerator_count: Optional. The number of accelerators to attach to the
        machine.
      boot_disk_type: Optional. Type of the boot disk (default is "pd-ssd"). Valid
        values: "pd-ssd" (Persistent Disk Solid State Drive) or "pd-standard"
          (Persistent Disk Hard Disk Drive).
      boot_disk_size_gb: Optional. Size in GB of the boot disk (default is 100GB).
      timeout: Optional. The maximum job running time. The default is 7 days. A
        duration in seconds with up to nine fractional digits, terminated by 's'.
        Example: "3.5s"
      restart_job_on_worker_restart: Optional. Restarts the entire CustomJob if a
        worker gets restarted. This feature can be used by distributed training
        jobs that are not resilient to workers leaving and joining a job.
      service_account: Optional. Specifies the service account for workload run-as
        account.
      network: Optional. The full name of the Compute Engine network to which the
        job should be peered. For example, projects/12345/global/networks/myVPC.
      output_uri_prefix: Optional. Google Cloud Storage URI to output directory.
      additional_worker_pool_specs: Optional. Additional workerPoolSpecs for
        distributed training. For details, please see:
        https://cloud.google.com/ai-platform-unified/docs/training/distributed-training
    """
    warnings.warn(
        'This function will be deprecated in v2.0.0', category=FutureWarning,
    )

    job_spec = {}

    if worker_pool_specs is not None:
        worker_pool_specs = copy.deepcopy(worker_pool_specs)

        def _is_output_parameter(output_key: str) -> bool:
            return output_key in (
                op.component_spec.output_definitions.parameters.keys())

        for worker_pool_spec in worker_pool_specs:
            if 'containerSpec' in worker_pool_spec:
                container_spec = worker_pool_spec['containerSpec']
                if 'command' in container_spec:
                    dsl_utils.resolve_cmd_lines(container_spec['command'],
                                                _is_output_parameter)
                if 'args' in container_spec:
                    dsl_utils.resolve_cmd_lines(container_spec['args'],
                                                _is_output_parameter)

            elif 'pythonPackageSpec' in worker_pool_spec:
                # For custom Python training, resolve placeholders in args only.
                python_spec = worker_pool_spec['pythonPackageSpec']
                if 'args' in python_spec:
                    dsl_utils.resolve_cmd_lines(python_spec['args'],
                                                _is_output_parameter)

            else:
                raise ValueError(
                    'Expect either "containerSpec" or "pythonPackageSpec" in each '
                    'workerPoolSpec. Got: {}'.format(custom_job_spec))

        job_spec['workerPoolSpecs'] = worker_pool_specs

    else:
        worker_pool_spec = {
            'machineSpec': {
                'machineType': machine_type or _DEFAULT_CUSTOM_JOB_MACHINE_TYPE
            },
            'replicaCount': '1',
            'containerSpec': {
                'imageUri': op.container.image,
            }
        }
        if op.container.command:
            worker_pool_spec['containerSpec']['command'] = op.container.command
        if op.container.args:
            worker_pool_spec['containerSpec']['args'] = op.container.args
        if accelerator_type is not None:
            worker_pool_spec['machineSpec'][
                'acceleratorType'] = accelerator_type
        if accelerator_count is not None:
            worker_pool_spec['machineSpec'][
                'acceleratorCount'] = accelerator_count
        if boot_disk_type is not None:
            if 'diskSpec' not in worker_pool_spec:
                worker_pool_spec['diskSpec'] = {}
            worker_pool_spec['diskSpec']['bootDiskType'] = boot_disk_type
        if boot_disk_size_gb is not None:
            if 'diskSpec' not in worker_pool_spec:
                worker_pool_spec['diskSpec'] = {}
            worker_pool_spec['diskSpec']['bootDiskSizeGb'] = boot_disk_size_gb

        job_spec['workerPoolSpecs'] = [worker_pool_spec]
        if replica_count is not None and replica_count > 1:
            additional_worker_pool_spec = copy.deepcopy(worker_pool_spec)
            additional_worker_pool_spec['replicaCount'] = str(replica_count - 1)
            job_spec['workerPoolSpecs'].append(additional_worker_pool_spec)

    if timeout is not None:
        if 'scheduling' not in job_spec:
            job_spec['scheduling'] = {}
        job_spec['scheduling']['timeout'] = timeout
    if restart_job_on_worker_restart is not None:
        if 'scheduling' not in job_spec:
            job_spec['scheduling'] = {}
        job_spec['scheduling'][
            'restartJobOnWorkerRestart'] = restart_job_on_worker_restart
    if service_account is not None:
        job_spec['serviceAccount'] = service_account
    if network is not None:
        job_spec['network'] = network
    if output_uri_prefix is not None:
        job_spec['baseOutputDirectory'] = {'outputUriPrefix': output_uri_prefix}

    op.custom_job_spec = {
        'displayName': display_name or op.name,
        'jobSpec': job_spec
    }
