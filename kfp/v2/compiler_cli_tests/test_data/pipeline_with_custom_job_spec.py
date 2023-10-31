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

from kfp.components._structures import InputValuePlaceholder
from kfp.v2 import dsl
from kfp.v2.dsl import component
from kfp.v2.google import experimental
from kfp.v2 import compiler


@component
def training_op(input1: str):
    print('dummy training master: {}'.format(input1))


@dsl.pipeline(name='pipeline-on-custom-job', pipeline_root='dummy_root')
def my_pipeline():

    training_task1 = training_op('hello-world')
    experimental.run_as_aiplatform_custom_job(
        training_task1, replica_count=10, display_name='custom-job-simple')

    training_task2 = training_op('advanced setting - raw workerPoolSpec')
    experimental.run_as_aiplatform_custom_job(
        training_task2,
        display_name='custom-job-advanced',
        worker_pool_specs=[
            {
                'containerSpec': {
                    'imageUri':
                        'alpine',
                    'command': [
                        'sh', '-c', 'set -e -x\necho \"worker1:\" \"$0\"\n',
                        InputValuePlaceholder('input1')
                    ]
                },
                'machineSpec': {
                    'machineType': 'n1-standard-4'
                },
                'replicaCount': '1',
            },
        ])


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
