# Copyright 2020 The Kubeflow Authors
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

import pathlib

from kfp import components
from kfp.v2 import dsl
import kfp.v2.compiler as compiler

component_op_1 = components.load_component_from_text("""
name: Write to GCS
inputs:
- {name: text, type: String, description: 'Content to be written to GCS'}
outputs:
- {name: output_gcs_path, type: GCSPath, description: 'GCS file path'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0" | gsutil cp - "$1"
    - {inputValue: text}
    - {outputUri: output_gcs_path}
""")

component_op_2 = components.load_component_from_text("""
name: Read from GCS
inputs:
- {name: input_gcs_path, type: GCSPath, description: 'GCS file path'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      gsutil cat "$0"
    - {inputUri: input_gcs_path}
""")


@dsl.pipeline(name='simple-two-step-pipeline', pipeline_root='dummy_root')
def my_pipeline(text: str = 'Hello world!'):
    component_1 = component_op_1(text=text).set_display_name('Producer')
    component_2 = component_op_2(
        input_gcs_path=component_1.outputs['output_gcs_path'])
    component_2.set_display_name('Consumer')


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        pipeline_parameters={'text': 'Hello KFP!'},
        package_path=__file__.replace('.py', '.json'))
