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
from kfp.v2 import compiler, dsl

component_op_1 = components.load_component_from_text("""
name: upstream
inputs:
- {name: input_1, type: String}
- {name: input_2, type: Float}
- {name: input_3, type: String}
- {name: input_4, type: String}
outputs:
- {name: output_1, type: Integer}
- {name: output_2, type: Model}
- {name: output_3}
- {name: output_4, type: Model}
- {name: output_5, type: Datasets}
- {name: output_6, type: Some arbitrary type}
- {name: output_7, type: {GcsPath: {data_type: TSV}}}
- {name: output_8, type: HTML}
- {name: output_9, type: google.BQMLModel}
implementation:
  container:
    image: gcr.io/image
    args:
    - {inputValue: input_1}
    - {inputValue: input_2}
    - {inputValue: input_3}
    - {inputValue: input_4}
    - {outputPath: output_1}
    - {outputUri: output_2}
    - {outputPath: output_3}
    - {outputUri: output_4}
    - {outputUri: output_5}
    - {outputPath: output_6}
    - {outputPath: output_7}
    - {outputPath: output_8}
""")

component_op_2 = components.load_component_from_text("""
name: downstream
inputs:
- {name: input_a, type: Integer}
- {name: input_b, type: Model}
- {name: input_c}
- {name: input_d, type: Model}
- {name: input_e, type: Datasets}
- {name: input_f, type: Some arbitrary type}
- {name: input_g, type: {GcsPath: {data_type: TSV}}}
- {name: input_h, type: HTML}
- {name: input_i, type: google.BQMLModel}
implementation:
  container:
    image: gcr.io/image
    args:
    - {inputValue: input_a}
    - {inputUri: input_b}
    - {inputPath: input_c}
    - {inputUri: input_d}
    - {inputUri: input_e}
    - {inputPath: input_f}
    - {inputPath: input_g}
    - {inputPath: input_h}
""")


@dsl.pipeline(name='pipeline-with-various-types', pipeline_root='dummy_root')
def my_pipeline(input1: str, input3: str, input4: str = ''):
    component_1 = component_op_1(
        input_1=input1,
        input_2=3.1415926,
        input_3=input3,
        input_4=input4,
    )
    component_2 = component_op_2(
        input_a=component_1.outputs['output_1'],
        input_b=component_1.outputs['output_2'],
        input_c=component_1.outputs['output_3'],
        input_d=component_1.outputs['output_4'],
        input_e=component_1.outputs['output_5'],
        input_f=component_1.outputs['output_6'],
        input_g=component_1.outputs['output_7'],
        input_h=component_1.outputs['output_8'],
        input_i=component_1.outputs['output_9'],
    )


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
