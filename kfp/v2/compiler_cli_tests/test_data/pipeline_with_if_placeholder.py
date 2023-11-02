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
from kfp import dsl
import kfp.v2.compiler as compiler

test_data_dir = pathlib.Path(__file__).parent / 'component_yaml'
component_op = components.load_component_from_file(
    str(test_data_dir / 'if_placeholder_component.yaml'))


@dsl.pipeline(
    name='one-step-pipeline-with-if-placeholder', pipeline_root='dummy_root')
def my_pipeline(input0: str, input1: str, input2: str):
    # supply only optional_input_1 but not optional_input_2
    component = component_op(required_input=input0, optional_input_1=input1)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
