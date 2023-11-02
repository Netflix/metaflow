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
from kfp.v2 import compiler
from kfp.v2 import dsl

test_data_dir = pathlib.Path(__file__).parent / 'component_yaml'
add_op = components.load_component_from_file(
    str(test_data_dir / 'add_component.yaml'))


@dsl.pipeline(name='add-pipeline', pipeline_root='dummy_root')
def my_pipeline(
    a: int = 2,
    b: int = 5,
):
    first_add_task = add_op(a, 3)
    second_add_task = add_op(first_add_task.outputs['sum'], b)
    third_add_task = add_op(second_add_task.outputs['sum'], 7)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
