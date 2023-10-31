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
from typing import Optional

from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component


@component
def component_op(
    input1: str = 'default value',
    input2: Optional[str] = None,
    input3: Optional[str] = None,
):
    print(f'input1: {input1}, type: {type(input1)}')
    print(f'input2: {input2}, type: {type(input2)}')
    print(f'input3: {input3}, type: {type(input3)}')


@dsl.pipeline(pipeline_root='dummy_root', name='v2-component-optional-input')
def pipeline():
    component_op(
        input1='Hello',
        input2='World',
    )


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=__file__.replace('.py', '.json'))
