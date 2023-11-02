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

from typing import List

from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component


@component
def args_generator_op() -> List[str]:
    return [{'A_a': '1', 'B_b': '2'}, {'A_a': '10', 'B_b': '20'}]


@component
def print_op(msg: str):
    print(msg)


@dsl.pipeline(name='pipeline-with-loops')
def my_pipeline(loop_parameter: List[str]):

    # Loop argument is from a pipeline input
    with dsl.ParallelFor(loop_parameter) as item:
        print_op(item)

    # Loop argument is from a component output
    args_generator = args_generator_op()
    with dsl.ParallelFor(args_generator.output) as item:
        print_op(item)
        print_op(item.A_a)
        print_op(item.B_b)

    # Loop argument is a static value known at compile time
    loop_args = [{'A_a': '1', 'B_b': '2'}, {'A_a': '10', 'B_b': '20'}]
    with dsl.ParallelFor(loop_args) as item:
        print_op(item)
        print_op(item.A_a)
        print_op(item.B_b)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
