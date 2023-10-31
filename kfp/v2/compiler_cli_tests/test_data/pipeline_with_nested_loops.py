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
def print_op(msg: str, msg2: Optional[str] = None):
    print(f'msg: {msg}, msg2: {msg2}')


@dsl.pipeline(name='pipeline-with-nested-loops')
def my_pipeline(loop_parameter: list = [
    {
        "p_a": [{
            "q_a": 1
        }, {
            "q_a": 2
        }],
        "p_b": "hello",
    },
    {
        "p_a": [{
            "q_a": 11
        }, {
            "q_a": 22
        }],
        "p_b": "halo",
    },
]):
    # Nested loop with withParams loop args
    with dsl.ParallelFor(loop_parameter) as item:
        with dsl.ParallelFor(item.p_a) as item_p_a:
            print_op(item_p_a.q_a)

    # Nested loop with withItems loop args
    with dsl.ParallelFor([1, 2]) as outter_item:
        print_op(outter_item)
        with dsl.ParallelFor([100, 200, 300]) as inner_item:
            print_op(outter_item, inner_item)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
