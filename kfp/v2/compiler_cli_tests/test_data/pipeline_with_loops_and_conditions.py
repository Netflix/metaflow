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
def args_generator_op() -> list:
    return [
        {
            'A_a': '1',
            'B_b': ['2', '20'],
        },
        {
            'A_a': '10',
            'B_b': ['22', '222'],
        },
    ]


@component
def print_op(msg: str, msg2: Optional[str] = None):
    print(f'msg: {msg}, msg2: {msg2}')


@component
def flip_coin_op() -> str:
    """Flip a coin and output heads or tails randomly."""
    import random
    result = 'heads' if random.randint(0, 1) == 0 else 'tails'
    return result


@dsl.pipeline(name='pipeline-with-loops-and-conditions-multi-layers')
def my_pipeline(
    msg: str = 'hello',
    loop_parameter: list = [
        {
            'A_a': 'heads',
            'B_b': ['A', 'B'],
        },
        {
            'A_a': 'tails',
            'B_b': ['X', 'Y', 'Z'],
        },
    ],
):

    flip = flip_coin_op()
    outter_args_generator = args_generator_op()

    with dsl.Condition(flip.output != 'no-such-result'):  # always true

        inner_arg_generator = args_generator_op()

        with dsl.ParallelFor(outter_args_generator.output) as item:

            print_op(msg)

            with dsl.Condition(item.A_a == 'heads'):
                print_op(item.B_b)

            with dsl.Condition(flip.output == 'heads'):
                print_op(item.B_b)

            with dsl.Condition(item.A_a == 'tails'):
                with dsl.ParallelFor([{'a': '-1'}, {'a': '-2'}]) as inner_item:
                    print_op(inner_item)

            with dsl.ParallelFor(item.B_b) as item_b:
                print_op(item_b)

            with dsl.ParallelFor(loop_parameter) as pipeline_item:
                print_op(pipeline_item)

                with dsl.ParallelFor(inner_arg_generator.output) as inner_item:
                    print_op(pipeline_item, inner_item.A_a)

            with dsl.ParallelFor(['1', '2']) as static_item:
                print_op(static_item)

                with dsl.Condition(static_item == '1'):
                    print_op('1')

    # Reference loop item from grand child
    with dsl.ParallelFor(loop_parameter) as item:
        with dsl.Condition(item.A_a == 'heads'):
            with dsl.ParallelFor(item.B_b) as item_b:
                print_op(item_b)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
