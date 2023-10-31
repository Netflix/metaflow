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

from kfp import components
from kfp import dsl
import kfp.v2.compiler as compiler


def random_num_op(low, high):
    """Generate a random number between low and high."""
    return components.load_component_from_text("""
      name: Generate random number
      outputs:
      - {name: output, type: Integer}
      implementation:
        container:
          image: python:alpine3.6
          command:
          - sh
          - -c
          args:
          - mkdir -p "$(dirname $2)" && python -c "import random; print(random.randint($0, $1), end='')" | tee $2
          - "%s"
          - "%s"
          - {outputPath: output}
      """ % (low, high))


flip_coin_op = components.load_component_from_text("""
      name: Flip coin
      outputs:
      - {name: output, type: String}
      implementation:
        container:
          image: python:alpine3.6
          command:
          - sh
          - -c
          args:
          - mkdir -p "$(dirname $0)" && python -c "import random; result = \'heads\' if random.randint(0,1) == 0 else \'tails\'; print(result, end='')" | tee $0
          - {outputPath: output}
      """)

print_op = components.load_component_from_text("""
      name: Print
      inputs:
      - {name: msg, type: String}
      implementation:
        container:
          image: python:alpine3.6
          command:
          - echo
          - {inputValue: msg}
      """)


@dsl.pipeline(
    name='conditional-execution-pipeline',
    pipeline_root='dummy_root',
    description='Shows how to use dsl.Condition().')
def my_pipeline():
    flip = flip_coin_op()
    with dsl.Condition(flip.output == 'heads'):
        random_num_head = random_num_op(0, 9)()
        with dsl.Condition(random_num_head.output > 5):
            print_op('heads and %s > 5!' % random_num_head.output)
        with dsl.Condition(random_num_head.output <= 5):
            print_op('heads and %s <= 5!' % random_num_head.output)

    with dsl.Condition(flip.output == 'tails'):
        random_num_tail = random_num_op(10, 19)()
        with dsl.Condition(random_num_tail.output > 15):
            print_op('tails and %s > 15!' % random_num_tail.output)
        with dsl.Condition(random_num_tail.output <= 15):
            print_op('tails and %s <= 15!' % random_num_tail.output)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
