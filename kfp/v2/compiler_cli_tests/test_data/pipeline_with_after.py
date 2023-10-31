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

from kfp import components
from kfp.v2 import dsl
import kfp.v2.compiler as compiler

component_op = components.load_component_from_text("""
name: Print Text
inputs:
- {name: text, type: String}
implementation:
  container:
    image: alpine
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0"
    - {inputValue: text}
""")


@dsl.pipeline(name='pipeline-with-after', pipeline_root='dummy_root')
def my_pipeline():
    task1 = component_op(text='1st task')
    task2 = component_op(text='2nd task').after(task1)
    task3 = component_op(text='3rd task').after(task1, task2)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
