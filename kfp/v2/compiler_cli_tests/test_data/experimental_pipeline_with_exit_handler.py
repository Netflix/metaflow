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
"""Pipeline using ExitHandler."""

import kfp.v2.components.experimental as components
import kfp.v2.dsl.experimental as dsl
from kfp.v2.compiler.experimental import compiler

print_op = components.load_component_from_text("""
name: print op
inputs:
- {name: msg, type: String}
implementation:
  container:
    image: alpine
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0"
    - {inputValue: msg}
""")

fail_op = components.load_component_from_text("""
name: fail op
inputs:
- {name: msg, type: String}
implementation:
  container:
    image: alpine
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0"
      exit 1
    - {inputValue: msg}
""")


@dsl.pipeline(name='pipeline-with-exit-handler')
def my_pipeline(message: str = 'Hello World!'):

    exit_task = print_op(msg='Exit handler has worked!')

    with dsl.ExitHandler(exit_task):
        print_op(msg=message)
        fail_op(msg='Task failed.')


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
