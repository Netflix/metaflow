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

import kfp.v2.compiler as compiler
from kfp import components
from kfp.v2 import dsl
from kfp.v2.dsl import component


@component
def print_env_op():
    import os
    print(os.environ['ENV1'])


print_env_2_op = components.load_component_from_text("""
name: Print env
implementation:
  container:
    image: alpine
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$ENV2"
      echo "$ENV3"
    env:
      ENV2: val0
""")


@dsl.pipeline(name='pipeline-with-env', pipeline_root='dummy_root')
def my_pipeline():
    print_env_op().set_env_variable(name='ENV1', value='val1')
    print_env_2_op().set_env_variable(
        name='ENV2', value='val2').set_env_variable(
            name='ENV3', value='val3')


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
