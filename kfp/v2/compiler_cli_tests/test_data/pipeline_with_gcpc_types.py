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
from kfp.v2.dsl import component, Input, Output
from kfp.v2 import compiler
from kfp.v2 import dsl


class VertexModel(dsl.Artifact):
    TYPE_NAME = 'google.VertexModel'


producer_op = components.load_component_from_text("""
name: producer
outputs:
  - {name: model, type: google.VertexModel}
implementation:
  container:
    image: dummy
    command:
    - cmd
    args:
    - {outputPath: model}
""")


@component
def consumer_op(model: Input[VertexModel]):
    pass


@dsl.pipeline(name='pipeline-with-gcpc-types')
def my_pipeline():
    consumer_op(model=producer_op().outputs['model'])


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
