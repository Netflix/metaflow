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
"""Lightweight functions v2 with outputs."""
from typing import NamedTuple
from kfp import components, dsl
from kfp.v2 import compiler
from kfp.v2.dsl import component, Input, Dataset, Model, Metrics


@component
def concat_message(first: str, second: str) -> str:
    return first + second


@component
def add_numbers(first: int, second: int) -> int:
    return first + second


@component
def output_artifact(number: int, message: str) -> Dataset:
    result = [message for _ in range(number)]
    return '\n'.join(result)


@component
def output_named_tuple(
    artifact: Input[Dataset]
) -> NamedTuple('Outputs', [
    ('scalar', str),
    ('metrics', Metrics),
    ('model', Model),
]):
    scalar = "123"

    import json
    metrics = json.dumps({
        'metrics': [{
            'name': 'accuracy',
            'numberValue': 0.9,
            'format': "PERCENTAGE",
        }]
    })

    with open(artifact.path, 'r') as f:
        artifact_contents = f.read()
    model = "Model contents: " + artifact_contents

    from collections import namedtuple
    output = namedtuple('Outputs', ['scalar', 'metrics', 'model'])
    return output(scalar, metrics, model)


@dsl.pipeline(pipeline_root='dummy_root', name='functions-with-outputs')
def pipeline(first_message: str, second_message: str, first_number: int,
             second_number: int):
    concat_op = concat_message(first=first_message, second=second_message)
    add_numbers_op = add_numbers(first=first_number, second=second_number)
    output_artifact_op = output_artifact(
        number=add_numbers_op.output, message=concat_op.output)
    output_name_tuple_op = output_named_tuple(output_artifact_op.output)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=__file__.replace('.py', '.json'))
