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
"""Pipeline with Metrics outputs."""

from typing import NamedTuple
from kfp import components
from kfp import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import component, Dataset, Input, Metrics, Output


@component
def output_metrics(metrics: Output[Metrics]):
    """Dummy component that outputs metrics with a random accuracy."""
    import random
    result = random.randint(0, 100)
    metrics.log_metric('accuracy', result)


@dsl.pipeline(name='pipeline-with-metrics-outputs', pipeline_root='dummy_root')
def my_pipeline():
    output_metrics()

    with dsl.ParallelFor([1, 2]) as item:
        output_metrics()


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.json'))
