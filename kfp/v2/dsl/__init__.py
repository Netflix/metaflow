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

from kfp.v2.components.component_decorator import component

from kfp.v2.components.importer_node import importer

from kfp.v2.components.types.artifact_types import (
    Artifact,
    ClassificationMetrics,
    Dataset,
    HTML,
    Markdown,
    Metrics,
    Model,
    SlicedClassificationMetrics,
)

from kfp.v2.components.types.type_annotations import (
    Input,
    Output,
    InputPath,
    OutputPath,
)

from kfp.dsl import (
    graph_component,
    pipeline,
    Condition,
    ContainerOp,
    ExitHandler,
    ParallelFor,
)

PIPELINE_JOB_NAME_PLACEHOLDER = '{{$.pipeline_job_name}}'
PIPELINE_JOB_RESOURCE_NAME_PLACEHOLDER = '{{$.pipeline_job_resource_name}}'
PIPELINE_JOB_ID_PLACEHOLDER = '{{$.pipeline_job_uuid}}'
PIPELINE_TASK_NAME_PLACEHOLDER = '{{$.pipeline_task_name}}'
PIPELINE_TASK_ID_PLACEHOLDER = '{{$.pipeline_task_uuid}}'