# Copyright 2018-2019 The Kubeflow Authors
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

from ._pipeline_param import PipelineParam, match_serialized_pipelineparam
from ._pipeline import Pipeline, PipelineExecutionMode, pipeline, get_pipeline_conf, PipelineConf
from ._container_op import BaseOp, ContainerOp, InputArgumentPath, UserContainer, Sidecar
from ._resource_op import ResourceOp
from ._volume_op import VolumeOp, VOLUME_MODE_RWO, VOLUME_MODE_RWM, VOLUME_MODE_ROM
from ._pipeline_volume import PipelineVolume
from ._volume_snapshot_op import VolumeSnapshotOp
from ._ops_group import OpsGroup, ExitHandler, Condition, ParallelFor, SubGraph
from ._component import python_component, graph_component, component


def importer(*args, **kwargs):
    import warnings
    from kfp.v2.dsl import importer as v2importer
    warnings.warn(
        '`kfp.dsl.importer` is a deprecated alias and will be removed'
        ' in KFP v2.0. Please import from `kfp.v2.dsl` instead.',
        category=FutureWarning)
    return v2importer(*args, **kwargs)


EXECUTION_ID_PLACEHOLDER = '{{workflow.uid}}-{{pod.name}}'
RUN_ID_PLACEHOLDER = '{{workflow.uid}}'

ROOT_PARAMETER_NAME = 'pipeline-root'
