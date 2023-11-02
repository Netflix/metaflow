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
"""Deprecated. See kfp.v2.types.artifact_types instead.

This module will be removed in KFP v2.0.
"""
import warnings
from kfp.v2.components.types import artifact_types

warnings.warn(
    'Module kfp.dsl.io_types is deprecated and will be removed'
    ' in KFP v2.0. Please import types from kfp.v2.dsl instead.',
    category=FutureWarning)

Artifact = artifact_types.Artifact
Dataset = artifact_types.Dataset
Metrics = artifact_types.Metrics
ClassificationMetrics = artifact_types.ClassificationMetrics
Model = artifact_types.Model
SlicedClassificationMetrics = artifact_types.SlicedClassificationMetrics
HTML = artifact_types.HTML
Markdown = artifact_types.Markdown
create_runtime_artifact = artifact_types.create_runtime_artifact
