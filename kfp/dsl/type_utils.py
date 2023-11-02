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
"""Deprecated. See kfp.v2.components.types.type_utils instead.

This module will be removed in KFP v2.0.
"""
import warnings
from kfp.v2.components.types import type_utils

warnings.warn(
    'Module kfp.dsl.type_utils is deprecated and will be removed'
    ' in KFP v2.0. Please use from kfp.v2.components.types.type_utils instead.',
    category=FutureWarning)

is_parameter_type = type_utils.is_parameter_type
get_artifact_type_schema = type_utils.get_artifact_type_schema
get_parameter_type = type_utils.get_parameter_type
get_parameter_type_field_name = type_utils.get_parameter_type_field_name
get_input_artifact_type_schema = type_utils.get_input_artifact_type_schema
