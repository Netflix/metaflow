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
"""Placeholders for component inputs and outputs."""


def input_artifact_uri_placeholder(input_key: str) -> str:
    return "{{{{$.inputs.artifacts['{}'].uri}}}}".format(input_key)


def input_artifact_path_placeholder(input_key: str) -> str:
    return "{{{{$.inputs.artifacts['{}'].path}}}}".format(input_key)


def input_parameter_placeholder(input_key: str) -> str:
    return "{{{{$.inputs.parameters['{}']}}}}".format(input_key)


def output_artifact_uri_placeholder(output_key: str) -> str:
    return "{{{{$.outputs.artifacts['{}'].uri}}}}".format(output_key)


def output_artifact_path_placeholder(output_key: str) -> str:
    return "{{{{$.outputs.artifacts['{}'].path}}}}".format(output_key)


def output_parameter_path_placeholder(output_key: str) -> str:
    return "{{{{$.outputs.parameters['{}'].output_file}}}}".format(output_key)


def executor_input_placeholder() -> str:
    return "{{{{$}}}}"
