# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Builder for CAIP pipelines Pipeline level proto spec."""

import copy
import json
from typing import Any, Dict, Mapping, Optional, Union


class RuntimeConfigBuilder(object):
    """CAIP pipelines RuntimeConfig builder.

    Constructs a RuntimeConfig spec with pipeline_root and parameter
    overrides.
    """

    def __init__(
        self,
        pipeline_root: str,
        parameter_types: Mapping[str, str],
        parameter_values: Optional[Dict[str, Any]] = None,
    ):
        """Creates a RuntimeConfigBuilder object.

        Args:
          pipeline_root: The root of the pipeline outputs.
          parameter_types: The mapping from pipeline parameter name to its type.
          parameter_values: The mapping from runtime parameter name to its value.
        """
        self._pipeline_root = pipeline_root
        self._parameter_types = parameter_types
        self._parameter_values = copy.deepcopy(parameter_values or {})

    @classmethod
    def from_job_spec_json(
            cls, job_spec: Mapping[str, Any]) -> 'RuntimeConfigBuilder':
        """Creates a RuntimeConfigBuilder object from PipelineJob json spec.

        Args:
          job_spec: The PipelineJob spec.

        Returns:
          A RuntimeConfigBuilder object.
        """
        runtime_config_spec = job_spec['runtimeConfig']
        parameter_types = {}
        parameter_input_definitions = job_spec['pipelineSpec']['root'].get(
            'inputDefinitions', {}).get('parameters', {})
        for k, v in parameter_input_definitions.items():
            parameter_types[k] = v['type']

        pipeline_root = runtime_config_spec.get('gcsOutputDirectory')
        parameter_values = _parse_runtime_parameters(runtime_config_spec)
        return cls(pipeline_root, parameter_types, parameter_values)

    def update_pipeline_root(self, pipeline_root: Optional[str]) -> None:
        """Updates pipeline_root value.

        Args:
          pipeline_root: The root of the pipeline outputs.
        """
        if pipeline_root:
            self._pipeline_root = pipeline_root

    def update_runtime_parameters(
            self, parameter_values: Optional[Mapping[str, Any]]) -> None:
        """Merges runtime parameter values.

        Args:
          parameter_values: The mapping from runtime parameter names to its values.
        """
        if parameter_values:
            for k, v in parameter_values.items():
                if isinstance(v, (dict, list, bool)):
                    parameter_values[k] = json.dumps(v)
        if parameter_values:
            self._parameter_values.update(parameter_values)

    def build(self) -> Mapping[str, Any]:
        """Build a RuntimeConfig proto."""
        if not self._pipeline_root:
            raise ValueError(
                'Pipeline root must be specified, either during compile '
                'time, or when calling the service.')
        return {
            'gcsOutputDirectory': self._pipeline_root,
            'parameters': {
                k: self._get_caip_value(k, v)
                for k, v in self._parameter_values.items()
                if v is not None
            }
        }

    def _get_caip_value(self, name: str,
                        value: Union[int, float, str]) -> Mapping[str, Any]:
        """Converts primitive values into CAIP pipeline Value proto message.

        Args:
          name: The name of the pipeline parameter.
          value: The value of the pipeline parameter.

        Returns:
          A dictionary represents the CAIP pipeline Value proto message.

        Raises:
          AssertionError: if the value is None.
          ValueError: if the parameeter name is not found in pipeline root inputs.
          TypeError: if the paraemter type is not one of 'INT', 'DOUBLE', 'STRING'.
        """
        assert value is not None, 'None values should be filterd out.'

        if name not in self._parameter_types:
            raise ValueError(
                'The pipeline parameter {} is not found in the pipeline '
                'job input definitions.'.format(name))

        result = {}
        if self._parameter_types[name] == 'INT':
            result['intValue'] = value
        elif self._parameter_types[name] == 'DOUBLE':
            result['doubleValue'] = value
        elif self._parameter_types[name] == 'STRING':
            result['stringValue'] = value
        else:
            raise TypeError('Got unknown type of value: {}'.format(value))

        return result


def _parse_runtime_parameters(
        runtime_config_spec: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Extracts runtime parameters from runtime config json spec."""
    runtime_parameters = runtime_config_spec.get('parameters')
    if not runtime_parameters:
        return None

    result = {}
    for name, value in runtime_parameters.items():
        if 'intValue' in value:
            result[name] = int(value['intValue'])
        elif 'doubleValue' in value:
            result[name] = float(value['doubleValue'])
        elif 'stringValue' in value:
            result[name] = value['stringValue']
        else:
            raise TypeError('Got unknown type of value: {}'.format(value))

    return result
