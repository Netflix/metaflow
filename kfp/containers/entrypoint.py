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

from typing import Dict, NamedTuple, Optional, Union

from absl import logging
import fire
from google.protobuf import json_format
import os

from kfp.containers import _gcs_helper
from kfp.containers import entrypoint_utils
from kfp.dsl import artifact
from kfp.pipeline_spec import pipeline_spec_pb2

FN_SOURCE = 'ml/main.py'
FN_NAME_ARG = 'function_name'

PARAM_METADATA_SUFFIX = '_input_param_metadata_file'
ARTIFACT_METADATA_SUFFIX = '_input_artifact_metadata_file'
FIELD_NAME_SUFFIX = '_input_field_name'
ARGO_PARAM_SUFFIX = '_input_argo_param'
INPUT_URI_SUFFIX = '_input_uri'
PRODUCER_POD_ID_SUFFIX = '_pod_id'
OUTPUT_NAME_SUFFIX = '_input_output_name'

OUTPUT_PARAM_PATH_SUFFIX = '_parameter_output_path'
OUTPUT_ARTIFACT_PATH_SUFFIX = '_artifact_output_uri'

METADATA_FILE_ARG = 'executor_metadata_json_file'


class InputParam(object):
    """POD that holds an input parameter."""

    def __init__(self,
                 value: Optional[Union[str, float, int]] = None,
                 metadata_file: Optional[str] = None,
                 field_name: Optional[str] = None):
        """Instantiates an InputParam object.

        Args:
          value: The actual value of the parameter.
          metadata_file: The location of the metadata JSON file output by the
            producer step.
          field_name: The output name of the producer.

        Raises:
          ValueError: when neither of the following is true:
            1) value is provided, and metadata_file and field_name are not; or
            2) both metadata_file and field_name are provided, and value is not.
        """
        if not (value is not None and not (metadata_file or field_name) or
                (metadata_file and field_name and value is None)):
            raise ValueError(
                'Either value or both metadata_file and field_name '
                'needs to be provided. Got value={value}, field_name='
                '{field_name}, metadata_file={metadata_file}'.format(
                    value=value,
                    field_name=field_name,
                    metadata_file=metadata_file))
        if value is not None:
            self._value = value
        else:
            # Parse the value by inspecting the producer's metadata JSON file.
            self._value = entrypoint_utils.get_parameter_from_output(
                metadata_file, field_name)

        self._metadata_file = metadata_file
        self._field_name = field_name

    # Following properties are read-only
    @property
    def value(self) -> Union[float, str, int]:
        return self._value

    @property
    def metadata_file(self) -> str:
        return self._metadata_file

    @property
    def field_name(self) -> str:
        return self._field_name


class InputArtifact(object):
    """POD that holds an input artifact."""

    def __init__(self,
                 uri: Optional[str] = None,
                 metadata_file: Optional[str] = None,
                 output_name: Optional[str] = None):
        """Instantiates an InputParam object.

        Args:
          uri: The uri holds the input artifact.
          metadata_file: The location of the metadata JSON file output by the
            producer step.
          output_name: The output name of the artifact in producer step.

        Raises:
          ValueError: when neither of the following is true:
            1) uri is provided, and metadata_file and output_name are not; or
            2) both metadata_file and output_name are provided, and uri is not.
        """
        if not ((uri and not (metadata_file or output_name) or
                 (metadata_file and output_name and not uri))):
            raise ValueError(
                'Either uri or both metadata_file and output_name '
                'needs to be provided. Got uri={uri}, output_name='
                '{output_name}, metadata_file={metadata_file}'.format(
                    uri=uri,
                    output_name=output_name,
                    metadata_file=metadata_file))

        self._metadata_file = metadata_file
        self._output_name = output_name
        if uri:
            self._uri = uri
        else:
            self._uri = self.get_artifact().uri

    # Following properties are read-only.
    @property
    def uri(self) -> str:
        return self._uri

    @property
    def metadata_file(self) -> str:
        return self._metadata_file

    @property
    def output_name(self) -> str:
        return self._output_name

    def get_artifact(self) -> artifact.Artifact:
        """Gets an artifact object by parsing metadata or creating one from
        uri."""
        if self.metadata_file and self.output_name:
            return entrypoint_utils.get_artifact_from_output(
                self.metadata_file, self.output_name)
        else:
            # Provide an empty schema when returning a raw Artifact.
            result = artifact.Artifact(
                instance_schema=artifact.DEFAULT_ARTIFACT_SCHEMA)
            result.uri = self.uri
            return result


def _write_output_metadata_file(fn_res: Union[int, str, float, NamedTuple],
                                output_artifacts: Dict[str, artifact.Artifact],
                                output_metadata_path: str):
    """Writes the output metadata file to the designated place."""
    # If output_params is a singleton value, needs to transform it to a mapping.
    output_parameters = {}
    if isinstance(fn_res, (int, str, float)):
        output_parameters['output'] = fn_res
    else:
        # When multiple outputs, we'll need to match each field to the output paths.
        for idx, output_name in enumerate(fn_res._fields):
            output_parameters[output_name] = fn_res[idx]

    executor_output = entrypoint_utils.get_executor_output(
        output_artifacts=output_artifacts, output_params=output_parameters)

    with open(output_metadata_path, 'w') as f:
        f.write(json_format.MessageToJson(executor_output))

    return executor_output


def main(executor_input_str: str,
         function_name: str,
         output_metadata_path: Optional[str] = None):
    """Container entrypoint used by KFP Python function based component.

    executor_input_str: A serialized ExecutorInput proto message.
    function_name: The name of the user-defined function.
    output_metadata_path: A local path where the output metadata JSON file should
      be written to.
    """
    executor_input = pipeline_spec_pb2.ExecutorInput()
    json_format.Parse(text=executor_input_str, message=executor_input)
    output_metadata_path = output_metadata_path or executor_input.outputs.output_file
    parameter_dict = {}  # kwargs to be passed to UDF.
    for name, input_param in executor_input.inputs.parameters.items():
        parameter_dict[name] = entrypoint_utils.get_python_value(input_param)

    for name, input_artifacts in executor_input.inputs.artifacts.items():
        parameter_dict[name] = artifact.Artifact.get_from_runtime_artifact(
            input_artifacts.artifacts[0])

    # Also, determine a way to inspect the function signature to decide the type
    # of output artifacts.
    fn = entrypoint_utils.import_func_from_source(FN_SOURCE, function_name)

    # In the ExeuctorInput message passed into the entrypoint, the output artifact
    # URIs are already specified. The output artifact is constructed according to
    # the specified URIs + type information retrieved from function signature.
    output_uris = {}
    for name, output_artifacts in executor_input.outputs.artifacts.items():
        output_uris[name] = output_artifacts.artifacts[0].uri

    output_artifacts = entrypoint_utils.get_output_artifacts(fn, output_uris)
    for name, art in output_artifacts.items():
        parameter_dict[name] = art

    # Execute the user function. fn_res is expected to contain output parameters
    # only. It's either an namedtuple or a single primitive value.
    fn_res = fn(**parameter_dict)

    _write_output_metadata_file(
        fn_res=fn_res,
        output_artifacts=output_artifacts,
        output_metadata_path=output_metadata_path)


if __name__ == '__main__':
    fire.Fire(main)
