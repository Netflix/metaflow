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

from absl import logging
import importlib
import sys
from typing import Callable, Dict, Optional, Union
from google.protobuf import json_format

from kfp.components import _python_op
from kfp.containers import _gcs_helper
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.dsl import artifact

# If path starts with one of those, consider files are in remote filesystem.
_REMOTE_FS_PREFIX = ['gs://', 'hdfs://', 's3://']

# Constant user module name when importing the function from a Python file.
_USER_MODULE = 'user_module'


def get_parameter_from_output(file_path: str, param_name: str):
    """Gets a parameter value by its name from output metadata JSON."""
    output = pipeline_spec_pb2.ExecutorOutput()
    json_format.Parse(
        text=_gcs_helper.GCSHelper.read_from_gcs_path(file_path),
        message=output)
    value = output.parameters[param_name]
    return getattr(value, value.WhichOneof('value'))


def get_artifact_from_output(file_path: str,
                             output_name: str) -> artifact.Artifact:
    """Gets an artifact object from output metadata JSON."""
    output = pipeline_spec_pb2.ExecutorOutput()
    json_format.Parse(
        text=_gcs_helper.GCSHelper.read_from_gcs_path(file_path),
        message=output)
    # Currently we bear the assumption that each output contains only one artifact
    json_str = json_format.MessageToJson(
        output.artifacts[output_name].artifacts[0], sort_keys=True)

    # Convert runtime_artifact to Python artifact
    return artifact.Artifact.deserialize(json_str)


def import_func_from_source(source_path: str, fn_name: str) -> Callable:
    """Imports a function from a Python file.

    The implementation is borrowed from
    https://github.com/tensorflow/tfx/blob/8f25a4d1cc92dfc8c3a684dfc8b82699513cafb5/tfx/utils/import_utils.py#L50

    Args:
      source_path: The local path to the Python source file.
      fn_name: The function name, which can be found in the source file.

    Return: A Python function object.

    Raises:
      ImportError when failed to load the source file or cannot find the function
        with the given name.
    """
    if any([source_path.startswith(prefix) for prefix in _REMOTE_FS_PREFIX]):
        raise RuntimeError(
            'Only local source file can be imported. Please make '
            'sure the user code is built into executor container. '
            'Got file path: %s' % source_path)
    try:
        loader = importlib.machinery.SourceFileLoader(
            fullname=_USER_MODULE,
            path=source_path,
        )
        spec = importlib.util.spec_from_loader(
            loader.name, loader, origin=source_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[loader.name] = module
        loader.exec_module(module)
    except IOError:
        raise ImportError(
            '{} in {} not found in import_func_from_source()'.format(
                fn_name, source_path))
    try:
        return getattr(module, fn_name)
    except AttributeError:
        raise ImportError(
            '{} in {} not found in import_func_from_source()'.format(
                fn_name, source_path))


def get_output_artifacts(
        fn: Callable, output_uris: Dict[str,
                                        str]) -> Dict[str, artifact.Artifact]:
    """Gets the output artifacts from function signature and provided URIs.

    Args:
      fn: A user-provided function, whose signature annotates the type of output
        artifacts.
      output_uris: The mapping from output artifact name to its URI.

    Returns:
      A mapping from output artifact name to Python artifact objects.
    """
    # Inspect the function signature to determine the set of output artifact.
    spec = _python_op._extract_component_interface(fn)

    result = {}  # Mapping from output name to artifacts.
    for output in spec.outputs:
        if (getattr(output, '_passing_style',
                    None) == _python_op.OutputArtifact):
            # Creates an artifact according to its name
            type_name = getattr(output, 'type', None)
            if not type_name:
                continue

            try:
                artifact_cls = getattr(
                    importlib.import_module(
                        artifact.KFP_ARTIFACT_ONTOLOGY_MODULE), type_name)

            except (AttributeError, ImportError, ValueError):
                logging.warning((
                    'Could not load artifact class %s.%s; using fallback deserialization'
                    ' for the relevant artifact. Please make sure that any artifact '
                    'classes can be imported within your container or environment.'
                ), artifact.KFP_ARTIFACT_ONTOLOGY_MODULE, type_name)
                artifact_cls = artifact.Artifact

            if artifact_cls == artifact.Artifact:
                # Provide an empty schema if instantiating an bare-metal artifact.
                art = artifact_cls(
                    instance_schema=artifact.DEFAULT_ARTIFACT_SCHEMA)
            else:
                art = artifact_cls()

            art.uri = output_uris[output.name]
            result[output.name] = art

    return result


def _get_pipeline_value(
        value: Union[int, float, str]) -> Optional[pipeline_spec_pb2.Value]:
    """Converts Python primitive value to pipeline value pb."""
    if value is None:
        return None

    result = pipeline_spec_pb2.Value()
    if isinstance(value, int):
        result.int_value = value
    elif isinstance(value, float):
        result.double_value = value
    elif isinstance(value, str):
        result.string_value = value
    else:
        raise TypeError('Got unknown type of value: {}'.format(value))

    return result


def get_python_value(value: pipeline_spec_pb2.Value) -> Union[int, float, str]:
    """Gets Python value from pipeline value pb message."""
    return getattr(value, value.WhichOneof('value'))


def get_executor_output(
    output_artifacts: Dict[str, artifact.Artifact],
    output_params: Dict[str, Union[int, float, str]]
) -> pipeline_spec_pb2.ExecutorOutput:
    """Gets the output metadata message."""
    result = pipeline_spec_pb2.ExecutorOutput()

    for name, art in output_artifacts.items():
        result.artifacts[name].CopyFrom(
            pipeline_spec_pb2.ArtifactList(artifacts=[art.runtime_artifact]))

    for name, param in output_params.items():
        result.parameters[name].CopyFrom(_get_pipeline_value(param))

    return result
