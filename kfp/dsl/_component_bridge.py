# Copyright 2018 The Kubeflow Authors
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

import collections
import copy
import inspect
import json
import pathlib
from typing import Any, Mapping, Optional

import kfp
from kfp.components import _structures
from kfp.components import _components
from kfp.components import _naming
from kfp import dsl
from kfp.dsl import _container_op
from kfp.dsl import _for_loop
from kfp.dsl import _pipeline_param
from kfp.dsl import component_spec as dsl_component_spec
from kfp.dsl import dsl_utils
from kfp.dsl import types
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2.components.types import type_utils

# Placeholder to represent the output directory hosting all the generated URIs.
# Its actual value will be specified during pipeline compilation.
# The format of OUTPUT_DIR_PLACEHOLDER is serialized dsl.PipelineParam, to
# ensure being extracted as a pipeline parameter during compilation.
# Note that we cannot direclty import dsl module here due to circular
# dependencies.
OUTPUT_DIR_PLACEHOLDER = '{{pipelineparam:op=;name=pipeline-root}}'
# Placeholder to represent to UID of the current pipeline at runtime.
# Will be replaced by engine-specific placeholder during compilation.
RUN_ID_PLACEHOLDER = '{{kfp.run_uid}}'
# Format of the Argo parameter used to pass the producer's Pod ID to
# the consumer.
PRODUCER_POD_NAME_PARAMETER = '{}-producer-pod-id-'
# Format of the input output port name placeholder.
INPUT_OUTPUT_NAME_PATTERN = '{{{{kfp.input-output-name.{}}}}}'
# Fixed name for per-task output metadata json file.
OUTPUT_METADATA_JSON = '/tmp/outputs/executor_output.json'
# Executor input placeholder.
_EXECUTOR_INPUT_PLACEHOLDER = '{{$}}'


# TODO(chensun): block URI placeholder usage in v1.
def _generate_output_uri_placeholder(port_name: str) -> str:
    """Generates the URI placeholder for an output."""
    return "{{{{$.outputs.artifacts['{}'].uri}}}}".format(port_name)


def _generate_input_uri_placeholder(port_name: str) -> str:
    """Generates the URI placeholder for an input."""
    return "{{{{$.inputs.artifacts['{}'].uri}}}}".format(port_name)


def _generate_output_metadata_path() -> str:
    """Generates the URI to write the output metadata JSON file."""

    return OUTPUT_METADATA_JSON


def _generate_input_metadata_path(port_name: str) -> str:
    """Generates the placeholder for input artifact metadata file."""

    # Return a placeholder for path to input artifact metadata, which will be
    # rewritten during pipeline compilation.
    return str(
        pathlib.PurePosixPath(
            OUTPUT_DIR_PLACEHOLDER, RUN_ID_PLACEHOLDER,
            '{{{{inputs.parameters.{input}}}}}'.format(
                input=PRODUCER_POD_NAME_PARAMETER.format(port_name)),
            OUTPUT_METADATA_JSON))


def _generate_input_output_name(port_name: str) -> str:
    """Generates the placeholder for input artifact's output name."""

    # Return a placeholder for the output port name of the input artifact, which
    # will be rewritten during pipeline compilation.
    return INPUT_OUTPUT_NAME_PATTERN.format(port_name)


def _generate_executor_input() -> str:
    """Generates the placeholder for serialized executor input."""
    return _EXECUTOR_INPUT_PLACEHOLDER


class ExtraPlaceholderResolver:

    def __init__(self):
        self.input_paths = {}
        self.input_metadata_paths = {}
        self.output_paths = {}

    def resolve_placeholder(
        self,
        arg,
        component_spec: _structures.ComponentSpec,
        arguments: dict,
    ) -> str:
        inputs_dict = {
            input_spec.name: input_spec
            for input_spec in component_spec.inputs or []
        }

        if isinstance(arg, _structures.InputUriPlaceholder):
            input_name = arg.input_name
            if input_name in arguments:
                input_uri = _generate_input_uri_placeholder(input_name)
                self.input_paths[
                    input_name] = _components._generate_input_file_name(
                        input_name)
                return input_uri
            else:
                input_spec = inputs_dict[input_name]
                if input_spec.optional:
                    return None
                else:
                    raise ValueError(
                        'No value provided for input {}'.format(input_name))

        elif isinstance(arg, _structures.OutputUriPlaceholder):
            output_name = arg.output_name
            output_uri = _generate_output_uri_placeholder(output_name)
            self.output_paths[
                output_name] = _components._generate_output_file_name(
                    output_name)
            return output_uri

        elif isinstance(arg, _structures.InputMetadataPlaceholder):
            input_name = arg.input_name
            if input_name in arguments:
                input_metadata_path = _generate_input_metadata_path(input_name)
                self.input_metadata_paths[input_name] = input_metadata_path
                return input_metadata_path
            else:
                input_spec = inputs_dict[input_name]
                if input_spec.optional:
                    return None
                else:
                    raise ValueError(
                        'No value provided for input {}'.format(input_name))

        elif isinstance(arg, _structures.InputOutputPortNamePlaceholder):
            input_name = arg.input_name
            if input_name in arguments:
                return _generate_input_output_name(input_name)
            else:
                input_spec = inputs_dict[input_name]
                if input_spec.optional:
                    return None
                else:
                    raise ValueError(
                        'No value provided for input {}'.format(input_name))

        elif isinstance(arg, _structures.OutputMetadataPlaceholder):
            # TODO: Consider making the output metadata per-artifact.
            return _generate_output_metadata_path()
        elif isinstance(arg, _structures.ExecutorInputPlaceholder):
            return _generate_executor_input()

        return None


def _create_container_op_from_component_and_arguments(
    component_spec: _structures.ComponentSpec,
    arguments: Mapping[str, Any],
    component_ref: Optional[_structures.ComponentReference] = None,
) -> _container_op.ContainerOp:
    """Instantiates ContainerOp object.

    Args:
      component_spec: The component spec object.
      arguments: The dictionary of component arguments.
      component_ref: (only for v1) The component references.

    Returns:
      A ContainerOp instance.
    """
    # Add component inputs with default value to the arguments dict if they are not
    # in the arguments dict already.
    arguments = arguments.copy()
    for input_spec in component_spec.inputs or []:
        if input_spec.name not in arguments and input_spec.default is not None:
            default_value = input_spec.default
            if input_spec.type == 'Integer':
                default_value = int(default_value)
            elif input_spec.type == 'Float':
                default_value = float(default_value)
            arguments[input_spec.name] = default_value

    # Check types of the reference arguments and serialize PipelineParams
    original_arguments = arguments
    arguments = arguments.copy()
    for input_name, argument_value in arguments.items():
        if isinstance(argument_value, _pipeline_param.PipelineParam):
            input_type = component_spec._inputs_dict[input_name].type
            argument_type = argument_value.param_type
            types.verify_type_compatibility(
                argument_type, input_type,
                'Incompatible argument passed to the input "{}" of component "{}": '
                .format(input_name, component_spec.name))

            arguments[input_name] = str(argument_value)
        if isinstance(argument_value, _container_op.ContainerOp):
            raise TypeError(
                'ContainerOp object was passed to component as an input argument. '
                'Pass a single output instead.')
    placeholder_resolver = ExtraPlaceholderResolver()
    resolved_cmd = _components._resolve_command_line_and_paths(
        component_spec=component_spec,
        arguments=arguments,
        placeholder_resolver=placeholder_resolver.resolve_placeholder,
    )

    container_spec = component_spec.implementation.container

    old_warn_value = _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING
    _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING = True

    output_paths = collections.OrderedDict(resolved_cmd.output_paths or {})
    output_paths.update(placeholder_resolver.output_paths)
    input_paths = collections.OrderedDict(resolved_cmd.input_paths or {})
    input_paths.update(placeholder_resolver.input_paths)

    artifact_argument_paths = [
        dsl.InputArgumentPath(
            argument=arguments[input_name],
            input=input_name,
            path=path,
        ) for input_name, path in input_paths.items()
    ]

    task = _container_op.ContainerOp(
        name=component_spec.name or _components._default_component_name,
        image=container_spec.image,
        command=resolved_cmd.command,
        arguments=resolved_cmd.args,
        file_outputs=output_paths,
        artifact_argument_paths=artifact_argument_paths,
    )
    _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING = old_warn_value

    component_meta = copy.copy(component_spec)
    task._set_metadata(component_meta, original_arguments)
    if component_ref:
        component_ref_without_spec = copy.copy(component_ref)
        component_ref_without_spec.spec = None
        task._component_ref = component_ref_without_spec

    task._parameter_arguments = resolved_cmd.inputs_consumed_by_value
    name_to_spec_type = {}
    if component_meta.inputs:
        name_to_spec_type = {
            input.name: input.type for input in component_meta.inputs
        }
    if kfp.COMPILING_FOR_V2:
        for name, spec_type in name_to_spec_type.items():
            if (name in original_arguments and
                    type_utils.is_parameter_type(spec_type)):
                task._parameter_arguments[name] = str(original_arguments[name])

    for name in list(task.artifact_arguments.keys()):
        if name in task._parameter_arguments:
            del task.artifact_arguments[name]

    for name in list(task.input_artifact_paths.keys()):
        if name in task._parameter_arguments:
            del task.input_artifact_paths[name]

    # Previously, ContainerOp had strict requirements for the output names, so we
    # had to convert all the names before passing them to the ContainerOp
    # constructor.
    # Outputs with non-pythonic names could not be accessed using their original
    # names. Now ContainerOp supports any output names, so we're now using the
    # original output names. However to support legacy pipelines, we're also
    # adding output references with pythonic names.
    # TODO: Add warning when people use the legacy output names.
    output_names = [
        output_spec.name for output_spec in component_spec.outputs or []
    ]  # Stabilizing the ordering
    output_name_to_python = _naming.generate_unique_name_conversion_table(
        output_names, _naming._sanitize_python_function_name)
    for output_name in output_names:
        pythonic_output_name = output_name_to_python[output_name]
        # Note: Some component outputs are currently missing from task.outputs
        # (e.g. MLPipeline UI Metadata)
        if pythonic_output_name not in task.outputs and output_name in task.outputs:
            task.outputs[pythonic_output_name] = task.outputs[output_name]

    if container_spec.env:
        from kubernetes import client as k8s_client
        for name, value in container_spec.env.items():
            task.container.add_env_variable(
                k8s_client.V1EnvVar(name=name, value=value))

    if component_spec.metadata:
        annotations = component_spec.metadata.annotations or {}
        for key, value in annotations.items():
            task.add_pod_annotation(key, value)
        for key, value in (component_spec.metadata.labels or {}).items():
            task.add_pod_label(key, value)
        # Disabling the caching for the volatile components by default
        if annotations.get('volatile_component', 'false') == 'true':
            task.execution_options.caching_strategy.max_cache_staleness = 'P0D'

    _attach_v2_specs(task, component_spec, original_arguments)

    return task


def _attach_v2_specs(
    task: _container_op.ContainerOp,
    component_spec: _structures.ComponentSpec,
    arguments: Mapping[str, Any],
) -> None:
    """Attaches v2 specs to a ContainerOp object.

    Attach v2_specs to the ContainerOp object regardless whether the pipeline is
    being compiled to v1 (Argo yaml) or v2 (IR json).
    However, there're different behaviors for the two cases. Namely, resolved
    commands and arguments, error handling, etc.
    Regarding the difference in error handling, v2 has a stricter requirement on
    input type annotation. For instance, an input without any type annotation is
    viewed as an artifact, and if it's paired with InputValuePlaceholder, an
    error will be thrown at compile time. However, we cannot raise such an error
    in v1, as it wouldn't break existing pipelines.

    Args:
      task: The ContainerOp object to attach IR specs.
      component_spec: The component spec object.
      arguments: The dictionary of component arguments.
    """

    def _resolve_commands_and_args_v2(
        component_spec: _structures.ComponentSpec,
        arguments: Mapping[str, Any],
    ) -> _components._ResolvedCommandLineAndPaths:
        """Resolves the command line argument placeholders for v2 (IR).

        Args:
          component_spec: The component spec object.
          arguments: The dictionary of component arguments.

        Returns:
          A named tuple: _components._ResolvedCommandLineAndPaths.
        """
        inputs_dict = {
            input_spec.name: input_spec
            for input_spec in component_spec.inputs or []
        }
        outputs_dict = {
            output_spec.name: output_spec
            for output_spec in component_spec.outputs or []
        }

        def _input_artifact_uri_placeholder(input_key: str) -> str:
            if kfp.COMPILING_FOR_V2 and type_utils.is_parameter_type(
                    inputs_dict[input_key].type):
                raise TypeError(
                    'Input "{}" with type "{}" cannot be paired with '
                    'InputUriPlaceholder.'.format(input_key,
                                                  inputs_dict[input_key].type))
            else:
                return _generate_input_uri_placeholder(input_key)

        def _input_artifact_path_placeholder(input_key: str) -> str:
            if kfp.COMPILING_FOR_V2 and type_utils.is_parameter_type(
                    inputs_dict[input_key].type):
                raise TypeError(
                    'Input "{}" with type "{}" cannot be paired with '
                    'InputPathPlaceholder.'.format(input_key,
                                                   inputs_dict[input_key].type))
            else:
                return "{{{{$.inputs.artifacts['{}'].path}}}}".format(input_key)

        def _input_parameter_placeholder(input_key: str) -> str:
            if kfp.COMPILING_FOR_V2 and not type_utils.is_parameter_type(
                    inputs_dict[input_key].type):
                raise TypeError(
                    'Input "{}" with type "{}" cannot be paired with '
                    'InputValuePlaceholder.'.format(
                        input_key, inputs_dict[input_key].type))
            else:
                return "{{{{$.inputs.parameters['{}']}}}}".format(input_key)

        def _output_artifact_uri_placeholder(output_key: str) -> str:
            if kfp.COMPILING_FOR_V2 and type_utils.is_parameter_type(
                    outputs_dict[output_key].type):
                raise TypeError(
                    'Output "{}" with type "{}" cannot be paired with '
                    'OutputUriPlaceholder.'.format(
                        output_key, outputs_dict[output_key].type))
            else:
                return _generate_output_uri_placeholder(output_key)

        def _output_artifact_path_placeholder(output_key: str) -> str:
            return "{{{{$.outputs.artifacts['{}'].path}}}}".format(output_key)

        def _output_parameter_path_placeholder(output_key: str) -> str:
            return "{{{{$.outputs.parameters['{}'].output_file}}}}".format(
                output_key)

        def _resolve_output_path_placeholder(output_key: str) -> str:
            if type_utils.is_parameter_type(outputs_dict[output_key].type):
                return _output_parameter_path_placeholder(output_key)
            else:
                return _output_artifact_path_placeholder(output_key)

        placeholder_resolver = ExtraPlaceholderResolver()

        def _resolve_ir_placeholders_v2(
            arg,
            component_spec: _structures.ComponentSpec,
            arguments: dict,
        ) -> str:
            inputs_dict = {
                input_spec.name: input_spec
                for input_spec in component_spec.inputs or []
            }
            if isinstance(arg, _structures.InputValuePlaceholder):
                input_name = arg.input_name
                input_value = arguments.get(input_name, None)
                if input_value is not None:
                    return _input_parameter_placeholder(input_name)
                else:
                    input_spec = inputs_dict[input_name]
                    if input_spec.optional:
                        return None
                    else:
                        raise ValueError(
                            'No value provided for input {}'.format(input_name))

            elif isinstance(arg, _structures.InputUriPlaceholder):
                input_name = arg.input_name
                if input_name in arguments:
                    input_uri = _input_artifact_uri_placeholder(input_name)
                    return input_uri
                else:
                    input_spec = inputs_dict[input_name]
                    if input_spec.optional:
                        return None
                    else:
                        raise ValueError(
                            'No value provided for input {}'.format(input_name))

            elif isinstance(arg, _structures.OutputUriPlaceholder):
                output_name = arg.output_name
                output_uri = _output_artifact_uri_placeholder(output_name)
                return output_uri

            return placeholder_resolver.resolve_placeholder(
                arg=arg,
                component_spec=component_spec,
                arguments=arguments,
            )

        resolved_cmd = _components._resolve_command_line_and_paths(
            component_spec=component_spec,
            arguments=arguments,
            input_path_generator=_input_artifact_path_placeholder,
            output_path_generator=_resolve_output_path_placeholder,
            placeholder_resolver=_resolve_ir_placeholders_v2,
        )
        return resolved_cmd

    pipeline_task_spec = pipeline_spec_pb2.PipelineTaskSpec()

    # Check types of the reference arguments and serialize PipelineParams
    arguments = arguments.copy()

    # Preserve input params for ContainerOp.inputs
    input_params_set = set([
        param for param in arguments.values()
        if isinstance(param, _pipeline_param.PipelineParam)
    ])

    for input_name, argument_value in arguments.items():
        input_type = component_spec._inputs_dict[input_name].type
        argument_type = None

        if isinstance(argument_value, _pipeline_param.PipelineParam):
            argument_type = argument_value.param_type

            types.verify_type_compatibility(
                argument_type, input_type,
                'Incompatible argument passed to the input "{}" of component "{}": '
                .format(input_name, component_spec.name))

            # Loop arguments defaults to 'String' type if type is unknown.
            # This has to be done after the type compatiblity check.
            if argument_type is None and isinstance(
                    argument_value,
                (_for_loop.LoopArguments, _for_loop.LoopArgumentVariable)):
                argument_type = 'String'

            arguments[input_name] = str(argument_value)

            if type_utils.is_parameter_type(input_type):
                if argument_value.op_name:
                    pipeline_task_spec.inputs.parameters[
                        input_name].task_output_parameter.producer_task = (
                            dsl_utils.sanitize_task_name(
                                argument_value.op_name))
                    pipeline_task_spec.inputs.parameters[
                        input_name].task_output_parameter.output_parameter_key = (
                            argument_value.name)
                else:
                    pipeline_task_spec.inputs.parameters[
                        input_name].component_input_parameter = argument_value.name
            else:
                if argument_value.op_name:
                    pipeline_task_spec.inputs.artifacts[
                        input_name].task_output_artifact.producer_task = (
                            dsl_utils.sanitize_task_name(
                                argument_value.op_name))
                    pipeline_task_spec.inputs.artifacts[
                        input_name].task_output_artifact.output_artifact_key = (
                            argument_value.name)
        elif isinstance(argument_value, str):
            argument_type = 'String'
            pipeline_params = _pipeline_param.extract_pipelineparams_from_any(
                argument_value)
            if pipeline_params and kfp.COMPILING_FOR_V2:
                # argument_value contains PipelineParam placeholders which needs to be
                # replaced. And the input needs to be added to the task spec.
                for param in pipeline_params:
                    # Form the name for the compiler injected input, and make sure it
                    # doesn't collide with any existing input names.
                    additional_input_name = (
                        dsl_component_spec
                        .additional_input_name_for_pipelineparam(param))
                    for existing_input_name, _ in arguments.items():
                        if existing_input_name == additional_input_name:
                            raise ValueError(
                                'Name collision between existing input name '
                                '{} and compiler injected input name {}'.format(
                                    existing_input_name, additional_input_name))

                    # Add the additional param to the input params set. Otherwise, it will
                    # not be included when the params set is not empty.
                    input_params_set.add(param)
                    additional_input_placeholder = (
                        "{{{{$.inputs.parameters['{}']}}}}".format(
                            additional_input_name))
                    argument_value = argument_value.replace(
                        param.pattern, additional_input_placeholder)

                    # The output references are subject to change -- the producer task may
                    # not be whitin the same DAG.
                    if param.op_name:
                        pipeline_task_spec.inputs.parameters[
                            additional_input_name].task_output_parameter.producer_task = (
                                dsl_utils.sanitize_task_name(param.op_name))
                        pipeline_task_spec.inputs.parameters[
                            additional_input_name].task_output_parameter.output_parameter_key = param.name
                    else:
                        pipeline_task_spec.inputs.parameters[
                            additional_input_name].component_input_parameter = param.full_name

            input_type = component_spec._inputs_dict[input_name].type
            if type_utils.is_parameter_type(input_type):
                pipeline_task_spec.inputs.parameters[
                    input_name].runtime_value.constant_value.string_value = (
                        argument_value)
        elif isinstance(argument_value, int):
            argument_type = 'Integer'
            pipeline_task_spec.inputs.parameters[
                input_name].runtime_value.constant_value.int_value = (
                    argument_value)
        elif isinstance(argument_value, float):
            argument_type = 'Float'
            pipeline_task_spec.inputs.parameters[
                input_name].runtime_value.constant_value.double_value = (
                    argument_value)
        elif isinstance(argument_value,
                        (dict, list, bool)) and kfp.COMPILING_FOR_V2:
            argument_type = type(argument_value).__name__
            pipeline_task_spec.inputs.parameters[
                input_name].runtime_value.constant_value.string_value = (
                    json.dumps(argument_value))
        elif isinstance(argument_value, _container_op.ContainerOp):
            raise TypeError(
                f'ContainerOp object {input_name} was passed to component as an '
                'input argument. Pass a single output instead.')
        else:
            if kfp.COMPILING_FOR_V2:
                raise NotImplementedError(
                    'Input argument supports only the following types: '
                    'PipelineParam, str, int, float, bool, dict, and list. Got: '
                    f'"{argument_value}".')

        argument_is_parameter_type = type_utils.is_parameter_type(argument_type)
        input_is_parameter_type = type_utils.is_parameter_type(input_type)
        if kfp.COMPILING_FOR_V2 and (argument_is_parameter_type !=
                                     input_is_parameter_type):
            if isinstance(argument_value, dsl.PipelineParam):
                param_or_value_msg = 'PipelineParam "{}"'.format(
                    argument_value.full_name)
            else:
                param_or_value_msg = 'value "{}"'.format(argument_value)

            raise TypeError(
                'Passing '
                '{param_or_value} with type "{arg_type}" (as "{arg_category}") to '
                'component input '
                '"{input_name}" with type "{input_type}" (as "{input_category}") is '
                'incompatible. Please fix the type of the component input.'
                .format(
                    param_or_value=param_or_value_msg,
                    arg_type=argument_type,
                    arg_category='Parameter'
                    if argument_is_parameter_type else 'Artifact',
                    input_name=input_name,
                    input_type=input_type,
                    input_category='Paramter'
                    if input_is_parameter_type else 'Artifact',
                ))

    if not component_spec.name:
        component_spec.name = _components._default_component_name

    resolved_cmd = _resolve_commands_and_args_v2(
        component_spec=component_spec, arguments=arguments)

    task.container_spec = (
        pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec(
            image=component_spec.implementation.container.image,
            command=resolved_cmd.command,
            args=resolved_cmd.args,
            env=[
                pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec
                .EnvVar(name=name, value=value)
                for name, value in task.container.env_dict.items()
            ],
        ))

    # TODO(chensun): dedupe IR component_spec and contaienr_spec
    pipeline_task_spec.component_ref.name = (
        dsl_utils.sanitize_component_name(task.name))
    executor_label = dsl_utils.sanitize_executor_label(task.name)

    task.component_spec = dsl_component_spec.build_component_spec_from_structure(
        component_spec, executor_label, arguments.keys())

    task.task_spec = pipeline_task_spec

    # Override command and arguments if compiling to v2.
    if kfp.COMPILING_FOR_V2:
        task.command = resolved_cmd.command
        task.arguments = resolved_cmd.args

        # limit this to v2 compiling only to avoid possible behavior change in v1.
        task.inputs = list(input_params_set)
