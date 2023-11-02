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
"""Functions for creating PipelineSpec proto objects."""

import json
from typing import List, Mapping, Optional, Tuple, Union

from google.protobuf import struct_pb2
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2.components import utils as component_utils
from kfp.v2.components.experimental import for_loop
from kfp.v2.components.experimental import pipeline_channel
from kfp.v2.components.experimental import pipeline_task
from kfp.v2.components.experimental import placeholders
from kfp.v2.components.experimental import structures
from kfp.v2.components.experimental import tasks_group
from kfp.v2.components.types import artifact_types
from kfp.v2.components.types.experimental import type_utils

_GroupOrTask = Union[tasks_group.TasksGroup, pipeline_task.PipelineTask]


def _additional_input_name_for_pipeline_channel(
        channel_or_name: Union[pipeline_channel.PipelineChannel, str]) -> str:
    """Gets the name for an additional (compiler-injected) input."""

    # Adding a prefix to avoid (reduce chance of) name collision between the
    # original component inputs and the injected input.
    return 'pipelinechannel--' + (
        channel_or_name.full_name if isinstance(
            channel_or_name, pipeline_channel.PipelineChannel) else
        channel_or_name)


def _to_protobuf_value(value: type_utils.PARAMETER_TYPES) -> struct_pb2.Value:
    """Creates a google.protobuf.struct_pb2.Value message out of a provide
    value.

    Args:
        value: The value to be converted to Value message.

    Returns:
         A google.protobuf.struct_pb2.Value message.

    Raises:
        ValueError if the given value is not one of the parameter types.
    """
    if isinstance(value, str):
        return struct_pb2.Value(string_value=value)
    elif isinstance(value, (int, float)):
        return struct_pb2.Value(number_value=value)
    elif isinstance(value, bool):
        return struct_pb2.Value(bool_value=value)
    elif isinstance(value, dict):
        return struct_pb2.Value(
            struct_value=struct_pb2.Struct(
                fields={k: _to_protobuf_value(v) for k, v in value.items()}))
    elif isinstance(value, list):
        return struct_pb2.Value(
            list_value=struct_pb2.ListValue(
                values=[_to_protobuf_value(v) for v in value]))
    else:
        raise ValueError('Value must be one of the following types: '
                         'str, int, float, bool, dict, and list. Got: '
                         f'"{value}" of type "{type(value)}".')


def build_task_spec_for_task(
    task: pipeline_task.PipelineTask,
    parent_component_inputs: pipeline_spec_pb2.ComponentInputsSpec,
    tasks_in_current_dag: List[str],
    input_parameters_in_current_dag: List[str],
    input_artifacts_in_current_dag: List[str],
) -> pipeline_spec_pb2.PipelineTaskSpec:
    """Builds PipelineTaskSpec for a pipeline task.

    A task input may reference an output outside its immediate DAG.
    For instance::

        random_num = random_num_op(...)
        with dsl.Condition(random_num.output > 5):
            print_op('%s > 5' % random_num.output)

    In this example, `dsl.Condition` forms a subDAG with one task from `print_op`
    inside the subDAG. The task of `print_op` references output from `random_num`
    task, which is outside the sub-DAG. When compiling to IR, such cross DAG
    reference is disallowed. So we need to "punch a hole" in the sub-DAG to make
    the input available in the subDAG component inputs if it's not already there,
    Next, we can call this method to fix the tasks inside the subDAG to make them
    reference the component inputs instead of directly referencing the original
    producer task.

    Args:
        task: The task to build a PipelineTaskSpec for.
        parent_component_inputs: The task's parent component's input specs.
        tasks_in_current_dag: The list of tasks names for tasks in the same dag.
        input_parameters_in_current_dag: The list of input parameters in the DAG
            component.
        input_artifacts_in_current_dag: The list of input artifacts in the DAG
            component.

    Returns:
        A PipelineTaskSpec object representing the task.
    """
    pipeline_task_spec = pipeline_spec_pb2.PipelineTaskSpec()
    pipeline_task_spec.task_info.name = (
        task.task_spec.display_name or task.name)
    # Use task.name for component_ref.name because we may customize component
    # spec for individual tasks to work around the lack of optional inputs
    # support in IR.
    pipeline_task_spec.component_ref.name = (
        component_utils.sanitize_component_name(task.name))
    pipeline_task_spec.caching_options.enable_cache = (
        task.task_spec.enable_caching)

    for input_name, input_value in task.inputs.items():
        input_type = task.component_spec.inputs[input_name].type

        if isinstance(input_value, pipeline_channel.PipelineArtifactChannel):

            if input_value.task_name:
                # Value is produced by an upstream task.
                if input_value.task_name in tasks_in_current_dag:
                    # Dependent task within the same DAG.
                    pipeline_task_spec.inputs.artifacts[
                        input_name].task_output_artifact.producer_task = (
                            component_utils.sanitize_task_name(
                                input_value.task_name))
                    pipeline_task_spec.inputs.artifacts[
                        input_name].task_output_artifact.output_artifact_key = (
                            input_value.name)
                else:
                    # Dependent task not from the same DAG.
                    component_input_artifact = (
                        _additional_input_name_for_pipeline_channel(input_value)
                    )
                    assert component_input_artifact in parent_component_inputs.artifacts, \
                        'component_input_artifact: {} not found. All inputs: {}'.format(
                            component_input_artifact, parent_component_inputs)
                    pipeline_task_spec.inputs.artifacts[
                        input_name].component_input_artifact = (
                            component_input_artifact)
            else:
                raise RuntimeError(
                    f'Artifacts must be produced by a task. Got {input_value}.')

        elif isinstance(input_value, pipeline_channel.PipelineParameterChannel):

            if input_value.task_name:
                # Value is produced by an upstream task.
                if input_value.task_name in tasks_in_current_dag:
                    # Dependent task within the same DAG.
                    pipeline_task_spec.inputs.parameters[
                        input_name].task_output_parameter.producer_task = (
                            component_utils.sanitize_task_name(
                                input_value.task_name))
                    pipeline_task_spec.inputs.parameters[
                        input_name].task_output_parameter.output_parameter_key = (
                            input_value.name)
                else:
                    # Dependent task not from the same DAG.
                    component_input_parameter = (
                        _additional_input_name_for_pipeline_channel(input_value)
                    )
                    assert component_input_parameter in parent_component_inputs.parameters, \
                        'component_input_parameter: {} not found. All inputs: {}'.format(
                            component_input_parameter, parent_component_inputs)
                    pipeline_task_spec.inputs.parameters[
                        input_name].component_input_parameter = (
                            component_input_parameter)
            else:
                # Value is from pipeline input.
                component_input_parameter = input_value.full_name
                if component_input_parameter not in parent_component_inputs.parameters:
                    component_input_parameter = (
                        _additional_input_name_for_pipeline_channel(input_value)
                    )
                pipeline_task_spec.inputs.parameters[
                    input_name].component_input_parameter = (
                        component_input_parameter)

        elif isinstance(input_value, for_loop.LoopArgument):

            component_input_parameter = (
                _additional_input_name_for_pipeline_channel(input_value))
            assert component_input_parameter in parent_component_inputs.parameters, \
                'component_input_parameter: {} not found. All inputs: {}'.format(
                    component_input_parameter, parent_component_inputs)
            pipeline_task_spec.inputs.parameters[
                input_name].component_input_parameter = (
                    component_input_parameter)

        elif isinstance(input_value, for_loop.LoopArgumentVariable):

            component_input_parameter = (
                _additional_input_name_for_pipeline_channel(
                    input_value.loop_argument))
            assert component_input_parameter in parent_component_inputs.parameters, \
                'component_input_parameter: {} not found. All inputs: {}'.format(
                    component_input_parameter, parent_component_inputs)
            pipeline_task_spec.inputs.parameters[
                input_name].component_input_parameter = (
                    component_input_parameter)
            pipeline_task_spec.inputs.parameters[
                input_name].parameter_expression_selector = (
                    'parseJson(string_value)["{}"]'.format(
                        input_value.subvar_name))

        elif isinstance(input_value, str):

            # Handle extra input due to string concat
            pipeline_channels = (
                pipeline_channel.extract_pipeline_channels_from_any(input_value)
            )
            for channel in pipeline_channels:
                # value contains PipelineChannel placeholders which needs to be
                # replaced. And the input needs to be added to the task spec.

                # Form the name for the compiler injected input, and make sure it
                # doesn't collide with any existing input names.
                additional_input_name = (
                    _additional_input_name_for_pipeline_channel(channel))

                # We don't expect collision to happen because we prefix the name
                # of additional input with 'pipelinechannel--'. But just in case
                # collision did happend, throw a RuntimeError so that we don't
                # get surprise at runtime.
                for existing_input_name, _ in task.inputs.items():
                    if existing_input_name == additional_input_name:
                        raise RuntimeError(
                            'Name collision between existing input name '
                            '{} and compiler injected input name {}'.format(
                                existing_input_name, additional_input_name))

                additional_input_placeholder = (
                    placeholders.input_parameter_placeholder(
                        additional_input_name))
                input_value = input_value.replace(channel.pattern,
                                                  additional_input_placeholder)

                if channel.task_name:
                    # Value is produced by an upstream task.
                    if channel.task_name in tasks_in_current_dag:
                        # Dependent task within the same DAG.
                        pipeline_task_spec.inputs.parameters[
                            additional_input_name].task_output_parameter.producer_task = (
                                component_utils.sanitize_task_name(
                                    channel.task_name))
                        pipeline_task_spec.inputs.parameters[
                            input_name].task_output_parameter.output_parameter_key = (
                                channel.name)
                    else:
                        # Dependent task not from the same DAG.
                        component_input_parameter = (
                            _additional_input_name_for_pipeline_channel(channel)
                        )
                        assert component_input_parameter in parent_component_inputs.parameters, \
                            'component_input_parameter: {} not found. All inputs: {}'.format(
                                component_input_parameter, parent_component_inputs)
                        pipeline_task_spec.inputs.parameters[
                            additional_input_name].component_input_parameter = (
                                component_input_parameter)
                else:
                    # Value is from pipeline input. (or loop?)
                    component_input_parameter = channel.full_name
                    if component_input_parameter not in parent_component_inputs.parameters:
                        component_input_parameter = (
                            _additional_input_name_for_pipeline_channel(channel)
                        )
                    pipeline_task_spec.inputs.parameters[
                        additional_input_name].component_input_parameter = (
                            component_input_parameter)

            pipeline_task_spec.inputs.parameters[
                input_name].runtime_value.constant.string_value = input_value

        elif isinstance(input_value, (str, int, float, bool, dict, list)):

            pipeline_task_spec.inputs.parameters[
                input_name].runtime_value.constant.CopyFrom(
                    _to_protobuf_value(input_value))

        else:
            raise ValueError(
                'Input argument supports only the following types: '
                'str, int, float, bool, dict, and list.'
                f'Got {input_value} of type {type(input_value)}.')

    return pipeline_task_spec


def build_component_spec_for_task(
        task: pipeline_task.PipelineTask) -> pipeline_spec_pb2.ComponentSpec:
    """Builds ComponentSpec for a pipeline task.

    Args:
        task: The task to build a ComponentSpec for.

    Returns:
        A ComponentSpec object for the task.
    """
    component_spec = pipeline_spec_pb2.ComponentSpec()
    component_spec.executor_label = component_utils.sanitize_executor_label(
        task.name)

    for input_name, input_spec in (task.component_spec.inputs or {}).items():

        # skip inputs not present, as a workaround to support optional inputs.
        if input_name not in task.inputs:
            continue

        if type_utils.is_parameter_type(input_spec.type):
            component_spec.input_definitions.parameters[
                input_name].parameter_type = type_utils.get_parameter_type(
                    input_spec.type)
        else:
            component_spec.input_definitions.artifacts[
                input_name].artifact_type.CopyFrom(
                    type_utils.get_artifact_type_schema(input_spec.type))

    for output_name, output_spec in (task.component_spec.outputs or {}).items():
        if type_utils.is_parameter_type(output_spec.type):
            component_spec.output_definitions.parameters[
                output_name].parameter_type = type_utils.get_parameter_type(
                    output_spec.type)
        else:
            component_spec.output_definitions.artifacts[
                output_name].artifact_type.CopyFrom(
                    type_utils.get_artifact_type_schema(output_spec.type))

    return component_spec


def build_container_spec_for_task(
    task: pipeline_task.PipelineTask
) -> pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec:
    """Builds PipelineContainerSpec for a pipeline task.

    Args:
        task: The task to build a ComponentSpec for.

    Returns:
        A PipelineContaienrSpec object for the task.
    """
    container_spec = (
        pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec(
            image=task.container_spec.image,
            command=task.container_spec.commands,
            args=task.container_spec.arguments,
        ))

    if task.container_spec.env is not None:
        container_spec.env = [
            pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec
            .EnvVar(name=name, value=value)
            for name, value in task.container_spec.env.items()
        ]

    if task.container_spec.resources is not None:
        container_spec.reources.cpu_limit = (
            task.container_spec.resources.cpu_limit)
        container_spec.reources.memory_limit = (
            task.container_spec.resources.memory_limit)
        if task.container_spec.resources.accelerator_count is not None:
            container_spec.resources.accelerator.CopyFrom(
                pipeline_spec_pb2.PipelineDeploymentConfig.PipelineContainerSpec
                .ResourceSpec.AcceleratorConfig(
                    type=task.container_spec.resources.accelerator_type,
                    count=task.container_spec.resources.accelerator_count,
                ))

    return container_spec


def _fill_in_component_input_default_value(
    component_spec: pipeline_spec_pb2.ComponentSpec,
    input_name: str,
    default_value: Optional[type_utils.PARAMETER_TYPES],
) -> None:
    """Fills in the default of component input parameter.

    Args:
        component_spec: The ComponentSpec to update in place.
        input_name: The name of the input parameter.
        default_value: The default value of the input parameter.
    """
    if default_value is None:
        return

    parameter_type = component_spec.input_definitions.parameters[
        input_name].parameter_type
    if pipeline_spec_pb2.ParameterType.NUMBER_INTEGER == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.number_value = default_value
    elif pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.number_value = default_value
    elif pipeline_spec_pb2.ParameterType.STRING == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.string_value = default_value
    elif pipeline_spec_pb2.ParameterType.BOOLEAN == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.bool_value = default_value
    elif pipeline_spec_pb2.ParameterType.STRUCT == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.CopyFrom(
                _to_protobuf_value(default_value))
    elif pipeline_spec_pb2.ParameterType.LIST == parameter_type:
        component_spec.input_definitions.parameters[
            input_name].default_value.CopyFrom(
                _to_protobuf_value(default_value))


def build_component_spec_for_group(
    pipeline_channels: List[pipeline_channel.PipelineChannel],
    is_root_group: bool,
) -> pipeline_spec_pb2.ComponentSpec:
    """Builds ComponentSpec for a TasksGroup.

    Args:
        group: The group to build a ComponentSpec for.
        pipeline_channels: The list of pipeline channels referenced by the group.

    Returns:
        A PipelineTaskSpec object representing the loop group.
    """
    component_spec = pipeline_spec_pb2.ComponentSpec()

    for channel in pipeline_channels:

        input_name = (
            channel.name if is_root_group else
            _additional_input_name_for_pipeline_channel(channel))

        if isinstance(channel, pipeline_channel.PipelineArtifactChannel):
            component_spec.input_definitions.artifacts[
                input_name].artifact_type.CopyFrom(
                    type_utils.get_artifact_type_schema(channel.channel_type))
        else:
            # channel is one of PipelineParameterChannel, LoopArgument, or
            # LoopArgumentVariable.
            component_spec.input_definitions.parameters[
                input_name].parameter_type = type_utils.get_parameter_type(
                    channel.channel_type)

            # TODO: should we fill in default value for all groups and tasks?
            if is_root_group:
                _fill_in_component_input_default_value(
                    component_spec=component_spec,
                    input_name=input_name,
                    default_value=channel.value,
                )

    return component_spec


def _pop_input_from_task_spec(
    task_spec: pipeline_spec_pb2.PipelineTaskSpec,
    input_name: str,
) -> None:
    """Removes an input from task spec inputs.

    Args:
      task_spec: The pipeline task spec to update in place.
      input_name: The name of the input, which could be an artifact or paremeter.
    """
    task_spec.inputs.artifacts.pop(input_name)
    task_spec.inputs.parameters.pop(input_name)

    if task_spec.inputs == pipeline_spec_pb2.TaskInputsSpec():
        task_spec.ClearField('inputs')


def _update_task_spec_for_loop_group(
    group: tasks_group.ParallelFor,
    pipeline_task_spec: pipeline_spec_pb2.PipelineTaskSpec,
) -> None:
    """Updates PipelineTaskSpec for loop group.

    Args:
        group: The loop group to update task spec for.
        pipeline_task_spec: The pipeline task spec to update in place.
    """
    if group.items_is_pipeline_channel:
        loop_items_channel = group.loop_argument.items_or_pipeline_channel
        input_parameter_name = _additional_input_name_for_pipeline_channel(
            loop_items_channel)
        loop_argument_item_name = _additional_input_name_for_pipeline_channel(
            group.loop_argument.full_name)

        loop_arguments_item = '{}-{}'.format(
            input_parameter_name, for_loop.LoopArgument.LOOP_ITEM_NAME_BASE)
        assert loop_arguments_item == loop_argument_item_name

        pipeline_task_spec.parameter_iterator.items.input_parameter = (
            input_parameter_name)
        pipeline_task_spec.parameter_iterator.item_input = (
            loop_argument_item_name)

        # If the loop items itself is a loop arguments variable, handle the
        # subvar name.
        if isinstance(loop_items_channel, for_loop.LoopArgumentVariable):
            pipeline_task_spec.inputs.parameters[
                input_parameter_name].parameter_expression_selector = (
                    'parseJson(string_value)["{}"]'.format(
                        loop_items_channel.subvar_name))
            pipeline_task_spec.inputs.parameters[
                input_parameter_name].component_input_parameter = (
                    _additional_input_name_for_pipeline_channel(
                        loop_items_channel.loop_argument))

        remove_input_name = loop_argument_item_name
    else:
        input_parameter_name = _additional_input_name_for_pipeline_channel(
            group.loop_argument)
        raw_values = group.loop_argument.items_or_pipeline_channel

        pipeline_task_spec.parameter_iterator.items.raw = json.dumps(
            raw_values, sort_keys=True)
        pipeline_task_spec.parameter_iterator.item_input = (
            input_parameter_name)

    _pop_input_from_task_spec(
        task_spec=pipeline_task_spec,
        input_name=pipeline_task_spec.parameter_iterator.item_input)


def _resolve_condition_operands(
    left_operand: Union[str, pipeline_channel.PipelineChannel],
    right_operand: Union[str, pipeline_channel.PipelineChannel],
) -> Tuple[str, str]:
    """Resolves values and PipelineChannels for condition operands.

    Args:
        left_operand: The left operand of a condition expression.
        right_operand: The right operand of a condition expression.

    Returns:
        A tuple of the resolved operands values:
        (left_operand_value, right_operand_value).
    """

    # Pre-scan the operand to get the type of constant value if there's any.
    # The value_type can be used to backfill missing PipelineChannel.channel_type.
    value_type = None
    for value_or_reference in [left_operand, right_operand]:
        if isinstance(value_or_reference, pipeline_channel.PipelineChannel):
            parameter_type = type_utils.get_parameter_type(
                value_or_reference.channel_type)
            if parameter_type in [
                    pipeline_spec_pb2.ParameterType.STRUCT,
                    pipeline_spec_pb2.ParameterType.LIST,
                    pipeline_spec_pb2.ParameterType
                    .PARAMETER_TYPE_ENUM_UNSPECIFIED,
            ]:
                input_name = _additional_input_name_for_pipeline_channel(
                    value_or_reference)
                raise ValueError('Conditional requires scalar parameter values'
                                 ' for comparison. Found input "{}" of type {}'
                                 ' in pipeline definition instead.'.format(
                                     input_name,
                                     value_or_reference.channel_type))
    parameter_types = set()
    for value_or_reference in [left_operand, right_operand]:
        if isinstance(value_or_reference, pipeline_channel.PipelineChannel):
            parameter_type = type_utils.get_parameter_type(
                value_or_reference.channel_type)
        else:
            parameter_type = type_utils.get_parameter_type(
                type(value_or_reference).__name__)

        parameter_types.add(parameter_type)

    if len(parameter_types) == 2:
        # Two different types being compared. The only possible types are
        # String, Boolean, Double and Integer. We'll promote the other type
        # using the following precedence:
        # String > Boolean > Double > Integer
        if pipeline_spec_pb2.ParameterType.STRING in parameter_types:
            canonical_parameter_type = pipeline_spec_pb2.ParameterType.STRING
        elif pipeline_spec_pb2.ParameterType.BOOLEAN in parameter_types:
            canonical_parameter_type = pipeline_spec_pb2.ParameterType.BOOLEAN
        else:
            # Must be a double and int, promote to double.
            assert pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE in parameter_types, \
                'Types: {} [{} {}]'.format(
                parameter_types, left_operand, right_operand)
            assert pipeline_spec_pb2.ParameterType.NUMBER_INTEGER in parameter_types, \
                'Types: {} [{} {}]'.format(
                parameter_types, left_operand, right_operand)
            canonical_parameter_type = pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE
    elif len(parameter_types) == 1:  # Both operands are the same type.
        canonical_parameter_type = parameter_types.pop()
    else:
        # Probably shouldn't happen.
        raise ValueError('Unable to determine operand types for'
                         ' "{}" and "{}"'.format(left_operand, right_operand))

    operand_values = []
    for value_or_reference in [left_operand, right_operand]:
        if isinstance(value_or_reference, pipeline_channel.PipelineChannel):
            input_name = _additional_input_name_for_pipeline_channel(
                value_or_reference)
            operand_value = "inputs.parameter_values['{input_name}']".format(
                input_name=input_name)
            parameter_type = type_utils.get_parameter_type(
                value_or_reference.channel_type)
            if parameter_type == pipeline_spec_pb2.ParameterType.NUMBER_INTEGER:
                operand_value = 'int({})'.format(operand_value)
        elif isinstance(value_or_reference, str):
            operand_value = "'{}'".format(value_or_reference)
            parameter_type = pipeline_spec_pb2.ParameterType.STRING
        elif isinstance(value_or_reference, bool):
            # Booleans need to be compared as 'true' or 'false' in CEL.
            operand_value = str(value_or_reference).lower()
            parameter_type = pipeline_spec_pb2.ParameterType.BOOLEAN
        elif isinstance(value_or_reference, int):
            operand_value = str(value_or_reference)
            parameter_type = pipeline_spec_pb2.ParameterType.NUMBER_INTEGER
        else:
            assert isinstance(value_or_reference, float), value_or_reference
            operand_value = str(value_or_reference)
            parameter_type = pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE

        if parameter_type != canonical_parameter_type:
            # Type-cast to so CEL does not complain.
            if canonical_parameter_type == pipeline_spec_pb2.ParameterType.STRING:
                assert parameter_type in [
                    pipeline_spec_pb2.ParameterType.BOOLEAN,
                    pipeline_spec_pb2.ParameterType.NUMBER_INTEGER,
                    pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE,
                ]
                operand_value = "'{}'".format(operand_value)
            elif canonical_parameter_type == pipeline_spec_pb2.ParameterType.BOOLEAN:
                assert parameter_type in [
                    pipeline_spec_pb2.ParameterType.NUMBER_INTEGER,
                    pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE,
                ]
                operand_value = 'true' if int(operand_value) == 0 else 'false'
            else:
                assert canonical_parameter_type == pipeline_spec_pb2.ParameterType.NUMBER_DOUBLE
                assert parameter_type == pipeline_spec_pb2.ParameterType.NUMBER_INTEGER
                operand_value = 'double({})'.format(operand_value)

        operand_values.append(operand_value)

    return tuple(operand_values)


def _update_task_spec_for_condition_group(
    group: tasks_group.Condition,
    pipeline_task_spec: pipeline_spec_pb2.PipelineTaskSpec,
) -> None:
    """Updates PipelineTaskSpec for condition group.

    Args:
        group: The condition group to update task spec for.
        pipeline_task_spec: The pipeline task spec to update in place.
    """
    left_operand_value, right_operand_value = _resolve_condition_operands(
        group.condition.left_operand, group.condition.right_operand)

    condition_string = (
        f'{left_operand_value} {group.condition.operator} {right_operand_value}'
    )
    pipeline_task_spec.trigger_policy.CopyFrom(
        pipeline_spec_pb2.PipelineTaskSpec.TriggerPolicy(
            condition=condition_string))


def build_task_spec_for_exit_task(
    task: pipeline_task.PipelineTask,
    dependent_task: str,
    pipeline_inputs: pipeline_spec_pb2.ComponentInputsSpec,
) -> pipeline_spec_pb2.PipelineTaskSpec:
    """Builds PipelineTaskSpec for an exit handler's exit task.

    Args:
        tasks: The exit handler's exit task to build task spec for.
        dependent_task: The dependent task name for the exit task, i.e. the name
            of the exit handler group.
        pipeline_inputs: The pipeline level input definitions.

    Returns:
        A PipelineTaskSpec object representing the exit task.
    """
    pipeline_task_spec = build_task_spec_for_task(
        task=task,
        parent_component_inputs=pipeline_inputs,
        tasks_in_current_dag=[],  # Does not matter for exit task
        input_parameters_in_current_dag=pipeline_inputs.parameters.keys(),
        input_artifacts_in_current_dag=[],
    )
    pipeline_task_spec.dependent_tasks.extend([dependent_task])
    pipeline_task_spec.trigger_policy.strategy = (
        pipeline_spec_pb2.PipelineTaskSpec.TriggerPolicy.TriggerStrategy
        .ALL_UPSTREAM_TASKS_COMPLETED)

    return pipeline_task_spec


def build_task_spec_for_group(
    group: tasks_group.TasksGroup,
    pipeline_channels: List[pipeline_channel.PipelineChannel],
    tasks_in_current_dag: List[str],
    is_parent_component_root: bool,
) -> pipeline_spec_pb2.PipelineTaskSpec:
    """Builds PipelineTaskSpec for a group.

    Args:
        group: The group to build PipelineTaskSpec for.
        pipeline_channels: The list of pipeline channels referenced by the group.
        tasks_in_current_dag: The list of tasks names for tasks in the same dag.
        is_parent_component_root: Whether the parent component is the pipeline's
            root dag.

    Returns:
        A PipelineTaskSpec object representing the group.
    """
    pipeline_task_spec = pipeline_spec_pb2.PipelineTaskSpec()
    pipeline_task_spec.task_info.name = group.name
    pipeline_task_spec.component_ref.name = (
        component_utils.sanitize_component_name(group.name))

    for channel in pipeline_channels:

        channel_full_name = channel.full_name
        subvar_name = None
        if isinstance(channel, for_loop.LoopArgumentVariable):
            channel_full_name = channel.loop_argument.full_name
            subvar_name = channel.subvar_name

        input_name = _additional_input_name_for_pipeline_channel(channel)

        channel_name = channel.name
        if subvar_name:
            pipeline_task_spec.inputs.parameters[
                input_name].parameter_expression_selector = (
                    'parseJson(string_value)["{}"]'.format(subvar_name))
            if not channel.is_with_items_loop_argument:
                channel_name = channel.items_or_pipeline_channel.name

        if isinstance(channel, pipeline_channel.PipelineArtifactChannel):
            if channel.task_name and channel.task_name in tasks_in_current_dag:
                pipeline_task_spec.inputs.artifacts[
                    input_name].task_output_artifact.producer_task = (
                        component_utils.sanitize_task_name(channel.task_name))
                pipeline_task_spec.inputs.artifacts[
                    input_name].task_output_artifact.output_artifact_key = (
                        channel_name)
            else:
                pipeline_task_spec.inputs.artifacts[
                    input_name].component_input_artifact = (
                        channel_full_name
                        if is_parent_component_root else input_name)
        else:
            # channel is one of PipelineParameterChannel, LoopArgument, or
            # LoopArgumentVariable
            if channel.task_name and channel.task_name in tasks_in_current_dag:
                pipeline_task_spec.inputs.parameters[
                    input_name].task_output_parameter.producer_task = (
                        component_utils.sanitize_task_name(channel.task_name))
                pipeline_task_spec.inputs.parameters[
                    input_name].task_output_parameter.output_parameter_key = (
                        channel_name)
            else:
                pipeline_task_spec.inputs.parameters[
                    input_name].component_input_parameter = (
                        channel_full_name if is_parent_component_root else
                        _additional_input_name_for_pipeline_channel(
                            channel_full_name))

    if isinstance(group, tasks_group.ParallelFor):
        _update_task_spec_for_loop_group(
            group=group,
            pipeline_task_spec=pipeline_task_spec,
        )
    elif isinstance(group, tasks_group.Condition):
        _update_task_spec_for_condition_group(
            group=group,
            pipeline_task_spec=pipeline_task_spec,
        )

    return pipeline_task_spec


def populate_metrics_in_dag_outputs(
    tasks: List[pipeline_task.PipelineTask],
    task_name_to_parent_groups: Mapping[str, List[_GroupOrTask]],
    task_name_to_task_spec: Mapping[str, pipeline_spec_pb2.PipelineTaskSpec],
    task_name_to_component_spec: Mapping[str, pipeline_spec_pb2.ComponentSpec],
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
) -> None:
    """Populates metrics artifacts in DAG outputs.

    Args:
        tasks: The list of tasks that may produce metrics outputs.
        task_name_to_parent_groups: The dict of task name to parent groups.
            Key is the task's name. Value is a list of ancestor groups including
            the task itself. The list of a given op is sorted in a way that the
            farthest group is the first and the task itself is the last.
        task_name_to_task_spec: The dict of task name to PipelineTaskSpec.
        task_name_to_component_spec: The dict of task name to ComponentSpec.
        pipeline_spec: The pipeline_spec to update in-place.
    """
    for task in tasks:
        task_spec = task_name_to_task_spec[task.name]
        component_spec = task_name_to_component_spec[task.name]

        # Get the tuple of (component_name, task_name) of all its parent groups.
        parent_components_and_tasks = [('_root', '')]
        # skip the op itself and the root group which cannot be retrived via name.
        for group_name in task_name_to_parent_groups[task.name][1:-1]:
            parent_components_and_tasks.append(
                (component_utils.sanitize_component_name(group_name),
                 component_utils.sanitize_task_name(group_name)))
        # Reverse the order to make the farthest group in the end.
        parent_components_and_tasks.reverse()

        for output_name, artifact_spec in \
            component_spec.output_definitions.artifacts.items():

            if artifact_spec.artifact_type.WhichOneof(
                    'kind'
            ) == 'schema_title' and artifact_spec.artifact_type.schema_title in [
                    artifact_types.Metrics.TYPE_NAME,
                    artifact_types.ClassificationMetrics.TYPE_NAME,
            ]:
                unique_output_name = '{}-{}'.format(op.name, output_name)

                sub_task_name = task.name
                sub_task_output = output_name
                for component_name, task_name in parent_components_and_tasks:
                    group_component_spec = (
                        pipeline_spec.root if component_name == '_root' else
                        pipeline_spec.components[component_name])
                    group_component_spec.output_definitions.artifacts[
                        unique_output_name].CopyFrom(artifact_spec)
                    group_component_spec.dag.outputs.artifacts[
                        unique_output_name].artifact_selectors.append(
                            pipeline_spec_pb2.DagOutputsSpec
                            .ArtifactSelectorSpec(
                                producer_subtask=sub_task_name,
                                output_artifact_key=sub_task_output,
                            ))
                    sub_task_name = task_name
                    sub_task_output = unique_output_name
