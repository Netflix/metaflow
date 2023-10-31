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
"""KFP DSL v2 compiler.

This is an experimental implementation of KFP compiler that compiles KFP
pipeline into Pipeline IR:
https://docs.google.com/document/d/1PUDuSQ8vmeKSBloli53mp7GIvzekaY7sggg6ywy35Dk/
"""

import collections
import inspect
import json
import uuid
import warnings
from typing import (Any, Callable, Dict, List, Mapping, Optional, Set, Tuple,
                    Union)

import kfp
import kfp.v2.dsl.experimental as dsl
from google.protobuf import json_format
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2.compiler import compiler_utils
from kfp.v2.compiler.experimental import pipeline_spec_builder as builder
from kfp.v2.components import utils as component_utils
from kfp.v2.components.experimental import component_factory
from kfp.v2.components.experimental import for_loop
from kfp.v2.components.experimental import pipeline_channel
from kfp.v2.components.experimental import pipeline_task
from kfp.v2.components.experimental import tasks_group
from kfp.v2.components.types import artifact_types
from kfp.v2.components.types.experimental import type_utils

_GroupOrTask = Union[tasks_group.TasksGroup, pipeline_task.PipelineTask]


class Compiler:
    """Experimental DSL compiler that targets the PipelineSpec IR.

    It compiles pipeline function into PipelineSpec json string.
    PipelineSpec is the IR protobuf message that defines a pipeline:
    https://github.com/kubeflow/pipelines/blob/237795539f7b85bac77435e2464367226ee19391/api/v2alpha1/pipeline_spec.proto#L8
    In this initial implementation, we only support components authored through
    Component yaml spec. And we don't support advanced features like conditions,
    static and dynamic loops, etc.

    Example::

        @dsl.pipeline(
          name='name',
          description='description',
        )
        def my_pipeline(a: int = 1, b: str = "default value"):
            ...

        kfp.v2.compiler.Compiler().compile(
            pipeline_func=my_pipeline,
            package_path='path/to/pipeline.json',
        )
    """

    def compile(
        self,
        pipeline_func: Callable[..., Any],
        package_path: str,
        pipeline_name: Optional[str] = None,
        pipeline_parameters: Optional[Mapping[str, Any]] = None,
        type_check: bool = True,
    ) -> None:
        """Compile the given pipeline function into pipeline job json.

        Args:
            pipeline_func: Pipeline function with @dsl.pipeline decorator.
            package_path: The output pipeline spec .json file path. For example,
                "~/pipeline_spec.json".
            pipeline_name: Optional; the name of the pipeline.
            pipeline_parameters: Optional; the mapping from parameter names to
                values.
            type_check: Optional; whether to enable the type check or not.
                Default is True.
        """
        type_check_old_value = kfp.TYPE_CHECK
        try:
            kfp.TYPE_CHECK = type_check
            pipeline_spec = self._create_pipeline_v2(
                pipeline_func=pipeline_func,
                pipeline_name=pipeline_name,
                pipeline_parameters_override=pipeline_parameters,
            )
            self._write_pipeline_spec_json(
                pipeline_spec=pipeline_spec,
                output_path=package_path,
            )
        finally:
            kfp.TYPE_CHECK = type_check_old_value

    def _create_pipeline_v2(
        self,
        pipeline_func: Callable[..., Any],
        pipeline_name: Optional[str] = None,
        pipeline_parameters_override: Optional[Mapping[str, Any]] = None,
    ) -> pipeline_spec_pb2.PipelineSpec:
        """Creates a pipeline instance and constructs the pipeline spec from
        it.

        Args:
            pipeline_func: The pipeline function with @dsl.pipeline decorator.
            pipeline_name: Optional; the name of the pipeline.
            pipeline_parameters_override: Optional; the mapping from parameter
                names to values.

        Returns:
            A PipelineSpec proto representing the compiled pipeline.
        """

        # Create the arg list with no default values and call pipeline function.
        # Assign type information to the PipelineChannel
        pipeline_meta = component_factory.extract_component_interface(
            pipeline_func)
        pipeline_name = pipeline_name or pipeline_meta.name

        pipeline_root = getattr(pipeline_func, 'pipeline_root', None)

        args_list = []
        signature = inspect.signature(pipeline_func)

        for arg_name in signature.parameters:
            arg_type = pipeline_meta.inputs[arg_name].type
            if not type_utils.is_parameter_type(arg_type):
                raise TypeError(
                    'The pipeline argument "{arg_name}" is viewed as an artifact'
                    ' due to its type "{arg_type}". And we currently do not '
                    'support passing artifacts as pipeline inputs. Consider type'
                    ' annotating the argument with a primitive type, such as '
                    '"str", "int", "float", "bool", "dict", and "list".'.format(
                        arg_name=arg_name, arg_type=arg_type))
            args_list.append(
                dsl.PipelineParameterChannel(
                    name=arg_name, channel_type=arg_type))

        with dsl.Pipeline(pipeline_name) as dsl_pipeline:
            pipeline_func(*args_list)

        if not dsl_pipeline.tasks:
            raise ValueError('Task is missing from pipeline.')

        self._validate_exit_handler(dsl_pipeline)

        pipeline_inputs = pipeline_meta.inputs or {}

        # Verify that pipeline_parameters_override contains only input names
        # that match the pipeline inputs definition.
        pipeline_parameters_override = pipeline_parameters_override or {}
        for input_name in pipeline_parameters_override:
            if input_name not in pipeline_inputs:
                raise ValueError(
                    'Pipeline parameter {} does not match any known '
                    'pipeline argument.'.format(input_name))

        # Fill in the default values.
        args_list_with_defaults = [
            dsl.PipelineParameterChannel(
                name=input_name,
                channel_type=input_spec.type,
                value=pipeline_parameters_override.get(input_name) or
                input_spec.default,
            ) for input_name, input_spec in pipeline_inputs.items()
        ]

        # Making the pipeline group name unique to prevent name clashes with
        # templates
        pipeline_group = dsl_pipeline.groups[0]
        pipeline_group.name = uuid.uuid4().hex

        pipeline_spec = self._create_pipeline_spec(
            pipeline_args=args_list_with_defaults,
            pipeline=dsl_pipeline,
        )

        if pipeline_root:
            pipeline_spec.default_pipeline_root = pipeline_root

        return pipeline_spec

    def _write_pipeline_spec_json(
        self,
        pipeline_spec: pipeline_spec_pb2.PipelineSpec,
        output_path: str,
    ) -> None:
        """Writes pipeline spec into a json file.

        Args:
            pipeline_spec: IR pipeline spec.
            ouput_path: The file path to be written.

        Raises:
            ValueError: if the specified output path doesn't end with the
                acceptable extention.
        """
        json_text = json_format.MessageToJson(pipeline_spec, sort_keys=True)

        if output_path.endswith('.json'):
            with open(output_path, 'w') as json_file:
                json_file.write(json_text)
        else:
            raise ValueError(
                'The output path {} should ends with ".json".'.format(
                    output_path))

    def _validate_exit_handler(self, pipeline: dsl.Pipeline) -> None:
        """Makes sure there is only one global exit handler.

        This is temporary to be compatible with KFP v1.

        Raises:
            ValueError if there are more than one exit handler.
        """

        def _validate_exit_handler_helper(
            group: tasks_group.TasksGroup,
            exiting_task_names: List[str],
            handler_exists: bool,
        ) -> None:

            if isinstance(group, dsl.ExitHandler):
                if handler_exists or len(exiting_task_names) > 1:
                    raise ValueError(
                        'Only one global exit_handler is allowed and all ops need to be included.'
                    )
                handler_exists = True

            if group.tasks:
                exiting_task_names.extend([x.name for x in group.tasks])

            for group in group.groups:
                _validate_exit_handler_helper(
                    group=group,
                    exiting_task_names=exiting_task_names,
                    handler_exists=handler_exists,
                )

        _validate_exit_handler_helper(
            group=pipeline.groups[0],
            exiting_task_names=[],
            handler_exists=False,
        )

    def _create_pipeline_spec(
        self,
        pipeline_args: List[dsl.PipelineChannel],
        pipeline: dsl.Pipeline,
    ) -> pipeline_spec_pb2.PipelineSpec:
        """Creates a pipeline spec object.

        Args:
            pipeline_args: The list of pipeline input parameters.
            pipeline: The instantiated pipeline object.

        Returns:
            A PipelineSpec proto representing the compiled pipeline.

        Raises:
            ValueError if the argument is of unsupported types.
        """
        compiler_utils.validate_pipeline_name(pipeline.name)

        deployment_config = pipeline_spec_pb2.PipelineDeploymentConfig()
        pipeline_spec = pipeline_spec_pb2.PipelineSpec()

        pipeline_spec.pipeline_info.name = pipeline.name
        pipeline_spec.sdk_version = 'kfp-{}'.format(kfp.__version__)
        # Schema version 2.1.0 is required for kfp-pipeline-spec>0.1.13
        pipeline_spec.schema_version = '2.1.0'

        pipeline_spec.root.CopyFrom(
            builder.build_component_spec_for_group(
                pipeline_channels=pipeline_args,
                is_root_group=True,
            ))

        root_group = pipeline.groups[0]

        all_groups = self._get_all_groups(root_group)
        group_name_to_group = {group.name: group for group in all_groups}
        task_name_to_parent_groups, group_name_to_parent_groups = (
            self._get_parent_groups(root_group))
        condition_channels = self._get_condition_channels_for_tasks(root_group)
        name_to_for_loop_group = {
            group_name: group
            for group_name, group in group_name_to_group.items()
            if isinstance(group, dsl.ParallelFor)
        }
        inputs = self._get_inputs_for_all_groups(
            pipeline=pipeline,
            pipeline_args=pipeline_args,
            root_group=root_group,
            task_name_to_parent_groups=task_name_to_parent_groups,
            group_name_to_parent_groups=group_name_to_parent_groups,
            condition_channels=condition_channels,
            name_to_for_loop_group=name_to_for_loop_group,
        )
        dependencies = self._get_dependencies(
            pipeline=pipeline,
            root_group=root_group,
            task_name_to_parent_groups=task_name_to_parent_groups,
            group_name_to_parent_groups=group_name_to_parent_groups,
            group_name_to_group=group_name_to_group,
            condition_channels=condition_channels,
        )

        for group in all_groups:
            self._build_spec_by_group(
                pipeline_spec=pipeline_spec,
                deployment_config=deployment_config,
                group=group,
                inputs=inputs,
                dependencies=dependencies,
                rootgroup_name=root_group.name,
                task_name_to_parent_groups=task_name_to_parent_groups,
                group_name_to_parent_groups=group_name_to_parent_groups,
                name_to_for_loop_group=name_to_for_loop_group,
            )

        # TODO: refactor to support multiple exit handler per pipeline.
        if pipeline.groups[0].groups:
            first_group = pipeline.groups[0].groups[0]
            if isinstance(first_group, dsl.ExitHandler):
                exit_task = first_group.exit_task
                exit_task_name = component_utils.sanitize_task_name(
                    exit_task.name)
                exit_handler_group_task_name = component_utils.sanitize_task_name(
                    first_group.name)
                input_parameters_in_current_dag = [
                    input_name for input_name in
                    pipeline_spec.root.input_definitions.parameters
                ]
                exit_task_task_spec = builder.build_task_spec_for_exit_task(
                    task=exit_task,
                    dependent_task=exit_handler_group_task_name,
                    pipeline_inputs=pipeline_spec.root.input_definitions,
                )

                exit_task_component_spec = builder.build_component_spec_for_task(
                    task=exit_task)

                exit_task_container_spec = builder.build_container_spec_for_task(
                    task=exit_task)

                # Add exit task task spec
                pipeline_spec.root.dag.tasks[exit_task_name].CopyFrom(
                    exit_task_task_spec)

                # Add exit task component spec if it does not exist.
                component_name = exit_task_task_spec.component_ref.name
                if component_name not in pipeline_spec.components:
                    pipeline_spec.components[component_name].CopyFrom(
                        exit_task_component_spec)

                # Add exit task container spec if it does not exist.
                executor_label = exit_task_component_spec.executor_label
                if executor_label not in deployment_config.executors:
                    deployment_config.executors[
                        executor_label].container.CopyFrom(
                            exit_task_container_spec)
                    pipeline_spec.deployment_spec.update(
                        json_format.MessageToDict(deployment_config))

        return pipeline_spec

    def _get_all_groups(
        self,
        root_group: tasks_group.TasksGroup,
    ) -> List[tasks_group.TasksGroup]:
        """Gets all groups (not including tasks) in a pipeline.

        Args:
            root_group: The root group of a pipeline.

        Returns:
            A list of all groups in topological order (parent first).
        """
        all_groups = []

        def _get_all_groups_helper(
            group: tasks_group.TasksGroup,
            all_groups: List[tasks_group.TasksGroup],
        ):
            all_groups.append(group)
            for group in group.groups:
                _get_all_groups_helper(group, all_groups)

        _get_all_groups_helper(root_group, all_groups)
        return all_groups

    def _get_parent_groups(
        self,
        root_group: tasks_group.TasksGroup,
    ) -> Tuple[Mapping[str, List[_GroupOrTask]], Mapping[str,
                                                         List[_GroupOrTask]]]:
        """Get parent groups that contain the specified tasks.

        Each pipeline has a root group. Each group has a list of tasks (leaf)
        and groups.
        This function traverse the tree and get ancestor groups for all tasks.

        Args:
            root_group: The root group of a pipeline.

        Returns:
            A tuple. The first item is a mapping of task names to parent groups,
            and second item is a mapping of group names to parent groups.
            A list of parent groups is a list of ancestor groups including the
            task/group itself. The list is sorted in a way that the farthest
            parent group is the first and task/group itself is the last.
        """

        def _get_parent_groups_helper(
            current_groups: List[tasks_group.TasksGroup],
            tasks_to_groups: Dict[str, List[_GroupOrTask]],
            groups_to_groups: Dict[str, List[_GroupOrTask]],
        ) -> None:
            root_group = current_groups[-1]
            for group in root_group.groups:

                groups_to_groups[group.name] = [x.name for x in current_groups
                                               ] + [group.name]
                current_groups.append(group)

                _get_parent_groups_helper(
                    current_groups=current_groups,
                    tasks_to_groups=tasks_to_groups,
                    groups_to_groups=groups_to_groups,
                )
                del current_groups[-1]

            for task in root_group.tasks:
                tasks_to_groups[task.name] = [x.name for x in current_groups
                                             ] + [task.name]

        tasks_to_groups = {}
        groups_to_groups = {}
        current_groups = [root_group]

        _get_parent_groups_helper(
            current_groups=current_groups,
            tasks_to_groups=tasks_to_groups,
            groups_to_groups=groups_to_groups,
        )
        return (tasks_to_groups, groups_to_groups)

    # TODO: do we really need this?
    def _get_condition_channels_for_tasks(
        self,
        root_group: tasks_group.TasksGroup,
    ) -> Mapping[str, Set[dsl.PipelineChannel]]:
        """Gets channels referenced in conditions of tasks' parents.

        Args:
            root_group: The root group of a pipeline.

        Returns:
            A mapping of task name to a set of pipeline channels appeared in its
            parent dsl.Condition groups.
        """
        conditions = collections.defaultdict(set)

        def _get_condition_channels_for_tasks_helper(
            group,
            current_conditions_channels,
        ):
            new_current_conditions_channels = current_conditions_channels
            if isinstance(group, dsl.Condition):
                new_current_conditions_channels = list(
                    current_conditions_channels)
                if isinstance(group.condition.left_operand,
                              dsl.PipelineChannel):
                    new_current_conditions_channels.append(
                        group.condition.left_operand)
                if isinstance(group.condition.right_operand,
                              dsl.PipelineChannel):
                    new_current_conditions_channels.append(
                        group.condition.right_operand)
            for task in group.tasks:
                for channel in new_current_conditions_channels:
                    conditions[task.name].add(channel)
            for group in group.groups:
                _get_condition_channels_for_tasks_helper(
                    group, new_current_conditions_channels)

        _get_condition_channels_for_tasks_helper(root_group, [])
        return conditions

    def _get_inputs_for_all_groups(
        self,
        pipeline: dsl.Pipeline,
        pipeline_args: List[dsl.PipelineChannel],
        root_group: tasks_group.TasksGroup,
        task_name_to_parent_groups: Mapping[str, List[_GroupOrTask]],
        group_name_to_parent_groups: Mapping[str, List[tasks_group.TasksGroup]],
        condition_channels: Mapping[str, Set[dsl.PipelineParameterChannel]],
        name_to_for_loop_group: Mapping[str, dsl.ParallelFor],
    ) -> Mapping[str, List[Tuple[dsl.PipelineChannel, str]]]:
        """Get inputs and outputs of each group and op.

        Args:
            pipeline: The instantiated pipeline object.
            pipeline_args: The list of pipeline function arguments as
                PipelineChannel.
            root_group: The root group of the pipeline.
            task_name_to_parent_groups: The dict of task name to list of parent
                groups.
            group_name_to_parent_groups: The dict of group name to list of
                parent groups.
            condition_channels: The dict of task name to a set of pipeline
                channels referenced by its parent condition groups.
            name_to_for_loop_group: The dict of for loop group name to loop
                group.

        Returns:
            A mapping  with key being the group/task names and values being list
            of tuples (channel, producing_task_name).
            producing_task_name is the name of the task that produces the
            channel. If the channel is a pipeline argument (no producer task),
            then producing_task_name is None.
        """
        inputs = collections.defaultdict(set)

        for task in pipeline.tasks.values():
            # task's inputs and all channels used in conditions for that task are
            # considered.
            task_inputs = task.channel_inputs
            task_condition_inputs = list(condition_channels[task.name])

            for channel in task.channel_inputs + task_condition_inputs:

                # If the value is already provided (immediate value), then no
                # need to expose it as input for its parent groups.
                if getattr(channel, 'value', None):
                    continue

                # channels_to_add could be a list of PipelineChannels when loop
                # args are involved. Given a nested loops example as follows:
                #
                #  def my_pipeline(loop_parameter: list):
                #       with dsl.ParallelFor(loop_parameter) as item:
                #           with dsl.ParallelFor(item.p_a) as item_p_a:
                #               print_op(item_p_a.q_a)
                #
                # The print_op takes an input of
                # {{channel:task=;name=loop_parameter-loop-item-subvar-p_a-loop-item-subvar-q_a;}}.
                # Given this, we calculate the list of PipelineChannels potentially
                # needed by across DAG levels as follows:
                #
                # [{{channel:task=;name=loop_parameter-loop-item-subvar-p_a-loop-item-subvar-q_a}},
                #  {{channel:task=;name=loop_parameter-loop-item-subvar-p_a-loop-item}},
                #  {{channel:task=;name=loop_parameter-loop-item-subvar-p_a}},
                #  {{channel:task=;name=loop_parameter-loop-item}},
                #  {{chaenel:task=;name=loop_parameter}}]
                #
                # For the above example, the first loop needs the input of
                # {{channel:task=;name=loop_parameter}},
                # the second loop needs the input of
                # {{channel:task=;name=loop_parameter-loop-item}}
                # and the print_op needs the input of
                # {{channel:task=;name=loop_parameter-loop-item-subvar-p_a-loop-item}}
                #
                # When we traverse a DAG in a top-down direction, we add channels
                # from the end, and pop it out when it's no longer needed by the
                # sub-DAG.
                # When we traverse a DAG in a bottom-up direction, we add
                # channels from the front, and pop it out when it's no longer
                #  needed by the parent DAG.
                channels_to_add = collections.deque()
                channel_to_add = channel

                while isinstance(channel_to_add, (
                        for_loop.LoopArgument,
                        for_loop.LoopArgumentVariable,
                )):
                    channels_to_add.append(channel_to_add)
                    if isinstance(channel_to_add,
                                  for_loop.LoopArgumentVariable):
                        channel_to_add = channel_to_add.loop_argument
                    elif isinstance(channel_to_add.items_or_pipeline_channel,
                                    dsl.PipelineChannel):
                        channel_to_add = channel_to_add.items_or_pipeline_channel
                    else:
                        break

                if isinstance(channel_to_add, dsl.PipelineChannel):
                    channels_to_add.append(channel_to_add)

                if channel.task_name:
                    # The PipelineChannel is produced by a task.

                    upstream_task = pipeline.tasks[channel.task_name]
                    upstream_groups, downstream_groups = (
                        self._get_uncommon_ancestors(
                            task_name_to_parent_groups=task_name_to_parent_groups,
                            group_name_to_parent_groups=group_name_to_parent_groups,
                            task1=upstream_task,
                            task2=task,
                        ))

                    for i, group_name in enumerate(downstream_groups):
                        if i == 0:
                            # If it is the first uncommon downstream group, then
                            # the input comes from the first uncommon upstream
                            # group.
                            producer_task = upstream_groups[0]
                        else:
                            # If not the first downstream group, then the input
                            # is passed down from its ancestor groups so the
                            # upstream group is None.
                            producer_task = None

                        inputs[group_name].add(
                            (channels_to_add[-1], producer_task))

                        if group_name in name_to_for_loop_group:
                            loop_group = name_to_for_loop_group[group_name]

                            # Pop out the last elements from channels_to_add if it
                            # is found in the current (loop) DAG. Downstreams
                            # would only need the more specific versions for it.
                            if channels_to_add[
                                    -1].full_name in loop_group.loop_argument.full_name:
                                channels_to_add.pop()
                                if not channels_to_add:
                                    break

                else:
                    # The PipelineChannel is not produced by a task. It's either
                    # a top-level pipeline input, or a constant value to loop
                    # items.

                    # TODO: revisit if this is correct.
                    if getattr(task, 'is_exit_handler', False):
                        continue

                    # For PipelineChannel as a result of constant value used as
                    # loop items, we have to go from bottom-up because the
                    # PipelineChannel can be originated from the middle a DAG,
                    # which is not needed and visible to its parent DAG.
                    if isinstance(
                            channel,
                        (for_loop.LoopArgument, for_loop.LoopArgumentVariable
                        )) and channel.is_with_items_loop_argument:
                        for group_name in task_name_to_parent_groups[
                                task.name][::-1]:

                            inputs[group_name].add((channels_to_add[0], None))
                            if group_name in name_to_for_loop_group:
                                # for example:
                                #   loop_group.loop_argument.name = 'loop-item-param-1'
                                #   channel.name = 'loop-item-param-1-subvar-a'
                                loop_group = name_to_for_loop_group[group_name]

                                if channels_to_add[
                                        0].full_name in loop_group.loop_argument.full_name:
                                    channels_to_add.popleft()
                                    if not channels_to_add:
                                        break
                    else:
                        # For PipelineChannel from pipeline input, go top-down
                        # just like we do for PipelineChannel produced by a task.
                        for group_name in task_name_to_parent_groups[task.name]:

                            inputs[group_name].add((channels_to_add[-1], None))
                            if group_name in name_to_for_loop_group:
                                loop_group = name_to_for_loop_group[group_name]

                                if channels_to_add[
                                        -1].full_name in loop_group.loop_argument.full_name:
                                    channels_to_add.pop()
                                    if not channels_to_add:
                                        break

        return inputs

    def _get_uncommon_ancestors(
        self,
        task_name_to_parent_groups: Mapping[str, List[_GroupOrTask]],
        group_name_to_parent_groups: Mapping[str, List[tasks_group.TasksGroup]],
        task1: _GroupOrTask,
        task2: _GroupOrTask,
    ) -> Tuple[List[_GroupOrTask], List[_GroupOrTask]]:
        """Gets the unique ancestors between two tasks.

        For example, task1's ancestor groups are [root, G1, G2, G3, task1],
        task2's ancestor groups are [root, G1, G4, task2], then it returns a
        tuple ([G2, G3, task1], [G4, task2]).

        Args:
            task_name_to_parent_groups: The dict of task name to list of parent
                groups.
            group_name_tor_parent_groups: The dict of group name to list of
                parent groups.
            task1: One of the two tasks.
            task2: The other task.

        Returns:
            A tuple which are lists of uncommon ancestors for each task.
        """
        if task1.name in task_name_to_parent_groups:
            task1_groups = task_name_to_parent_groups[task1.name]
        elif task1.name in group_name_to_parent_groups:
            task1_groups = group_name_to_parent_groups[task1.name]
        else:
            raise ValueError(task1.name + ' does not exist.')

        if task2.name in task_name_to_parent_groups:
            task2_groups = task_name_to_parent_groups[task2.name]
        elif task2.name in group_name_to_parent_groups:
            task2_groups = group_name_to_parent_groups[task2.name]
        else:
            raise ValueError(task2.name + ' does not exist.')

        both_groups = [task1_groups, task2_groups]
        common_groups_len = sum(
            1 for x in zip(*both_groups) if x == (x[0],) * len(x))
        group1 = task1_groups[common_groups_len:]
        group2 = task2_groups[common_groups_len:]
        return (group1, group2)

    # TODO: revisit for dependency that breaks through DAGs.
    def _get_dependencies(
        self,
        pipeline: dsl.Pipeline,
        root_group: tasks_group.TasksGroup,
        task_name_to_parent_groups: Mapping[str, List[_GroupOrTask]],
        group_name_to_parent_groups: Mapping[str, List[tasks_group.TasksGroup]],
        group_name_to_group: Mapping[str, tasks_group.TasksGroup],
        condition_channels: Dict[str, dsl.PipelineChannel],
    ) -> Mapping[str, List[_GroupOrTask]]:
        """Gets dependent groups and tasks for all tasks and groups.

        Args:
            pipeline: The instantiated pipeline object.
            root_group: The root group of the pipeline.
            task_name_to_parent_groups: The dict of task name to list of parent
                groups.
            group_name_to_parent_groups: The dict of group name to list of
                parent groups.
            group_name_to_group: The dict of group name to group.
            condition_channels: The dict of task name to a set of pipeline
                channels referenced by its parent condition groups.

        Returns:
            A Mapping where key is group/task name, value is a list of dependent
            groups/tasks. The dependencies are calculated in the following way:
            if task2 depends on task1, and their ancestors are
            [root, G1, G2, task1] and [root, G1, G3, G4, task2], then G3 is
            dependent on G2. Basically dependency only exists in the first
            uncommon ancesters in their ancesters chain. Only sibling
            groups/tasks can have dependencies.
        """
        dependencies = collections.defaultdict(set)
        for task in pipeline.tasks.values():
            upstream_task_names = set()
            task_condition_inputs = list(condition_channels[task.name])
            for channel in task.channel_inputs + task_condition_inputs:
                if channel.task_name:
                    upstream_task_names.add(channel.task_name)
            upstream_task_names |= set(task.dependent_tasks)

            for upstream_task_name in upstream_task_names:
                # the dependent op could be either a BaseOp or an opsgroup
                if upstream_task_name in pipeline.tasks:
                    upstream_task = pipeline.tasks[upstream_task_name]
                elif upstream_task_name in group_name_to_group:
                    upstream_task = group_name_to_group[upstream_task_name]
                else:
                    raise ValueError(
                        f'Compiler cannot find task: {upstream_task_name}.')

                upstream_groups, downstream_groups = self._get_uncommon_ancestors(
                    task_name_to_parent_groups=task_name_to_parent_groups,
                    group_name_to_parent_groups=group_name_to_parent_groups,
                    task1=upstream_task,
                    task2=task,
                )
                dependencies[downstream_groups[0]].add(upstream_groups[0])

        return dependencies

    def _build_spec_by_group(
        self,
        pipeline_spec: pipeline_spec_pb2.PipelineSpec,
        deployment_config: pipeline_spec_pb2.PipelineDeploymentConfig,
        group: tasks_group.TasksGroup,
        inputs: Mapping[str, List[Tuple[dsl.PipelineChannel, str]]],
        dependencies: Dict[str, List[_GroupOrTask]],
        rootgroup_name: str,
        task_name_to_parent_groups: Mapping[str, List[_GroupOrTask]],
        group_name_to_parent_groups: Mapping[str, List[tasks_group.TasksGroup]],
        name_to_for_loop_group: Mapping[str, dsl.ParallelFor],
    ) -> None:
        """Generates IR spec given a TasksGroup.

        Args:
            pipeline_spec: The pipeline_spec to update in place.
            deployment_config: The deployment_config to hold all executors. The
                spec is updated in place.
            group: The TasksGroup to generate spec for.
            inputs: The inputs dictionary. The keys are group/task names and the
                values are lists of tuples (channel, producing_task_name).
            dependencies: The group dependencies dictionary. The keys are group
                or task names, and the values are lists of dependent groups or
                tasks.
            rootgroup_name: The name of the group root. Used to determine whether
                the component spec for the current group should be the root dag.
            task_name_to_parent_groups: The dict of task name to parent groups.
                Key is task name. Value is a list of ancestor groups including
                the task itself. The list of a given task is sorted in a way that
                the farthest group is the first and the task itself is the last.
            group_name_to_parent_groups: The dict of group name to parent groups.
                Key is the group name. Value is a list of ancestor groups
                including the group itself. The list of a given group is sorted
                in a way that the farthest group is the first and the group
                itself is the last.
            name_to_for_loop_group: The dict of for loop group name to loop
                group.
        """
        group_component_name = component_utils.sanitize_component_name(
            group.name)

        if group.name == rootgroup_name:
            group_component_spec = pipeline_spec.root
        else:
            group_component_spec = pipeline_spec.components[
                group_component_name]

        task_name_to_task_spec = {}
        task_name_to_component_spec = {}

        # Generate task specs and component specs for the dag.
        subgroups = group.groups + group.tasks
        for subgroup in subgroups:

            subgroup_inputs = inputs.get(subgroup.name, [])
            subgroup_channels = [channel for channel, _ in subgroup_inputs]

            subgroup_component_name = (
                component_utils.sanitize_component_name(subgroup.name))

            tasks_in_current_dag = [
                component_utils.sanitize_task_name(subgroup.name)
                for subgroup in subgroups
            ]
            input_parameters_in_current_dag = [
                input_name for input_name in
                group_component_spec.input_definitions.parameters
            ]
            input_artifacts_in_current_dag = [
                input_name for input_name in
                group_component_spec.input_definitions.artifacts
            ]
            is_parent_component_root = (
                group_component_spec == pipeline_spec.root)

            if isinstance(subgroup, pipeline_task.PipelineTask):

                subgroup_task_spec = builder.build_task_spec_for_task(
                    task=subgroup,
                    parent_component_inputs=group_component_spec
                    .input_definitions,
                    tasks_in_current_dag=tasks_in_current_dag,
                    input_parameters_in_current_dag=input_parameters_in_current_dag,
                    input_artifacts_in_current_dag=input_artifacts_in_current_dag,
                )
                task_name_to_task_spec[subgroup.name] = subgroup_task_spec

                subgroup_component_spec = builder.build_component_spec_for_task(
                    task=subgroup)
                task_name_to_component_spec[
                    subgroup.name] = subgroup_component_spec

                # TODO: handler importer spec.

                subgroup_container_spec = builder.build_container_spec_for_task(
                    task=subgroup)

                if compiler_utils.is_v2_component(subgroup):
                    compiler_utils.refactor_v2_container_spec(
                        subgroup_container_spec)

                executor_label = subgroup_component_spec.executor_label

                if executor_label not in deployment_config.executors:
                    deployment_config.executors[
                        executor_label].container.CopyFrom(
                            subgroup_container_spec)

            elif isinstance(subgroup, dsl.ParallelFor):

                # "Punch the hole", adding additional inputs (other than loop
                # arguments which will be handled separately) needed by its
                # subgroups or tasks.
                loop_subgroup_channels = []

                for channel in subgroup_channels:
                    # Skip 'withItems' loop arguments if it's from an inner loop.
                    if isinstance(
                            channel,
                        (for_loop.LoopArgument, for_loop.LoopArgumentVariable
                        )) and channel.is_with_items_loop_argument:
                        withitems_loop_arg_found_in_self_or_upstream = False
                        for group_name in group_name_to_parent_groups[
                                subgroup.name][::-1]:
                            if group_name in name_to_for_loop_group:
                                loop_group = name_to_for_loop_group[group_name]
                                if channel.name in loop_group.loop_argument.name:
                                    withitems_loop_arg_found_in_self_or_upstream = True
                                    break
                        if not withitems_loop_arg_found_in_self_or_upstream:
                            continue
                    loop_subgroup_channels.append(channel)

                if subgroup.items_is_pipeline_channel:
                    # This loop_argument is based on a pipeline channel, i.e.,
                    # rather than a static list, it is either the output of
                    # another task or an input as global pipeline parameters.
                    loop_subgroup_channels.append(
                        subgroup.loop_argument.items_or_pipeline_channel)

                loop_subgroup_channels.append(subgroup.loop_argument)

                subgroup_component_spec = builder.build_component_spec_for_group(
                    pipeline_channels=loop_subgroup_channels,
                    is_root_group=False,
                )

                subgroup_task_spec = builder.build_task_spec_for_group(
                    group=subgroup,
                    pipeline_channels=loop_subgroup_channels,
                    tasks_in_current_dag=tasks_in_current_dag,
                    is_parent_component_root=is_parent_component_root,
                )

            elif isinstance(subgroup, dsl.Condition):

                # "Punch the hole", adding inputs needed by its subgroups or
                # tasks.
                condition_subgroup_channels = list(subgroup_channels)
                for operand in [
                        subgroup.condition.left_operand,
                        subgroup.condition.right_operand,
                ]:
                    if isinstance(operand, dsl.PipelineChannel):
                        condition_subgroup_channels.append(operand)

                subgroup_component_spec = builder.build_component_spec_for_group(
                    pipeline_channels=condition_subgroup_channels,
                    is_root_group=False,
                )

                subgroup_task_spec = builder.build_task_spec_for_group(
                    group=subgroup,
                    pipeline_channels=condition_subgroup_channels,
                    tasks_in_current_dag=tasks_in_current_dag,
                    is_parent_component_root=is_parent_component_root,
                )

            elif isinstance(subgroup, dsl.ExitHandler):

                subgroup_component_spec = builder.build_component_spec_for_group(
                    pipeline_channels=subgroup_channels,
                    is_root_group=False,
                )

                subgroup_task_spec = builder.build_task_spec_for_group(
                    group=subgroup,
                    pipeline_channels=subgroup_channels,
                    tasks_in_current_dag=tasks_in_current_dag,
                    is_parent_component_root=is_parent_component_root,
                )

            else:
                raise RuntimeError(
                    f'Unexpected task/group type: Got {subgroup} of type '
                    f'{type(subgroup)}.')

            # Generate dependencies section for this task.
            if dependencies.get(subgroup.name, None):
                group_dependencies = list(dependencies[subgroup.name])
                group_dependencies.sort()
                subgroup_task_spec.dependent_tasks.extend([
                    component_utils.sanitize_task_name(dep)
                    for dep in group_dependencies
                ])

            # Add component spec if not exists
            if subgroup_component_name not in pipeline_spec.components:
                pipeline_spec.components[subgroup_component_name].CopyFrom(
                    subgroup_component_spec)

            # Add task spec
            group_component_spec.dag.tasks[subgroup.name].CopyFrom(
                subgroup_task_spec)

        pipeline_spec.deployment_spec.update(
            json_format.MessageToDict(deployment_config))

        # Surface metrics outputs to the top.
        builder.populate_metrics_in_dag_outputs(
            tasks=group.tasks,
            task_name_to_parent_groups=task_name_to_parent_groups,
            task_name_to_task_spec=task_name_to_task_spec,
            task_name_to_component_spec=task_name_to_component_spec,
            pipeline_spec=pipeline_spec,
        )
