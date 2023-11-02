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
import datetime
import json
from collections import defaultdict, OrderedDict
from deprecated import deprecated
import inspect
import re
import tarfile
import uuid
import warnings
import zipfile
from typing import Callable, Set, List, Text, Dict, Tuple, Any, Union, Optional

import kfp
from kfp.dsl import _for_loop
from kfp.compiler import _data_passing_rewriter, v2_compat

from kfp import dsl
from kfp.compiler._k8s_helper import convert_k8s_obj_to_json, sanitize_k8s_name
from kfp.compiler._op_to_template import _op_to_template, _process_obj
from kfp.compiler._default_transformers import add_pod_env, add_pod_labels

from kfp.components.structures import InputSpec
from kfp.components._yaml_utils import dump_yaml
from kfp.dsl._metadata import _extract_pipeline_metadata
from kfp.dsl._ops_group import OpsGroup
from kfp.dsl._pipeline_param import extract_pipelineparams_from_any, PipelineParam

_SDK_VERSION_LABEL = 'pipelines.kubeflow.org/kfp_sdk_version'
_SDK_ENV_LABEL = 'pipelines.kubeflow.org/pipeline-sdk-type'
_SDK_ENV_DEFAULT = 'kfp'


class Compiler(object):
    """DSL Compiler that compiles pipeline functions into workflow yaml.

    Example:
      How to use the compiler to construct workflow yaml::

        @dsl.pipeline(
          name='name',
          description='description'
        )
        def my_pipeline(a: int = 1, b: str = "default value"):
          ...

        Compiler().compile(my_pipeline, 'path/to/workflow.yaml')
    """

    def __init__(self,
                 mode: dsl.PipelineExecutionMode = kfp.dsl.PipelineExecutionMode
                 .V1_LEGACY,
                 launcher_image: Optional[str] = None):
        """Creates a KFP compiler for compiling pipeline functions for
        execution.

        Args:
          mode: The pipeline execution mode to use, defaults to kfp.dsl.PipelineExecutionMode.V1_LEGACY.
          launcher_image: Configurable image for KFP launcher to use. Only applies
            when `mode == dsl.PipelineExecutionMode.V2_COMPATIBLE`. Should only be
            needed for tests or custom deployments right now.
        """
        if mode == dsl.PipelineExecutionMode.V2_ENGINE:
            raise ValueError('V2_ENGINE execution mode is not supported yet.')

        if mode == dsl.PipelineExecutionMode.V2_COMPATIBLE:
            warnings.warn('V2_COMPATIBLE execution mode is at Beta quality.'
                          ' Some pipeline features may not work as expected.')
        self._mode = mode
        self._launcher_image = launcher_image
        self._pipeline_name_param: Optional[dsl.PipelineParam] = None
        self._pipeline_root_param: Optional[dsl.PipelineParam] = None

    def _get_groups_for_ops(self, root_group):
        """Helper function to get belonging groups for each op.

        Each pipeline has a root group. Each group has a list of operators (leaf) and groups.
        This function traverse the tree and get all ancestor groups for all operators.

        Returns:
          A dict. Key is the operator's name. Value is a list of ancestor groups including the
                  op itself. The list of a given operator is sorted in a way that the farthest
                  group is the first and operator itself is the last.
        """

        def _get_op_groups_helper(current_groups, ops_to_groups):
            root_group = current_groups[-1]
            for g in root_group.groups:
                # Add recursive opsgroup in the ops_to_groups
                # such that the i/o dependency can be propagated to the ancester opsgroups
                if g.recursive_ref:
                    ops_to_groups[g.name] = [x.name for x in current_groups
                                            ] + [g.name]
                    continue
                current_groups.append(g)
                _get_op_groups_helper(current_groups, ops_to_groups)
                del current_groups[-1]
            for op in root_group.ops:
                ops_to_groups[op.name] = [x.name for x in current_groups
                                         ] + [op.name]

        ops_to_groups = {}
        current_groups = [root_group]
        _get_op_groups_helper(current_groups, ops_to_groups)
        return ops_to_groups

    #TODO: combine with the _get_groups_for_ops
    def _get_groups_for_opsgroups(self, root_group):
        """Helper function to get belonging groups for each opsgroup.

        Each pipeline has a root group. Each group has a list of operators (leaf) and groups.
        This function traverse the tree and get all ancestor groups for all opsgroups.

        Returns:
          A dict. Key is the opsgroup's name. Value is a list of ancestor groups including the
                  opsgroup itself. The list of a given opsgroup is sorted in a way that the farthest
                  group is the first and opsgroup itself is the last.
        """

        def _get_opsgroup_groups_helper(current_groups, opsgroups_to_groups):
            root_group = current_groups[-1]
            for g in root_group.groups:
                # Add recursive opsgroup in the ops_to_groups
                # such that the i/o dependency can be propagated to the ancester opsgroups
                if g.recursive_ref:
                    continue
                opsgroups_to_groups[g.name] = [x.name for x in current_groups
                                              ] + [g.name]
                current_groups.append(g)
                _get_opsgroup_groups_helper(current_groups, opsgroups_to_groups)
                del current_groups[-1]

        opsgroups_to_groups = {}
        current_groups = [root_group]
        _get_opsgroup_groups_helper(current_groups, opsgroups_to_groups)
        return opsgroups_to_groups

    def _get_groups(self, root_group):
        """Helper function to get all groups (not including ops) in a
        pipeline."""

        def _get_groups_helper(group):
            groups = {group.name: group}
            for g in group.groups:
                # Skip the recursive opsgroup because no templates
                # need to be generated for the recursive opsgroups.
                if not g.recursive_ref:
                    groups.update(_get_groups_helper(g))
            return groups

        return _get_groups_helper(root_group)

    def _get_uncommon_ancestors(self, op_groups, opsgroup_groups, op1, op2):
        """Helper function to get unique ancestors between two ops.

        For example, op1's ancestor groups are [root, G1, G2, G3, op1],
        op2's ancestor groups are [root, G1, G4, op2], then it returns a
        tuple ([G2, G3, op1], [G4, op2]).
        """
        #TODO: extract a function for the following two code module
        if op1.name in op_groups:
            op1_groups = op_groups[op1.name]
        elif op1.name in opsgroup_groups:
            op1_groups = opsgroup_groups[op1.name]
        else:
            raise ValueError(op1.name + ' does not exist.')

        if op2.name in op_groups:
            op2_groups = op_groups[op2.name]
        elif op2.name in opsgroup_groups:
            op2_groups = opsgroup_groups[op2.name]
        else:
            raise ValueError(op2.name + ' does not exist.')

        both_groups = [op1_groups, op2_groups]
        common_groups_len = sum(
            1 for x in zip(*both_groups) if x == (x[0],) * len(x))
        group1 = op1_groups[common_groups_len:]
        group2 = op2_groups[common_groups_len:]
        return (group1, group2)

    def _get_condition_params_for_ops(self, root_group):
        """Get parameters referenced in conditions of ops."""
        conditions = defaultdict(set)

        def _get_condition_params_for_ops_helper(group,
                                                 current_conditions_params):
            new_current_conditions_params = current_conditions_params
            if group.type == 'condition':
                new_current_conditions_params = list(current_conditions_params)
                if isinstance(group.condition.operand1, dsl.PipelineParam):
                    new_current_conditions_params.append(
                        group.condition.operand1)
                if isinstance(group.condition.operand2, dsl.PipelineParam):
                    new_current_conditions_params.append(
                        group.condition.operand2)
            for op in group.ops:
                for param in new_current_conditions_params:
                    conditions[op.name].add(param)
            for g in group.groups:
                # If the subgroup is a recursive opsgroup, propagate the pipelineparams
                # in the condition expression, similar to the ops.
                if g.recursive_ref:
                    for param in new_current_conditions_params:
                        conditions[g.name].add(param)
                else:
                    _get_condition_params_for_ops_helper(
                        g, new_current_conditions_params)

        _get_condition_params_for_ops_helper(root_group, [])
        return conditions

    def _get_next_group_or_op(cls, to_visit: List, already_visited: Set):
        """Get next group or op to visit."""
        if len(to_visit) == 0:
            return None
        next = to_visit.pop(0)
        while next in already_visited:
            next = to_visit.pop(0)
        already_visited.add(next)
        return next

    def _get_for_loop_ops(self, new_root) -> Dict[Text, dsl.ParallelFor]:
        to_visit = self._get_all_subgroups_and_ops(new_root)
        op_name_to_op = {}
        already_visited = set()

        while len(to_visit):
            next_op = self._get_next_group_or_op(to_visit, already_visited)
            if next_op is None:
                break
            to_visit.extend(self._get_all_subgroups_and_ops(next_op))
            if isinstance(next_op, dsl.ParallelFor):
                op_name_to_op[next_op.name] = next_op

        return op_name_to_op

    def _get_all_subgroups_and_ops(self, op):
        """Get all ops and groups contained within this group."""
        subgroups = []
        if hasattr(op, 'ops'):
            subgroups.extend(op.ops)
        if hasattr(op, 'groups'):
            subgroups.extend(op.groups)
        return subgroups

    def _get_inputs_outputs(
        self,
        pipeline,
        root_group,
        op_groups,
        opsgroup_groups,
        condition_params,
        op_name_to_for_loop_op: Dict[Text, dsl.ParallelFor],
    ):
        """Get inputs and outputs of each group and op.

        Returns:
          A tuple (inputs, outputs).
          inputs and outputs are dicts with key being the group/op names and values being list of
          tuples (param_name, producing_op_name). producing_op_name is the name of the op that
          produces the param. If the param is a pipeline param (no producer op), then
          producing_op_name is None.
        """
        inputs = defaultdict(set)
        outputs = defaultdict(set)

        for op in pipeline.ops.values():
            # op's inputs and all params used in conditions for that op are both considered.
            for param in op.inputs + list(condition_params[op.name]):
                # if the value is already provided (immediate value), then no need to expose
                # it as input for its parent groups.
                if param.value:
                    continue
                if param.op_name:
                    upstream_op = pipeline.ops[param.op_name]
                    upstream_groups, downstream_groups = \
                      self._get_uncommon_ancestors(op_groups, opsgroup_groups, upstream_op, op)
                    for i, group_name in enumerate(downstream_groups):
                        if i == 0:
                            # If it is the first uncommon downstream group, then the input comes from
                            # the first uncommon upstream group.
                            inputs[group_name].add(
                                (param.full_name, upstream_groups[0]))
                        else:
                            # If not the first downstream group, then the input is passed down from
                            # its ancestor groups so the upstream group is None.
                            inputs[group_name].add((param.full_name, None))
                    for i, group_name in enumerate(upstream_groups):
                        if i == len(upstream_groups) - 1:
                            # If last upstream group, it is an operator and output comes from container.
                            outputs[group_name].add((param.full_name, None))
                        else:
                            # If not last upstream group, output value comes from one of its child.
                            outputs[group_name].add(
                                (param.full_name, upstream_groups[i + 1]))
                else:
                    if not op.is_exit_handler:
                        for group_name in op_groups[op.name][::-1]:
                            # if group is for loop group and param is that loop's param, then the param
                            # is created by that for loop ops_group and it shouldn't be an input to
                            # any of its parent groups.
                            inputs[group_name].add((param.full_name, None))
                            if group_name in op_name_to_for_loop_op:
                                # for example:
                                #   loop_group.loop_args.name = 'loop-item-param-99ca152e'
                                #   param.name =                'loop-item-param-99ca152e--a'
                                loop_group = op_name_to_for_loop_op[group_name]
                                if loop_group.loop_args.name in param.name:
                                    break

        # Generate the input/output for recursive opsgroups
        # It propagates the recursive opsgroups IO to their ancester opsgroups
        def _get_inputs_outputs_recursive_opsgroup(group):
            #TODO: refactor the following codes with the above
            if group.recursive_ref:
                params = [(param, False) for param in group.inputs]
                params.extend([(param, True)
                               for param in list(condition_params[group.name])])
                for param, is_condition_param in params:
                    if param.value:
                        continue
                    full_name = param.full_name
                    if param.op_name:
                        upstream_op = pipeline.ops[param.op_name]
                        upstream_groups, downstream_groups = \
                          self._get_uncommon_ancestors(op_groups, opsgroup_groups, upstream_op, group)
                        for i, g in enumerate(downstream_groups):
                            if i == 0:
                                inputs[g].add((full_name, upstream_groups[0]))
                            # There is no need to pass the condition param as argument to the downstream ops.
                            #TODO: this might also apply to ops. add a TODO here and think about it.
                            elif i == len(downstream_groups
                                         ) - 1 and is_condition_param:
                                continue
                            else:
                                inputs[g].add((full_name, None))
                        for i, g in enumerate(upstream_groups):
                            if i == len(upstream_groups) - 1:
                                outputs[g].add((full_name, None))
                            else:
                                outputs[g].add(
                                    (full_name, upstream_groups[i + 1]))
                    elif not is_condition_param:
                        for g in op_groups[group.name]:
                            inputs[g].add((full_name, None))
            for subgroup in group.groups:
                _get_inputs_outputs_recursive_opsgroup(subgroup)

        _get_inputs_outputs_recursive_opsgroup(root_group)

        # Generate the input for SubGraph along with parallelfor
        for sub_graph in opsgroup_groups:
            if sub_graph in op_name_to_for_loop_op:
                # The opsgroup list is sorted with the farthest group as the first and
                # the opsgroup itself as the last. To get the latest opsgroup which is
                # not the opsgroup itself -2 is used.
                parent = opsgroup_groups[sub_graph][-2]
                if parent and parent.startswith('subgraph'):
                    # propagate only op's pipeline param from subgraph to parallelfor
                    loop_op = op_name_to_for_loop_op[sub_graph]
                    pipeline_param = loop_op.loop_args.items_or_pipeline_param
                    if loop_op.items_is_pipeline_param and pipeline_param.op_name:
                        param_name = '%s-%s' % (sanitize_k8s_name(
                            pipeline_param.op_name), pipeline_param.name)
                        inputs[parent].add((param_name, pipeline_param.op_name))

        return inputs, outputs

    def _get_dependencies(self, pipeline, root_group, op_groups,
                          opsgroups_groups, opsgroups, condition_params):
        """Get dependent groups and ops for all ops and groups.

        Returns:
          A dict. Key is group/op name, value is a list of dependent groups/ops.
          The dependencies are calculated in the following way: if op2 depends on op1,
          and their ancestors are [root, G1, G2, op1] and [root, G1, G3, G4, op2],
          then G3 is dependent on G2. Basically dependency only exists in the first uncommon
          ancesters in their ancesters chain. Only sibling groups/ops can have dependencies.
        """
        dependencies = defaultdict(set)
        for op in pipeline.ops.values():
            upstream_op_names = set()
            for param in op.inputs + list(condition_params[op.name]):
                if param.op_name:
                    upstream_op_names.add(param.op_name)
            upstream_op_names |= set(op.dependent_names)

            for upstream_op_name in upstream_op_names:
                # the dependent op could be either a BaseOp or an opsgroup
                if upstream_op_name in pipeline.ops:
                    upstream_op = pipeline.ops[upstream_op_name]
                elif upstream_op_name in opsgroups:
                    upstream_op = opsgroups[upstream_op_name]
                else:
                    raise ValueError('compiler cannot find the ' +
                                     upstream_op_name)

                upstream_groups, downstream_groups = self._get_uncommon_ancestors(
                    op_groups, opsgroups_groups, upstream_op, op)
                dependencies[downstream_groups[0]].add(upstream_groups[0])

        # Generate dependencies based on the recursive opsgroups
        #TODO: refactor the following codes with the above
        def _get_dependency_opsgroup(group, dependencies):
            upstream_op_names = set(
                [dependency.name for dependency in group.dependencies])
            if group.recursive_ref:
                for param in group.inputs + list(condition_params[group.name]):
                    if param.op_name:
                        upstream_op_names.add(param.op_name)

            for op_name in upstream_op_names:
                if op_name in pipeline.ops:
                    upstream_op = pipeline.ops[op_name]
                elif op_name in opsgroups:
                    upstream_op = opsgroups[op_name]
                else:
                    raise ValueError('compiler cannot find the ' + op_name)
                upstream_groups, downstream_groups = \
                  self._get_uncommon_ancestors(op_groups, opsgroups_groups, upstream_op, group)
                dependencies[downstream_groups[0]].add(upstream_groups[0])

            for subgroup in group.groups:
                _get_dependency_opsgroup(subgroup, dependencies)

        _get_dependency_opsgroup(root_group, dependencies)

        return dependencies

    def _resolve_value_or_reference(self, value_or_reference,
                                    potential_references):
        """_resolve_value_or_reference resolves values and PipelineParams,
        which could be task parameters or input parameters.

        Args:
          value_or_reference: value or reference to be resolved. It could be basic python types or PipelineParam
          potential_references(dict{str->str}): a dictionary of parameter names to task names
        """
        if isinstance(value_or_reference, dsl.PipelineParam):
            parameter_name = value_or_reference.full_name
            task_names = [
                task_name for param_name, task_name in potential_references
                if param_name == parameter_name
            ]
            if task_names:
                task_name = task_names[0]
                # When the task_name is None, the parameter comes directly from ancient ancesters
                # instead of parents. Thus, it is resolved as the input parameter in the current group.
                if task_name is None:
                    return '{{inputs.parameters.%s}}' % parameter_name
                else:
                    return '{{tasks.%s.outputs.parameters.%s}}' % (
                        task_name, parameter_name)
            else:
                return '{{inputs.parameters.%s}}' % parameter_name
        else:
            return str(value_or_reference)

    @staticmethod
    def _resolve_task_pipeline_param(pipeline_param: PipelineParam,
                                     group_type) -> str:
        if pipeline_param.op_name is None:
            return '{{workflow.parameters.%s}}' % pipeline_param.name
        param_name = '%s-%s' % (sanitize_k8s_name(
            pipeline_param.op_name), pipeline_param.name)
        if group_type == 'subgraph':
            return '{{inputs.parameters.%s}}' % (param_name)
        return '{{tasks.%s.outputs.parameters.%s}}' % (sanitize_k8s_name(
            pipeline_param.op_name), param_name)

    def _group_to_dag_template(self, group, inputs, outputs, dependencies):
        """Generate template given an OpsGroup.

        inputs, outputs, dependencies are all helper dicts.
        """
        template = {'name': group.name}
        if group.parallelism != None:
            template["parallelism"] = group.parallelism

        # Generate inputs section.
        if inputs.get(group.name, None):
            template_inputs = [{'name': x[0]} for x in inputs[group.name]]
            template_inputs.sort(key=lambda x: x['name'])
            template['inputs'] = {'parameters': template_inputs}

        # Generate outputs section.
        if outputs.get(group.name, None):
            template_outputs = []
            for param_name, dependent_name in outputs[group.name]:
                template_outputs.append({
                    'name': param_name,
                    'valueFrom': {
                        'parameter':
                            '{{tasks.%s.outputs.parameters.%s}}' %
                            (dependent_name, param_name)
                    }
                })
            template_outputs.sort(key=lambda x: x['name'])
            template['outputs'] = {'parameters': template_outputs}

        # Generate tasks section.
        tasks = []
        sub_groups = group.groups + group.ops
        for sub_group in sub_groups:
            is_recursive_subgroup = (
                isinstance(sub_group, OpsGroup) and sub_group.recursive_ref)
            # Special handling for recursive subgroup: use the existing opsgroup name
            if is_recursive_subgroup:
                task = {
                    'name': sub_group.recursive_ref.name,
                    'template': sub_group.recursive_ref.name,
                }
            else:
                task = {
                    'name': sub_group.name,
                    'template': sub_group.name,
                }
            if isinstance(sub_group,
                          dsl.OpsGroup) and sub_group.type == 'condition':
                subgroup_inputs = inputs.get(sub_group.name, [])
                condition = sub_group.condition
                operand1_value = self._resolve_value_or_reference(
                    condition.operand1, subgroup_inputs)
                operand2_value = self._resolve_value_or_reference(
                    condition.operand2, subgroup_inputs)
                if condition.operator in ['==', '!=']:
                    operand1_value = '"' + operand1_value + '"'
                    operand2_value = '"' + operand2_value + '"'
                task['when'] = '{} {} {}'.format(operand1_value,
                                                 condition.operator,
                                                 operand2_value)

            # Generate dependencies section for this task.
            if dependencies.get(sub_group.name, None):
                group_dependencies = list(dependencies[sub_group.name])
                group_dependencies.sort()
                task['dependencies'] = group_dependencies

            # Generate arguments section for this task.
            if inputs.get(sub_group.name, None):
                task['arguments'] = {
                    'parameters':
                        self.get_arguments_for_sub_group(
                            sub_group, is_recursive_subgroup, inputs)
                }

            # additional task modifications for withItems and withParam
            if isinstance(sub_group, dsl.ParallelFor):
                if sub_group.items_is_pipeline_param:
                    # these loop args are a 'withParam' rather than 'withItems'.
                    # i.e., rather than a static list, they are either the output of another task or were input
                    # as global pipeline parameters

                    pipeline_param = sub_group.loop_args.items_or_pipeline_param
                    withparam_value = self._resolve_task_pipeline_param(
                        pipeline_param, group.type)
                    if pipeline_param.op_name:
                        # these loop args are the output of another task
                        if 'dependencies' not in task or task[
                                'dependencies'] is None:
                            task['dependencies'] = []
                        if sanitize_k8s_name(
                                pipeline_param.op_name
                        ) not in task[
                                'dependencies'] and group.type != 'subgraph':
                            task['dependencies'].append(
                                sanitize_k8s_name(pipeline_param.op_name))

                    task['withParam'] = withparam_value
                else:
                    # Need to sanitize the dict keys for consistency.
                    loop_tasks = sub_group.loop_args.to_list_for_task_yaml()
                    nested_pipeline_params = extract_pipelineparams_from_any(
                        loop_tasks)

                    # Set dependencies in case of nested pipeline_params
                    map_to_tmpl_var = {
                        str(p):
                        self._resolve_task_pipeline_param(p, group.type)
                        for p in nested_pipeline_params
                    }
                    for pipeline_param in nested_pipeline_params:
                        if pipeline_param.op_name:
                            # these pipeline_param are the output of another task
                            if 'dependencies' not in task or task[
                                    'dependencies'] is None:
                                task['dependencies'] = []
                            if sanitize_k8s_name(pipeline_param.op_name
                                                ) not in task['dependencies']:
                                task['dependencies'].append(
                                    sanitize_k8s_name(pipeline_param.op_name))

                    sanitized_tasks = []
                    if isinstance(loop_tasks[0], dict):
                        for argument_set in loop_tasks:
                            c_dict = {}
                            for k, v in argument_set.items():
                                c_dict[sanitize_k8s_name(k, True)] = v
                            sanitized_tasks.append(c_dict)
                    else:
                        sanitized_tasks = loop_tasks
                    # Replace pipeline param if map_to_tmpl_var not empty
                    task['withItems'] = _process_obj(
                        sanitized_tasks,
                        map_to_tmpl_var) if map_to_tmpl_var else sanitized_tasks

                # We will sort dependencies to have determinitc yaml and thus stable tests
                if task.get('dependencies'):
                    task['dependencies'].sort()

            tasks.append(task)
        tasks.sort(key=lambda x: x['name'])
        template['dag'] = {'tasks': tasks}
        return template

    def get_arguments_for_sub_group(
        self,
        sub_group: Union[OpsGroup, dsl._container_op.BaseOp],
        is_recursive_subgroup: Optional[bool],
        inputs: Dict[Text, Tuple[Text, Text]],
    ):
        arguments = []
        for param_name, dependent_name in inputs[sub_group.name]:
            if is_recursive_subgroup:
                for input_name, input in sub_group.arguments.items():
                    if param_name == input.full_name:
                        break
                referenced_input = sub_group.recursive_ref.arguments[input_name]
                argument_name = referenced_input.full_name
            else:
                argument_name = param_name

            # Preparing argument. It can be pipeline input reference, task output reference or loop item (or loop item attribute
            sanitized_loop_arg_full_name = '---'
            if isinstance(sub_group, dsl.ParallelFor):
                sanitized_loop_arg_full_name = sanitize_k8s_name(
                    sub_group.loop_args.full_name)
            arg_ref_full_name = sanitize_k8s_name(param_name)
            # We only care about the reference to the current loop item, not the outer loops
            if isinstance(sub_group,
                          dsl.ParallelFor) and arg_ref_full_name.startswith(
                              sanitized_loop_arg_full_name):
                if arg_ref_full_name == sanitized_loop_arg_full_name:
                    argument_value = '{{item}}'
                elif _for_loop.LoopArgumentVariable.name_is_loop_arguments_variable(
                        param_name):
                    subvar_name = _for_loop.LoopArgumentVariable.get_subvar_name(
                        param_name)
                    argument_value = '{{item.%s}}' % subvar_name
                else:
                    raise ValueError(
                        "Argument seems to reference the loop item, but not the item itself and not some attribute of the item. param_name: {}, "
                        .format(param_name))
            else:
                if dependent_name:
                    argument_value = '{{tasks.%s.outputs.parameters.%s}}' % (
                        dependent_name, param_name)
                else:
                    argument_value = '{{inputs.parameters.%s}}' % param_name

            arguments.append({
                'name': argument_name,
                'value': argument_value,
            })

        arguments.sort(key=lambda x: x['name'])

        return arguments

    def _create_dag_templates(self,
                              pipeline,
                              op_transformers=None,
                              op_to_templates_handler=None):
        """Create all groups and ops templates in the pipeline.

        Args:
          pipeline: Pipeline context object to get all the pipeline data from.
          op_transformers: A list of functions that are applied to all ContainerOp instances that are being processed.
          op_to_templates_handler: Handler which converts a base op into a list of argo templates.
        """
        op_to_templates_handler = op_to_templates_handler or (
            lambda op: [_op_to_template(op)])
        root_group = pipeline.groups[0]

        # Call the transformation functions before determining the inputs/outputs, otherwise
        # the user would not be able to use pipeline parameters in the container definition
        # (for example as pod labels) - the generated template is invalid.
        for op in pipeline.ops.values():
            for transformer in op_transformers or []:
                transformer(op)

        # Generate core data structures to prepare for argo yaml generation
        #   op_name_to_parent_groups: op name -> list of ancestor groups including the current op
        #   opsgroups: a dictionary of ospgroup.name -> opsgroup
        #   inputs, outputs: group/op names -> list of tuples (full_param_name, producing_op_name)
        #   condition_params: recursive_group/op names -> list of pipelineparam
        #   dependencies: group/op name -> list of dependent groups/ops.
        # Special Handling for the recursive opsgroup
        #   op_name_to_parent_groups also contains the recursive opsgroups
        #   condition_params from _get_condition_params_for_ops also contains the recursive opsgroups
        #   groups does not include the recursive opsgroups
        opsgroups = self._get_groups(root_group)
        op_name_to_parent_groups = self._get_groups_for_ops(root_group)
        opgroup_name_to_parent_groups = self._get_groups_for_opsgroups(
            root_group)
        condition_params = self._get_condition_params_for_ops(root_group)
        op_name_to_for_loop_op = self._get_for_loop_ops(root_group)
        inputs, outputs = self._get_inputs_outputs(
            pipeline,
            root_group,
            op_name_to_parent_groups,
            opgroup_name_to_parent_groups,
            condition_params,
            op_name_to_for_loop_op,
        )
        dependencies = self._get_dependencies(
            pipeline,
            root_group,
            op_name_to_parent_groups,
            opgroup_name_to_parent_groups,
            opsgroups,
            condition_params,
        )

        templates = []
        for opsgroup in opsgroups.keys():
            template = self._group_to_dag_template(opsgroups[opsgroup], inputs,
                                                   outputs, dependencies)
            templates.append(template)

        for op in pipeline.ops.values():
            if hasattr(op, 'importer_spec'):
                raise ValueError(
                    'dsl.importer is not supported with v1 compiler.'
                )

            if self._mode == dsl.PipelineExecutionMode.V2_COMPATIBLE:
                v2_compat.update_op(
                    op,
                    pipeline_name=self._pipeline_name_param,
                    pipeline_root=self._pipeline_root_param,
                    launcher_image=self._launcher_image)
            templates.extend(op_to_templates_handler(op))

            if hasattr(op, 'custom_job_spec'):
                warnings.warn(
                    'CustomJob spec is not supported yet when running on KFP.'
                    ' The component will execute within the KFP cluster.')

        return templates

    def _create_pipeline_workflow(self,
                                  parameter_defaults,
                                  pipeline,
                                  op_transformers=None,
                                  pipeline_conf=None):
        """Create workflow for the pipeline."""

        # Input Parameters
        input_params = []
        for name, value in parameter_defaults.items():
            param = {'name': name}
            if value is not None:
                param['value'] = value
            input_params.append(param)

        # Making the pipeline group name unique to prevent name clashes with templates
        pipeline_group = pipeline.groups[0]
        temp_pipeline_group_name = uuid.uuid4().hex
        pipeline_group.name = temp_pipeline_group_name

        # Templates
        templates = self._create_dag_templates(pipeline, op_transformers)

        # Exit Handler
        exit_handler = None
        if pipeline.groups[0].groups:
            first_group = pipeline.groups[0].groups[0]
            if first_group.type == 'exit_handler':
                exit_handler = first_group.exit_op

        # The whole pipeline workflow
        # It must valid as a subdomain
        pipeline_name = pipeline.name or 'pipeline'

        # Workaround for pipeline name clashing with container template names
        # TODO: Make sure template names cannot clash at all (container, DAG, workflow)
        template_map = {
            template['name'].lower(): template for template in templates
        }
        from ..components._naming import _make_name_unique_by_adding_index
        pipeline_template_name = _make_name_unique_by_adding_index(
            pipeline_name, template_map, '-')

        # Restoring the name of the pipeline template
        pipeline_template = template_map[temp_pipeline_group_name]
        pipeline_template['name'] = pipeline_template_name

        templates.sort(key=lambda x: x['name'])
        workflow = {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'Workflow',
            'metadata': {
                'generateName': pipeline_template_name + '-'
            },
            'spec': {
                'entrypoint': pipeline_template_name,
                'templates': templates,
                'arguments': {
                    'parameters': input_params
                },
                'serviceAccountName': 'pipeline-runner',
            }
        }
        # set parallelism limits at pipeline level
        if pipeline_conf.parallelism:
            workflow['spec']['parallelism'] = pipeline_conf.parallelism

        # set ttl after workflow finishes
        if pipeline_conf.ttl_seconds_after_finished >= 0:
            workflow['spec'][
                'ttlSecondsAfterFinished'] = pipeline_conf.ttl_seconds_after_finished

        if pipeline_conf._pod_disruption_budget_min_available:
            pod_disruption_budget = {
                "minAvailable":
                    pipeline_conf._pod_disruption_budget_min_available
            }
            workflow['spec']['podDisruptionBudget'] = pod_disruption_budget

        if len(pipeline_conf.image_pull_secrets) > 0:
            image_pull_secrets = []
            for image_pull_secret in pipeline_conf.image_pull_secrets:
                image_pull_secrets.append(
                    convert_k8s_obj_to_json(image_pull_secret))
            workflow['spec']['imagePullSecrets'] = image_pull_secrets

        if pipeline_conf.timeout:
            workflow['spec']['activeDeadlineSeconds'] = pipeline_conf.timeout

        if exit_handler:
            workflow['spec']['onExit'] = exit_handler.name

        # This can be overwritten by the task specific
        # nodeselection, specified in the template.
        if pipeline_conf.default_pod_node_selector:
            workflow['spec'][
                'nodeSelector'] = pipeline_conf.default_pod_node_selector

        if pipeline_conf.dns_config:
            workflow['spec']['dnsConfig'] = convert_k8s_obj_to_json(
                pipeline_conf.dns_config)

        if pipeline_conf.image_pull_policy != None:
            if pipeline_conf.image_pull_policy in [
                    "Always", "Never", "IfNotPresent"
            ]:
                for template in workflow["spec"]["templates"]:
                    container = template.get('container', None)
                    if container and "imagePullPolicy" not in container:
                        container[
                            "imagePullPolicy"] = pipeline_conf.image_pull_policy
            else:
                raise ValueError(
                    'Invalid imagePullPolicy. Must be one of `Always`, `Never`, `IfNotPresent`.'
                )
        return workflow

    def _validate_exit_handler(self, pipeline):
        """Makes sure there is only one global exit handler.

        Note this is a temporary workaround until argo supports local
        exit handler.
        """

        def _validate_exit_handler_helper(group, exiting_op_names,
                                          handler_exists):
            if group.type == 'exit_handler':
                if handler_exists or len(exiting_op_names) > 1:
                    raise ValueError(
                        'Only one global exit_handler is allowed and all ops need to be included.'
                    )
                handler_exists = True

            if group.ops:
                exiting_op_names.extend([x.name for x in group.ops])

            for g in group.groups:
                _validate_exit_handler_helper(g, exiting_op_names,
                                              handler_exists)

        return _validate_exit_handler_helper(pipeline.groups[0], [], False)

    def _sanitize_and_inject_artifact(self,
                                      pipeline: dsl.Pipeline,
                                      pipeline_conf=None):
        """Sanitize operator/param names and inject pipeline artifact
        location."""

        # Sanitize operator names and param names
        sanitized_ops = {}

        for op in pipeline.ops.values():
            sanitized_name = sanitize_k8s_name(op.name)
            op.name = sanitized_name
            for param in op.outputs.values():
                param.name = sanitize_k8s_name(param.name, True)
                if param.op_name:
                    param.op_name = sanitize_k8s_name(param.op_name)
            if op.output is not None and not isinstance(
                    op.output, dsl._container_op._MultipleOutputsError):
                op.output.name = sanitize_k8s_name(op.output.name, True)
                op.output.op_name = sanitize_k8s_name(op.output.op_name)
            if op.dependent_names:
                op.dependent_names = [
                    sanitize_k8s_name(name) for name in op.dependent_names
                ]
            if isinstance(op, dsl.ContainerOp) and op.file_outputs is not None:
                sanitized_file_outputs = {}
                for key in op.file_outputs.keys():
                    sanitized_file_outputs[sanitize_k8s_name(
                        key, True)] = op.file_outputs[key]
                op.file_outputs = sanitized_file_outputs
            elif isinstance(
                    op, dsl.ResourceOp) and op.attribute_outputs is not None:
                sanitized_attribute_outputs = {}
                for key in op.attribute_outputs.keys():
                    sanitized_attribute_outputs[sanitize_k8s_name(key, True)] = \
                      op.attribute_outputs[key]
                op.attribute_outputs = sanitized_attribute_outputs
            if isinstance(op, dsl.ContainerOp):
                if op.input_artifact_paths:
                    op.input_artifact_paths = {
                        sanitize_k8s_name(key, True): value
                        for key, value in op.input_artifact_paths.items()
                    }
                if op.artifact_arguments:
                    op.artifact_arguments = {
                        sanitize_k8s_name(key, True): value
                        for key, value in op.artifact_arguments.items()
                    }
            sanitized_ops[sanitized_name] = op
        pipeline.ops = sanitized_ops

    def _create_workflow(
        self,
        pipeline_func: Callable,
        pipeline_name: Optional[Text] = None,
        pipeline_description: Optional[Text] = None,
        params_list: Optional[List[dsl.PipelineParam]] = None,
        pipeline_conf: Optional[dsl.PipelineConf] = None,
    ) -> Dict[Text, Any]:
        """Internal implementation of create_workflow."""
        params_list = params_list or []

        # Create the arg list with no default values and call pipeline function.
        # Assign type information to the PipelineParam
        pipeline_meta = _extract_pipeline_metadata(pipeline_func)
        pipeline_meta.name = pipeline_name or pipeline_meta.name
        pipeline_meta.description = pipeline_description or pipeline_meta.description
        pipeline_name = sanitize_k8s_name(pipeline_meta.name)

        # Need to first clear the default value of dsl.PipelineParams. Otherwise, it
        # will be resolved immediately in place when being to each component.
        default_param_values = OrderedDict()

        if self._pipeline_root_param:
            params_list.append(self._pipeline_root_param)
        if self._pipeline_name_param:
            params_list.append(self._pipeline_name_param)

        for param in params_list:
            default_param_values[param.name] = param.value
            param.value = None

        args_list = []
        kwargs_dict = dict()
        signature = inspect.signature(pipeline_func)
        for arg_name, arg in signature.parameters.items():
            arg_type = None
            for input in pipeline_meta.inputs or []:
                if arg_name == input.name:
                    arg_type = input.type
                    break
            param = dsl.PipelineParam(
                sanitize_k8s_name(arg_name, True), param_type=arg_type)
            if arg.kind == inspect.Parameter.KEYWORD_ONLY:
                kwargs_dict[arg_name] = param
            else:
                args_list.append(param)

        with dsl.Pipeline(pipeline_name) as dsl_pipeline:
            pipeline_func(*args_list, **kwargs_dict)

        pipeline_conf = pipeline_conf or dsl_pipeline.conf  # Configuration passed to the compiler is overriding. Unfortunately, it's not trivial to detect whether the dsl_pipeline.conf was ever modified.

        self._validate_exit_handler(dsl_pipeline)
        self._sanitize_and_inject_artifact(dsl_pipeline, pipeline_conf)

        # Fill in the default values by merging two param lists.
        args_list_with_defaults = OrderedDict()
        if pipeline_meta.inputs:
            args_list_with_defaults = OrderedDict([
                (sanitize_k8s_name(input_spec.name, True), input_spec.default)
                for input_spec in pipeline_meta.inputs
            ])

        if params_list:
            # Or, if args are provided by params_list, fill in pipeline_meta.
            for k, v in default_param_values.items():
                args_list_with_defaults[k] = v

            pipeline_meta.inputs = pipeline_meta.inputs or []
            for param in params_list:
                pipeline_meta.inputs.append(
                    InputSpec(
                        name=param.name,
                        type=param.param_type,
                        default=default_param_values[param.name]))

        op_transformers = [add_pod_env]
        pod_labels = {
            _SDK_VERSION_LABEL: kfp.__version__.replace('+', '-'),
            _SDK_ENV_LABEL: _SDK_ENV_DEFAULT
        }
        op_transformers.append(add_pod_labels(pod_labels))
        op_transformers.extend(pipeline_conf.op_transformers)

        if self._mode == dsl.PipelineExecutionMode.V2_COMPATIBLE:
            # Add self._pipeline_name_param and self._pipeline_root_param to ops inputs
            # if they don't exist already.
            for op in dsl_pipeline.ops.values():
                insert_pipeline_name_param = True
                insert_pipeline_root_param = True
                for param in op.inputs:
                    if param.name == self._pipeline_name_param.name:
                        insert_pipeline_name_param = False
                    elif param.name == self._pipeline_root_param.name:
                        insert_pipeline_root_param = False

                if insert_pipeline_name_param:
                    op.inputs.append(self._pipeline_name_param)
                if insert_pipeline_root_param:
                    op.inputs.append(self._pipeline_root_param)

        workflow = self._create_pipeline_workflow(
            args_list_with_defaults,
            dsl_pipeline,
            op_transformers,
            pipeline_conf,
        )

        from ._data_passing_rewriter import fix_big_data_passing
        workflow = fix_big_data_passing(workflow)

        if pipeline_conf and pipeline_conf.data_passing_method != None:
            workflow = pipeline_conf.data_passing_method(workflow)

        metadata = workflow.setdefault('metadata', {})
        annotations = metadata.setdefault('annotations', {})
        labels = metadata.setdefault('labels', {})

        annotations[_SDK_VERSION_LABEL] = kfp.__version__.replace('+', '-')
        annotations[
            'pipelines.kubeflow.org/pipeline_compilation_time'] = datetime.datetime.now(
            ).isoformat()
        annotations['pipelines.kubeflow.org/pipeline_spec'] = json.dumps(
            pipeline_meta.to_dict(), sort_keys=True)

        if self._mode == dsl.PipelineExecutionMode.V2_COMPATIBLE:
            annotations['pipelines.kubeflow.org/v2_pipeline'] = "true"
            labels['pipelines.kubeflow.org/v2_pipeline'] = "true"

        # Labels might be logged better than annotations so adding some information here as well
        labels[_SDK_VERSION_LABEL] = kfp.__version__.replace('+', '-')

        return workflow

    # For now (0.1.31) this function is only used by TFX's KubeflowDagRunner.
    # See https://github.com/tensorflow/tfx/blob/811e4c1cc0f7903d73d151b9d4f21f79f6013d4a/tfx/orchestration/kubeflow/kubeflow_dag_runner.py#L238
    @deprecated(
        version='0.1.32',
        reason='Workflow spec is not intended to be handled by user, please '
        'switch to _create_workflow')
    def create_workflow(
            self,
            pipeline_func: Callable,
            pipeline_name: Text = None,
            pipeline_description: Text = None,
            params_list: List[dsl.PipelineParam] = None,
            pipeline_conf: dsl.PipelineConf = None) -> Dict[Text, Any]:
        """Create workflow spec from pipeline function and specified pipeline
        params/metadata. Currently, the pipeline params are either specified in
        the signature of the pipeline function or by passing a list of
        dsl.PipelineParam. Conflict will cause ValueError.

        Args:
          pipeline_func: Pipeline function where ContainerOps are invoked.
          pipeline_name: The name of the pipeline to compile.
          pipeline_description: The description of the pipeline.
          params_list: List of pipeline params to append to the pipeline.
          pipeline_conf: PipelineConf instance. Can specify op transforms, image pull secrets and other pipeline-level configuration options. Overrides any configuration that may be set by the pipeline.

        Returns:
          The created workflow dictionary.
        """
        return self._create_workflow(pipeline_func, pipeline_name,
                                     pipeline_description, params_list,
                                     pipeline_conf)

    @deprecated(version='0.1.32', reason='Switch to _create_workflow.')
    def _compile(self, pipeline_func, pipeline_conf: dsl.PipelineConf = None):
        """Compile the given pipeline function into workflow."""
        return self._create_workflow(
            pipeline_func=pipeline_func, pipeline_conf=pipeline_conf)

    def compile(self,
                pipeline_func,
                package_path,
                type_check: bool = True,
                pipeline_conf: Optional[dsl.PipelineConf] = None):
        """Compile the given pipeline function into workflow yaml.

        Args:
          pipeline_func: Pipeline functions with @dsl.pipeline decorator.
          package_path: The output workflow tar.gz file path. for example,
            "~/a.tar.gz"
          type_check: Whether to enable the type check or not, default: True.
          pipeline_conf: PipelineConf instance. Can specify op transforms, image
            pull secrets and other pipeline-level configuration options. Overrides
            any configuration that may be set by the pipeline.
        """
        pipeline_root_dir = getattr(pipeline_func, 'pipeline_root', None)
        if (pipeline_root_dir is not None or
                self._mode == dsl.PipelineExecutionMode.V2_COMPATIBLE):
            self._pipeline_root_param = dsl.PipelineParam(
                name=dsl.ROOT_PARAMETER_NAME, value=pipeline_root_dir or '')

        if self._mode == dsl.PipelineExecutionMode.V2_COMPATIBLE:
            pipeline_name = getattr(pipeline_func, '_component_human_name', '')
            if not pipeline_name:
                raise ValueError(
                    '@dsl.pipeline decorator name field is required in v2 compatible mode'
                )
            # pipeline names have one of the following formats:
            # * pipeline/<name>
            # * namespace/<ns>/pipeline/<name>
            # when compiling, we will only have pipeline/<name>, but it will be overriden
            # when uploading the pipeline to KFP API server.
            self._pipeline_name_param = dsl.PipelineParam(
                name='pipeline-name', value=f'pipeline/{pipeline_name}')

        import kfp
        type_check_old_value = kfp.TYPE_CHECK
        compiling_for_v2_old_value = kfp.COMPILING_FOR_V2
        kfp.COMPILING_FOR_V2 = self._mode in [
            dsl.PipelineExecutionMode.V2_COMPATIBLE,
            dsl.PipelineExecutionMode.V2_ENGINE,
        ]

        try:
            kfp.TYPE_CHECK = type_check
            self._create_and_write_workflow(
                pipeline_func=pipeline_func,
                pipeline_conf=pipeline_conf,
                package_path=package_path)
        finally:
            kfp.TYPE_CHECK = type_check_old_value
            kfp.COMPILING_FOR_V2 = compiling_for_v2_old_value

    @staticmethod
    def _write_workflow(workflow: Dict[Text, Any], package_path: Text = None):
        """Dump pipeline workflow into yaml spec and write out in the format
        specified by the user.

        Args:
          workflow: Workflow spec of the pipline, dict.
          package_path: file path to be written. If not specified, a yaml_text string will be returned.
        """
        yaml_text = dump_yaml(workflow)

        if package_path is None:
            return yaml_text

        if package_path.endswith('.tar.gz') or package_path.endswith('.tgz'):
            from contextlib import closing
            from io import BytesIO
            with tarfile.open(package_path, "w:gz") as tar:
                with closing(BytesIO(yaml_text.encode())) as yaml_file:
                    tarinfo = tarfile.TarInfo('pipeline.yaml')
                    tarinfo.size = len(yaml_file.getvalue())
                    tar.addfile(tarinfo, fileobj=yaml_file)
        elif package_path.endswith('.zip'):
            with zipfile.ZipFile(package_path, "w") as zip:
                zipinfo = zipfile.ZipInfo('pipeline.yaml')
                zipinfo.compress_type = zipfile.ZIP_DEFLATED
                zip.writestr(zipinfo, yaml_text)
        elif package_path.endswith('.yaml') or package_path.endswith('.yml'):
            with open(package_path, 'w') as yaml_file:
                yaml_file.write(yaml_text)
        else:
            raise ValueError('The output path ' + package_path +
                             ' should ends with one of the following formats: '
                             '[.tar.gz, .tgz, .zip, .yaml, .yml]')

    def _create_and_write_workflow(self,
                                   pipeline_func: Callable,
                                   pipeline_name: Text = None,
                                   pipeline_description: Text = None,
                                   params_list: List[dsl.PipelineParam] = None,
                                   pipeline_conf: dsl.PipelineConf = None,
                                   package_path: Text = None) -> None:
        """Compile the given pipeline function and dump it to specified file
        format."""
        workflow = self._create_workflow(pipeline_func, pipeline_name,
                                         pipeline_description, params_list,
                                         pipeline_conf)
        self._write_workflow(workflow, package_path)
        _validate_workflow(workflow)


def _validate_workflow(workflow: dict):
    workflow = workflow.copy()
    # Working around Argo lint issue
    for argument in workflow['spec'].get('arguments', {}).get('parameters', []):
        if 'value' not in argument:
            argument['value'] = ''

    yaml_text = dump_yaml(workflow)
    if '{{pipelineparam' in yaml_text:
        raise RuntimeError(
            '''Internal compiler error: Found unresolved PipelineParam.
Please create a new issue at https://github.com/kubeflow/pipelines/issues attaching the pipeline code and the pipeline package.'''
        )

    # Running Argo lint if available
    import shutil
    import subprocess
    argo_path = shutil.which('argo')
    if argo_path:
        has_working_argo_lint = False
        try:
            has_working_argo_lint = _run_argo_lint("""
        apiVersion: argoproj.io/v1alpha1
        kind: Workflow
        metadata:
          generateName: hello-world-
        spec:
          entrypoint: whalesay
          templates:
          - name: whalesay
            container:
              image: docker/whalesay:latest""")
        except:
            warnings.warn(
                "Cannot validate the compiled workflow. Found the argo program in PATH, but it's not usable. argo CLI v3.1.1+ should work."
            )

        if has_working_argo_lint:
            _run_argo_lint(yaml_text)


def _run_argo_lint(yaml_text: str):
    # Running Argo lint if available
    import shutil
    import subprocess
    argo_path = shutil.which('argo')
    if argo_path:
        result = subprocess.run([
            argo_path, '--offline=true', '--kinds=workflows', 'lint',
            '/dev/stdin'
        ],
                                input=yaml_text.encode('utf-8'),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        if result.returncode:
            if re.match(
                    pattern=r'.+failed to resolve {{tasks\..+\.outputs\.artifacts\..+}}.+',
                    string=result.stderr.decode('utf-8')):
                raise RuntimeError(
                    'Compiler has produced Argo-incompatible workflow due to '
                    'unresolvable input artifact(s). Please check whether inputPath has'
                    ' been connected to outputUri placeholder, which is not supported '
                    'yet. Otherwise, please create a new issue at '
                    'https://github.com/kubeflow/pipelines/issues attaching the '
                    'pipeline code and the pipeline package. Error: {}'.format(
                        result.stderr.decode('utf-8')))
            raise RuntimeError(
                '''Internal compiler error: Compiler has produced Argo-incompatible workflow.
Please create a new issue at https://github.com/kubeflow/pipelines/issues attaching the pipeline code and the pipeline package.
Error: {}'''.format(result.stderr.decode('utf-8')))

        return True
    return False
