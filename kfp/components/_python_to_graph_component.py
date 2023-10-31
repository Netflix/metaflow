# Copyright 2019 The Kubeflow Authors
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

__all__ = [
    'create_graph_component_from_pipeline_func',
]

import inspect
from collections import OrderedDict
from typing import Callable, Mapping, Optional

from . import _components
from . import structures
from ._structures import TaskSpec, ComponentSpec, OutputSpec, GraphInputReference, TaskOutputArgument, GraphImplementation, GraphSpec
from ._naming import _make_name_unique_by_adding_index
from ._python_op import _extract_component_interface
from ._components import _create_task_factory_from_component_spec


def create_graph_component_from_pipeline_func(
    pipeline_func: Callable,
    output_component_file: str = None,
    embed_component_specs: bool = False,
    annotations: Optional[Mapping[str, str]] = None,
) -> Callable:
    """Creates graph component definition from a python pipeline function. The
    component file can be published for sharing.

    Pipeline function is a function that only calls component functions and passes outputs to inputs.
    This feature is experimental and lacks support for some of the DSL features like conditions and loops.
    Only pipelines consisting of loaded components or python components are currently supported (no manually created ContainerOps or ResourceOps).

    .. warning::

        Please note this feature is considered experimental!

    Args:
        pipeline_func: Python function to convert
        output_component_file: Path of the file where the component definition will be written. The `component.yaml` file can then be published for sharing.
        embed_component_specs: Whether to embed component definitions or just reference them. Embedding makes the graph component self-contained. Default is False.
        annotations: Optional. Allows adding arbitrary key-value data to the component specification.

    Returns:
        A function representing the graph component. The component spec can be accessed using the .component_spec attribute.
        The function will have the same parameters as the original function.
        When called, the function will return a task object, corresponding to the graph component.
        To reference the outputs of the task, use task.outputs["Output name"].

    Example::

        producer_op = load_component_from_file('producer/component.yaml')
        processor_op = load_component_from_file('processor/component.yaml')

        def pipeline1(pipeline_param_1: int):
            producer_task = producer_op()
            processor_task = processor_op(pipeline_param_1, producer_task.outputs['Output 2'])

            return OrderedDict([
                ('Pipeline output 1', producer_task.outputs['Output 1']),
                ('Pipeline output 2', processor_task.outputs['Output 2']),
            ])

        create_graph_component_from_pipeline_func(pipeline1, output_component_file='pipeline.component.yaml')
    """
    component_spec = create_graph_component_spec_from_pipeline_func(
        pipeline_func, embed_component_specs)
    if annotations:
        component_spec.metadata = structures.MetadataSpec(
            annotations=annotations,)
    if output_component_file:
        from pathlib import Path
        from ._yaml_utils import dump_yaml
        component_dict = component_spec.to_dict()
        component_yaml = dump_yaml(component_dict)
        Path(output_component_file).write_text(component_yaml)

    return _create_task_factory_from_component_spec(component_spec)


def create_graph_component_spec_from_pipeline_func(
        pipeline_func: Callable,
        embed_component_specs: bool = False) -> ComponentSpec:

    component_spec = _extract_component_interface(pipeline_func)
    # Checking the function parameters - they should not have file passing annotations.
    input_specs = component_spec.inputs or []
    for input in input_specs:
        if input._passing_style:
            raise TypeError(
                'Graph component function parameter "{}" cannot have file-passing annotation "{}".'
                .format(input.name, input._passing_style))

    task_map = OrderedDict()  #Preserving task order

    from ._components import _create_task_spec_from_component_and_arguments

    def task_construction_handler(
        component_spec,
        arguments,
        component_ref,
    ):
        task = _create_task_spec_from_component_and_arguments(
            component_spec=component_spec,
            arguments=arguments,
            component_ref=component_ref,
        )

        #Rewriting task ids so that they're same every time
        task_id = task.component_ref.spec.name or "Task"
        task_id = _make_name_unique_by_adding_index(task_id, task_map.keys(),
                                                    ' ')
        for output_ref in task.outputs.values():
            output_ref.task_output.task_id = task_id
            output_ref.task_output.task = None
        task_map[task_id] = task
        # Remove the component spec from component reference unless it will make the reference empty or unless explicitly asked by the user
        if not embed_component_specs and any([
                task.component_ref.name, task.component_ref.url,
                task.component_ref.digest
        ]):
            task.component_ref.spec = None

        return task  #The handler is a transformation function, so it must pass the task through.

    # Preparing the pipeline_func arguments
    # TODO: The key should be original parameter name if different
    pipeline_func_args = {
        input.name: GraphInputReference(input_name=input.name).as_argument()
        for input in input_specs
    }

    try:
        #Setting the handler to fix and catch the tasks.
        # FIX: The handler only hooks container component creation
        old_handler = _components._container_task_constructor
        _components._container_task_constructor = task_construction_handler

        #Calling the pipeline_func with GraphInputArgument instances as arguments
        pipeline_func_result = pipeline_func(**pipeline_func_args)
    finally:
        _components._container_task_constructor = old_handler

    # Getting graph outputs
    output_names = [output.name for output in (component_spec.outputs or [])]

    if len(output_names) == 1 and output_names[
            0] == 'Output':  # TODO: Check whether the NamedTuple syntax was used
        pipeline_func_result = [pipeline_func_result]

    if isinstance(pipeline_func_result, tuple) and hasattr(
            pipeline_func_result,
            '_asdict'):  # collections.namedtuple and typing.NamedTuple
        pipeline_func_result = pipeline_func_result._asdict()

    if isinstance(pipeline_func_result, dict):
        if output_names:
            if set(output_names) != set(pipeline_func_result.keys()):
                raise ValueError(
                    'Returned outputs do not match outputs specified in the function signature: {} = {}'
                    .format(
                        str(set(pipeline_func_result.keys())),
                        str(set(output_names))))

    if pipeline_func_result is None:
        graph_output_value_map = {}
    elif isinstance(pipeline_func_result, dict):
        graph_output_value_map = OrderedDict(pipeline_func_result)
    elif isinstance(pipeline_func_result, (list, tuple)):
        if output_names:
            if len(pipeline_func_result) != len(output_names):
                raise ValueError(
                    'Expected {} values from pipeline function, but got {}.'
                    .format(len(output_names), len(pipeline_func_result)))
            graph_output_value_map = OrderedDict(
                (name_value[0], name_value[1])
                for name_value in zip(output_names, pipeline_func_result))
        else:
            graph_output_value_map = OrderedDict(
                (output_value.task_output.output_name, output_value)
                for output_value in pipeline_func_result
            )  # TODO: Fix possible name non-uniqueness (e.g. use task id as prefix or add index to non-unique names)
    else:
        raise TypeError('Pipeline must return outputs as tuple or OrderedDict.')

    #Checking the pipeline_func output object types
    for output_name, output_value in graph_output_value_map.items():
        if not isinstance(output_value, TaskOutputArgument):
            raise TypeError(
                'Only TaskOutputArgument instances should be returned from graph component, but got "{}" = "{}".'
                .format(output_name, str(output_value)))

    if not component_spec.outputs and graph_output_value_map:
        component_spec.outputs = [
            OutputSpec(name=output_name, type=output_value.task_output.type)
            for output_name, output_value in graph_output_value_map.items()
        ]

    component_spec.implementation = GraphImplementation(
        graph=GraphSpec(
            tasks=task_map,
            output_values=graph_output_value_map,
        ))
    return component_spec
