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

__all__ = [
    'InputSpec',
    'OutputSpec',
    'InputValuePlaceholder',
    'InputPathPlaceholder',
    'OutputPathPlaceholder',
    'InputUriPlaceholder',
    'OutputUriPlaceholder',
    'InputMetadataPlaceholder',
    'InputOutputPortNamePlaceholder',
    'OutputMetadataPlaceholder',
    'ExecutorInputPlaceholder',
    'ConcatPlaceholder',
    'IsPresentPlaceholder',
    'IfPlaceholderStructure',
    'IfPlaceholder',
    'ContainerSpec',
    'ContainerImplementation',
    'ComponentSpec',
    'ComponentReference',
    'GraphInputReference',
    'GraphInputArgument',
    'TaskOutputReference',
    'TaskOutputArgument',
    'EqualsPredicate',
    'NotEqualsPredicate',
    'GreaterThanPredicate',
    'GreaterThanOrEqualPredicate',
    'LessThenPredicate',
    'LessThenOrEqualPredicate',
    'NotPredicate',
    'AndPredicate',
    'OrPredicate',
    'RetryStrategySpec',
    'CachingStrategySpec',
    'ExecutionOptionsSpec',
    'TaskSpec',
    'GraphSpec',
    'GraphImplementation',
    'PipelineRunSpec',
]

from collections import OrderedDict

from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .modelbase import ModelBase

PrimitiveTypes = Union[str, int, float, bool]
PrimitiveTypesIncludingNone = Optional[PrimitiveTypes]

TypeSpecType = Union[str, Dict, List]


class InputSpec(ModelBase):
    """Describes the component input specification."""

    def __init__(
        self,
        name: str,
        type: Optional[TypeSpecType] = None,
        description: Optional[str] = None,
        default: Optional[PrimitiveTypes] = None,
        optional: Optional[bool] = False,
        annotations: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(locals())


class OutputSpec(ModelBase):
    """Describes the component output specification."""

    def __init__(
        self,
        name: str,
        type: Optional[TypeSpecType] = None,
        description: Optional[str] = None,
        annotations: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(locals())


class InputValuePlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by the input argument value."""
    _serialized_names = {
        'input_name': 'inputValue',
    }

    def __init__(
        self,
        input_name: str,
    ):
        super().__init__(locals())


class InputPathPlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by a local file path pointing to a file containing the input
    argument value."""
    _serialized_names = {
        'input_name': 'inputPath',
    }

    def __init__(
        self,
        input_name: str,
    ):
        super().__init__(locals())


class OutputPathPlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by a local file path pointing to a file where the program
    should write its output data."""
    _serialized_names = {
        'output_name': 'outputPath',
    }

    def __init__(
        self,
        output_name: str,
    ):
        super().__init__(locals())


class InputUriPlaceholder(ModelBase):  # Non-standard attr names
    """Represents a placeholder for the URI of an input artifact.

    Represents the command-line argument placeholder that will be
    replaced at run-time by the URI of the input artifact argument.
    """
    _serialized_names = {
        'input_name': 'inputUri',
    }

    def __init__(
        self,
        input_name: str,
    ):
        super().__init__(locals())


class OutputUriPlaceholder(ModelBase):  # Non-standard attr names
    """Represents a placeholder for the URI of an output artifact.

    Represents the command-line argument placeholder that will be
    replaced at run-time by a URI of the output artifac where the
    program should write its output data.
    """
    _serialized_names = {
        'output_name': 'outputUri',
    }

    def __init__(
        self,
        output_name: str,
    ):
        super().__init__(locals())


class InputMetadataPlaceholder(ModelBase):  # Non-standard attr names
    """Represents the file path to an input artifact metadata.

    During runtime, this command-line argument placeholder will be
    replaced by the path where the metadata file associated with this
    artifact has been written to. Currently only supported in v2
    components.
    """
    _serialized_names = {
        'input_name': 'inputMetadata',
    }

    def __init__(self, input_name: str):
        super().__init__(locals())


class InputOutputPortNamePlaceholder(ModelBase):  # Non-standard attr names
    """Represents the output port name of an input artifact.

    During compile time, this command-line argument placeholder will be
    replaced by the actual output port name used by the producer task.
    Currently only supported in v2 components.
    """
    _serialized_names = {
        'input_name': 'inputOutputPortName',
    }

    def __init__(self, input_name: str):
        super().__init__(locals())


class OutputMetadataPlaceholder(ModelBase):  # Non-standard attr names
    """Represents the output metadata JSON file location of this task.

    This file will encode the metadata information produced by this task:
    - Artifacts metadata, but not the content of the artifact, and
    - output parameters.

    Only supported in v2 components.
    """
    _serialized_names = {
        'output_metadata': 'outputMetadata',
    }

    def __init__(self, output_metadata: type(None) = None):
        if output_metadata:
            raise RuntimeError(
                'Output metadata placeholder cannot be associated with key')
        super().__init__(locals())

    def to_dict(self) -> Mapping[str, Any]:
        # Override parent implementation. Otherwise it always returns {}.
        return {'outputMetadata': None}


class ExecutorInputPlaceholder(ModelBase):  # Non-standard attr names
    """Represents the serialized ExecutorInput message at runtime.

    This placeholder will be replaced by a serialized
    [ExecutorInput](https://github.com/kubeflow/pipelines/blob/61f9c2c328d245d89c9d9b8c923f24dbbd08cdc9/api/v2alpha1/pipeline_spec.proto#L730)
    proto message at runtime, which includes parameters of the task, artifact
    URIs and metadata.
    """
    _serialized_names = {
        'executor_input': 'executorInput',
    }

    def __init__(self, executor_input: type(None) = None):
        if executor_input:
            raise RuntimeError(
                'Executor input placeholder cannot be associated with input key'
                '. Got %s' % executor_input)
        super().__init__(locals())

    def to_dict(self) -> Mapping[str, Any]:
        # Override parent implementation. Otherwise it always returns {}.
        return {'executorInput': None}


CommandlineArgumentType = Union[str, InputValuePlaceholder,
                                InputPathPlaceholder, OutputPathPlaceholder,
                                InputUriPlaceholder, OutputUriPlaceholder,
                                InputMetadataPlaceholder,
                                InputOutputPortNamePlaceholder,
                                OutputMetadataPlaceholder,
                                ExecutorInputPlaceholder, 'ConcatPlaceholder',
                                'IfPlaceholder',]


class ConcatPlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by the concatenated values of its items."""
    _serialized_names = {
        'items': 'concat',
    }

    def __init__(
        self,
        items: List[CommandlineArgumentType],
    ):
        super().__init__(locals())


class IsPresentPlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by a boolean value specifying whether the caller has passed an
    argument for the specified optional input."""
    _serialized_names = {
        'input_name': 'isPresent',
    }

    def __init__(
        self,
        input_name: str,
    ):
        super().__init__(locals())


IfConditionArgumentType = Union[bool, str, IsPresentPlaceholder,
                                InputValuePlaceholder]


class IfPlaceholderStructure(ModelBase):  #Non-standard attr names
    '''Used in by the IfPlaceholder - the command-line argument placeholder that will be replaced at run-time by the expanded value of either "then_value" or "else_value" depending on the submissio-time resolved value of the "cond" predicate.'''
    _serialized_names = {
        'condition': 'cond',
        'then_value': 'then',
        'else_value': 'else',
    }

    def __init__(
        self,
        condition: IfConditionArgumentType,
        then_value: Union[CommandlineArgumentType,
                          List[CommandlineArgumentType]],
        else_value: Optional[Union[CommandlineArgumentType,
                                   List[CommandlineArgumentType]]] = None,
    ):
        super().__init__(locals())


class IfPlaceholder(ModelBase):  #Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced
    at run-time by the expanded value of either "then_value" or "else_value"
    depending on the submissio-time resolved value of the "cond" predicate."""
    _serialized_names = {
        'if_structure': 'if',
    }

    def __init__(
        self,
        if_structure: IfPlaceholderStructure,
    ):
        super().__init__(locals())


class ContainerSpec(ModelBase):
    """Describes the container component implementation."""
    _serialized_names = {
        'file_outputs':
            'fileOutputs',  #TODO: rename to something like legacy_unconfigurable_output_paths
    }

    def __init__(
            self,
            image: str,
            command: Optional[List[CommandlineArgumentType]] = None,
            args: Optional[List[CommandlineArgumentType]] = None,
            env: Optional[Mapping[str, str]] = None,
            file_outputs:
        Optional[Mapping[
            str,
            str]] = None,  #TODO: rename to something like legacy_unconfigurable_output_paths
    ):
        super().__init__(locals())


class ContainerImplementation(ModelBase):
    """Represents the container component implementation."""

    def __init__(
        self,
        container: ContainerSpec,
    ):
        super().__init__(locals())


ImplementationType = Union[ContainerImplementation, 'GraphImplementation']


class MetadataSpec(ModelBase):

    def __init__(
        self,
        annotations: Optional[Dict[str, str]] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        super().__init__(locals())


class ComponentSpec(ModelBase):
    """Component specification.

    Describes the metadata (name, description, annotations and labels),
    the interface (inputs and outputs) and the implementation of the
    component.
    """

    def __init__(
        self,
        name: Optional[str] = None,  #? Move to metadata?
        description: Optional[str] = None,  #? Move to metadata?
        metadata: Optional[MetadataSpec] = None,
        inputs: Optional[List[InputSpec]] = None,
        outputs: Optional[List[OutputSpec]] = None,
        implementation: Optional[ImplementationType] = None,
        version: Optional[str] = 'google.com/cloud/pipelines/component/v1',
        #tags: Optional[Set[str]] = None,
    ):
        super().__init__(locals())
        self._post_init()

    def _post_init(self):
        #Checking input names for uniqueness
        self._inputs_dict = {}
        if self.inputs:
            for input in self.inputs:
                if input.name in self._inputs_dict:
                    raise ValueError('Non-unique input name "{}"'.format(
                        input.name))
                self._inputs_dict[input.name] = input

        #Checking output names for uniqueness
        self._outputs_dict = {}
        if self.outputs:
            for output in self.outputs:
                if output.name in self._outputs_dict:
                    raise ValueError('Non-unique output name "{}"'.format(
                        output.name))
                self._outputs_dict[output.name] = output

        if isinstance(self.implementation, ContainerImplementation):
            container = self.implementation.container

            if container.file_outputs:
                for output_name, path in container.file_outputs.items():
                    if output_name not in self._outputs_dict:
                        raise TypeError(
                            'Unconfigurable output entry "{}" references non-existing output.'
                            .format({output_name: path}))

            def verify_arg(arg):
                if arg is None:
                    pass
                elif isinstance(
                        arg, (str, int, float, bool, OutputMetadataPlaceholder,
                              ExecutorInputPlaceholder)):
                    pass
                elif isinstance(arg, list):
                    for arg2 in arg:
                        verify_arg(arg2)
                elif isinstance(
                        arg,
                    (InputUriPlaceholder, InputValuePlaceholder,
                     InputPathPlaceholder, IsPresentPlaceholder,
                     InputMetadataPlaceholder, InputOutputPortNamePlaceholder)):
                    if arg.input_name not in self._inputs_dict:
                        raise TypeError(
                            'Argument "{}" references non-existing input.'
                            .format(arg))
                elif isinstance(arg,
                                (OutputUriPlaceholder, OutputPathPlaceholder)):
                    if arg.output_name not in self._outputs_dict:
                        raise TypeError(
                            'Argument "{}" references non-existing output.'
                            .format(arg))
                elif isinstance(arg, ConcatPlaceholder):
                    for arg2 in arg.items:
                        verify_arg(arg2)
                elif isinstance(arg, IfPlaceholder):
                    verify_arg(arg.if_structure.condition)
                    verify_arg(arg.if_structure.then_value)
                    verify_arg(arg.if_structure.else_value)
                else:
                    raise TypeError('Unexpected argument "{}"'.format(arg))

            verify_arg(container.command)
            verify_arg(container.args)

        if isinstance(self.implementation, GraphImplementation):
            graph = self.implementation.graph

            if graph.output_values is not None:
                for output_name, argument in graph.output_values.items():
                    if output_name not in self._outputs_dict:
                        raise TypeError(
                            'Graph output argument entry "{}" references non-existing output.'
                            .format({output_name: argument}))

            if graph.tasks is not None:
                for task in graph.tasks.values():
                    if task.arguments is not None:
                        for argument in task.arguments.values():
                            if isinstance(
                                    argument, GraphInputArgument
                            ) and argument.graph_input.input_name not in self._inputs_dict:
                                raise TypeError(
                                    'Argument "{}" references non-existing input.'
                                    .format(argument))

    def save(self, file_path: str):
        """Saves the component definition to file.

        It can be shared online and later loaded using the
        load_component function.
        """
        from ._yaml_utils import dump_yaml
        component_yaml = dump_yaml(self.to_dict())
        with open(file_path, 'w') as f:
            f.write(component_yaml)


class ComponentReference(ModelBase):
    """Component reference.

    Contains information that can be used to locate and load a component
    by name, digest or URL
    """

    def __init__(
        self,
        name: Optional[str] = None,
        digest: Optional[str] = None,
        tag: Optional[str] = None,
        url: Optional[str] = None,
        spec: Optional[ComponentSpec] = None,
    ):
        super().__init__(locals())
        self._post_init()

    def _post_init(self) -> None:
        if not any([self.name, self.digest, self.tag, self.url, self.spec]):
            raise TypeError('Need at least one argument.')


class GraphInputReference(ModelBase):
    """References the input of the graph (the scope is a single graph)."""
    _serialized_names = {
        'input_name': 'inputName',
    }

    def __init__(
            self,
            input_name: str,
            type:
        Optional[
            TypeSpecType] = None,  # Can be used to override the reference data type
    ):
        super().__init__(locals())

    def as_argument(self) -> 'GraphInputArgument':
        return GraphInputArgument(graph_input=self)

    def with_type(self, type_spec: TypeSpecType) -> 'GraphInputReference':
        return GraphInputReference(
            input_name=self.input_name,
            type=type_spec,
        )

    def without_type(self) -> 'GraphInputReference':
        return self.with_type(None)


class GraphInputArgument(ModelBase):
    """Represents the component argument value that comes from the graph
    component input."""
    _serialized_names = {
        'graph_input': 'graphInput',
    }

    def __init__(
        self,
        graph_input: GraphInputReference,
    ):
        super().__init__(locals())


class TaskOutputReference(ModelBase):
    """References the output of some task (the scope is a single graph)."""
    _serialized_names = {
        'task_id': 'taskId',
        'output_name': 'outputName',
    }

    def __init__(
            self,
            output_name: str,
            task_id:
        Optional[
            str] = None,  # Used for linking to the upstream task in serialized component file.
            task:
        Optional[
            'TaskSpec'] = None,  # Used for linking to the upstream task in runtime since Task does not have an ID until inserted into a graph.
            type:
        Optional[
            TypeSpecType] = None,  # Can be used to override the reference data type
    ):
        super().__init__(locals())
        if self.task_id is None and self.task is None:
            raise TypeError('task_id and task cannot be None at the same time.')

    def with_type(self, type_spec: TypeSpecType) -> 'TaskOutputReference':
        return TaskOutputReference(
            output_name=self.output_name,
            task_id=self.task_id,
            task=self.task,
            type=type_spec,
        )

    def without_type(self) -> 'TaskOutputReference':
        return self.with_type(None)


class TaskOutputArgument(ModelBase
                        ):  #Has additional constructor for convenience
    """Represents the component argument value that comes from the output of
    another task."""
    _serialized_names = {
        'task_output': 'taskOutput',
    }

    def __init__(
        self,
        task_output: TaskOutputReference,
    ):
        super().__init__(locals())

    @staticmethod
    def construct(
        task_id: str,
        output_name: str,
    ) -> 'TaskOutputArgument':
        return TaskOutputArgument(
            TaskOutputReference(
                task_id=task_id,
                output_name=output_name,
            ))

    def with_type(self, type_spec: TypeSpecType) -> 'TaskOutputArgument':
        return TaskOutputArgument(
            task_output=self.task_output.with_type(type_spec),)

    def without_type(self) -> 'TaskOutputArgument':
        return self.with_type(None)


ArgumentType = Union[PrimitiveTypes, GraphInputArgument, TaskOutputArgument]


class TwoOperands(ModelBase):

    def __init__(
        self,
        op1: ArgumentType,
        op2: ArgumentType,
    ):
        super().__init__(locals())


class BinaryPredicate(ModelBase):  #abstract base type

    def __init__(self, operands: TwoOperands):
        super().__init__(locals())


class EqualsPredicate(BinaryPredicate):
    """Represents the "equals" comparison predicate."""
    _serialized_names = {'operands': '=='}


class NotEqualsPredicate(BinaryPredicate):
    """Represents the "not equals" comparison predicate."""
    _serialized_names = {'operands': '!='}


class GreaterThanPredicate(BinaryPredicate):
    """Represents the "greater than" comparison predicate."""
    _serialized_names = {'operands': '>'}


class GreaterThanOrEqualPredicate(BinaryPredicate):
    """Represents the "greater than or equal" comparison predicate."""
    _serialized_names = {'operands': '>='}


class LessThenPredicate(BinaryPredicate):
    """Represents the "less than" comparison predicate."""
    _serialized_names = {'operands': '<'}


class LessThenOrEqualPredicate(BinaryPredicate):
    """Represents the "less than or equal" comparison predicate."""
    _serialized_names = {'operands': '<='}


PredicateType = Union[ArgumentType, EqualsPredicate, NotEqualsPredicate,
                      GreaterThanPredicate, GreaterThanOrEqualPredicate,
                      LessThenPredicate, LessThenOrEqualPredicate,
                      'NotPredicate', 'AndPredicate', 'OrPredicate',]


class TwoBooleanOperands(ModelBase):

    def __init__(
        self,
        op1: PredicateType,
        op2: PredicateType,
    ):
        super().__init__(locals())


class NotPredicate(ModelBase):
    """Represents the "not" logical operation."""
    _serialized_names = {'operand': 'not'}

    def __init__(self, operand: PredicateType):
        super().__init__(locals())


class AndPredicate(ModelBase):
    """Represents the "and" logical operation."""
    _serialized_names = {'operands': 'and'}

    def __init__(self, operands: TwoBooleanOperands):
        super().__init__(locals())


class OrPredicate(ModelBase):
    """Represents the "or" logical operation."""
    _serialized_names = {'operands': 'or'}

    def __init__(self, operands: TwoBooleanOperands):
        super().__init__(locals())


class RetryStrategySpec(ModelBase):
    _serialized_names = {
        'max_retries': 'maxRetries',
    }

    def __init__(
        self,
        max_retries: int,
    ):
        super().__init__(locals())


class CachingStrategySpec(ModelBase):
    _serialized_names = {
        'max_cache_staleness': 'maxCacheStaleness',
    }

    def __init__(
            self,
            max_cache_staleness: Optional[
                str] = None,  # RFC3339 compliant duration: P30DT1H22M3S
    ):
        super().__init__(locals())


class ExecutionOptionsSpec(ModelBase):
    _serialized_names = {
        'retry_strategy': 'retryStrategy',
        'caching_strategy': 'cachingStrategy',
    }

    def __init__(
        self,
        retry_strategy: Optional[RetryStrategySpec] = None,
        caching_strategy: Optional[CachingStrategySpec] = None,
    ):
        super().__init__(locals())


class TaskSpec(ModelBase):
    """Task specification.

    Task is a "configured" component - a component supplied with arguments and other applied configuration changes.
    """
    _serialized_names = {
        'component_ref': 'componentRef',
        'is_enabled': 'isEnabled',
        'execution_options': 'executionOptions'
    }

    def __init__(
        self,
        component_ref: ComponentReference,
        arguments: Optional[Mapping[str, ArgumentType]] = None,
        is_enabled: Optional[PredicateType] = None,
        execution_options: Optional[ExecutionOptionsSpec] = None,
        annotations: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(locals())
        #TODO: If component_ref is resolved to component spec, then check that the arguments correspond to the inputs

    def _init_outputs(self):
        #Adding output references to the task
        if self.component_ref.spec is None:
            return
        task_outputs = OrderedDict()
        for output in self.component_ref.spec.outputs or []:
            task_output_ref = TaskOutputReference(
                output_name=output.name,
                task=self,
                type=output.
                type,  # TODO: Resolve type expressions. E.g. type: {TypeOf: Input 1}
            )
            task_output_arg = TaskOutputArgument(task_output=task_output_ref)
            task_outputs[output.name] = task_output_arg

        self.outputs = task_outputs
        if len(task_outputs) == 1:
            self.output = list(task_outputs.values())[0]


class GraphSpec(ModelBase):
    """Describes the graph component implementation.

    It represents a graph of component tasks connected to the upstream
    sources of data using the argument specifications. It also describes
    the sources of graph output values.
    """
    _serialized_names = {
        'output_values': 'outputValues',
    }

    def __init__(
        self,
        tasks: Mapping[str, TaskSpec],
        output_values: Mapping[str, ArgumentType] = None,
    ):
        super().__init__(locals())
        self._post_init()

    def _post_init(self):
        #Checking task output references and preparing the dependency table
        task_dependencies = {}
        for task_id, task in self.tasks.items():
            dependencies = set()
            task_dependencies[task_id] = dependencies
            if task.arguments is not None:
                for argument in task.arguments.values():
                    if isinstance(argument, TaskOutputArgument):
                        dependencies.add(argument.task_output.task_id)
                        if argument.task_output.task_id not in self.tasks:
                            raise TypeError(
                                'Argument "{}" references non-existing task.'
                                .format(argument))

        #Topologically sorting tasks to detect cycles
        task_dependents = {k: set() for k in task_dependencies.keys()}
        for task_id, dependencies in task_dependencies.items():
            for dependency in dependencies:
                task_dependents[dependency].add(task_id)
        task_number_of_remaining_dependencies = {
            k: len(v) for k, v in task_dependencies.items()
        }
        sorted_tasks = OrderedDict()

        def process_task(task_id):
            if task_number_of_remaining_dependencies[
                    task_id] == 0 and task_id not in sorted_tasks:
                sorted_tasks[task_id] = self.tasks[task_id]
                for dependent_task in task_dependents[task_id]:
                    task_number_of_remaining_dependencies[
                        dependent_task] = task_number_of_remaining_dependencies[
                            dependent_task] - 1
                    process_task(dependent_task)

        for task_id in task_dependencies.keys():
            process_task(task_id)
        if len(sorted_tasks) != len(task_dependencies):
            tasks_with_unsatisfied_dependencies = {
                k: v
                for k, v in task_number_of_remaining_dependencies.items()
                if v > 0
            }
            task_wth_minimal_number_of_unsatisfied_dependencies = min(
                tasks_with_unsatisfied_dependencies.keys(),
                key=lambda task_id: tasks_with_unsatisfied_dependencies[task_id]
            )
            raise ValueError('Task "{}" has cyclical dependency.'.format(
                task_wth_minimal_number_of_unsatisfied_dependencies))

        self._toposorted_tasks = sorted_tasks


class GraphImplementation(ModelBase):
    """Represents the graph component implementation."""

    def __init__(
        self,
        graph: GraphSpec,
    ):
        super().__init__(locals())


class PipelineRunSpec(ModelBase):
    """The object that can be sent to the backend to start a new Run."""
    _serialized_names = {
        'root_task': 'rootTask',
        #'on_exit_task': 'onExitTask',
    }

    def __init__(
        self,
        root_task: TaskSpec,
        #on_exit_task: Optional[TaskSpec] = None,
    ):
        super().__init__(locals())
