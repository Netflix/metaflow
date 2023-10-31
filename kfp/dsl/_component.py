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

import inspect
from deprecated.sphinx import deprecated
from ._pipeline_param import PipelineParam
from .types import check_types, InconsistentTypeException
from ._ops_group import Graph
import kfp


@deprecated(
    version='0.2.6',
    reason='This decorator does not seem to be used, so we deprecate it. '
    'If you need this decorator, please create an issue at '
    'https://github.com/kubeflow/pipelines/issues',
)
def python_component(name,
                     description=None,
                     base_image=None,
                     target_component_file: str = None):
    """Decorator for Python component functions.

    This decorator adds the metadata to the function object itself.

    Args:
      name: Human-readable name of the component
      description: Optional. Description of the component
      base_image: Optional. Docker container image to use as the base of the
        component. Needs to have Python 3.5+ installed.
      target_component_file: Optional. Local file to store the component
        definition. The file can then be used for sharing.

    Returns:
      The same function (with some metadata fields set).

    Example:
      ::

        @dsl.python_component(
          name='my awesome component',
          description='Come, Let's play',
          base_image='tensorflow/tensorflow:1.11.0-py3',
        )
        def my_component(a: str, b: int) -> str:
          ...
    """

    def _python_component(func):
        func._component_human_name = name
        if description:
            func._component_description = description
        if base_image:
            func._component_base_image = base_image
        if target_component_file:
            func._component_target_component_file = target_component_file
        return func

    return _python_component


def component(func):
    """Decorator for component functions that returns a ContainerOp.

    This is useful to enable type checking in the DSL compiler.

    Example:
      ::

        @dsl.component
        def foobar(model: TFModel(), step: MLStep()):
          return dsl.ContainerOp()
    """
    from functools import wraps

    @wraps(func)
    def _component(*args, **kargs):
        from ..components._python_op import _extract_component_interface
        component_meta = _extract_component_interface(func)
        if kfp.TYPE_CHECK:
            arg_index = 0
            for arg in args:
                if isinstance(arg, PipelineParam) and not check_types(
                        arg.param_type, component_meta.inputs[arg_index].type):
                    raise InconsistentTypeException(
                        'Component "' + component_meta.name +
                        '" is expecting ' +
                        component_meta.inputs[arg_index].name + ' to be type(' +
                        str(component_meta.inputs[arg_index].type) +
                        '), but the passed argument is type(' +
                        str(arg.param_type) + ')')
                arg_index += 1
            if kargs is not None:
                for key in kargs:
                    if isinstance(kargs[key], PipelineParam):
                        for input_spec in component_meta.inputs:
                            if input_spec.name == key and not check_types(
                                    kargs[key].param_type, input_spec.type):
                                raise InconsistentTypeException(
                                    'Component "' + component_meta.name +
                                    '" is expecting ' + input_spec.name +
                                    ' to be type(' + str(input_spec.type) +
                                    '), but the passed argument is type(' +
                                    str(kargs[key].param_type) + ')')

        container_op = func(*args, **kargs)
        container_op._set_metadata(component_meta)
        return container_op

    return _component


#TODO: combine the component and graph_component decorators into one
def graph_component(func):
    """Decorator for graph component functions.

    This decorator returns an ops_group.

    Example:
      ::

        # Warning: caching is tricky when recursion is involved. Please be careful
        # and set proper max_cache_staleness in case of infinite loop.
        import kfp.dsl as dsl
        @dsl.graph_component
        def flip_component(flip_result):
          print_flip = PrintOp(flip_result)
          flipA = FlipCoinOp().after(print_flip)
          flipA.execution_options.caching_strategy.max_cache_staleness = "P0D"
          with dsl.Condition(flipA.output == 'heads'):
            flip_component(flipA.output)
          return {'flip_result': flipA.output}
    """
    from functools import wraps

    @wraps(func)
    def _graph_component(*args, **kargs):
        # We need to make sure that the arguments are correctly mapped to inputs
        # regardless of the passing order
        signature = inspect.signature(func)
        bound_arguments = signature.bind(*args, **kargs)
        graph_ops_group = Graph(func.__name__)
        graph_ops_group.inputs = list(bound_arguments.arguments.values())
        graph_ops_group.arguments = bound_arguments.arguments
        for input in graph_ops_group.inputs:
            if not isinstance(input, PipelineParam):
                raise ValueError('arguments to ' + func.__name__ +
                                 ' should be PipelineParams.')

        # Entering the Graph Context
        with graph_ops_group:
            # Call the function
            if not graph_ops_group.recursive_ref:
                func(*args, **kargs)

        return graph_ops_group

    return _graph_component
