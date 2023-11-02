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
"""Base class for KFP components."""

import abc

from kfp.v2.components.experimental import structures
from kfp.v2.components.experimental import pipeline_task


class BaseComponent(metaclass=abc.ABCMeta):
    """Base class for a component.

    Attributes:
      name: The name of the component.
      component_spec: The component definition.
    """

    def __init__(self, component_spec: structures.ComponentSpec):
        """Init function for BaseComponent.

        Args:
          component_spec: The component definition.
        """
        self.component_spec = component_spec
        self.name = component_spec.name

        self._component_inputs = set((self.component_spec.inputs or {}).keys())

    def __call__(self, *args, **kwargs) -> pipeline_task.PipelineTask:
        """Creates a PipelineTask object.

        The arguments are generated on the fly based on component input
        definitions.
        """
        task_inputs = {}

        if len(args) > 0:
            raise TypeError(
                'Components must be instantiated using keyword arguments. Positional '
                f'parameters are not allowed (found {len(args)} such parameters for '
                f'component "{self.name}").')

        for k, v in kwargs.items():
            if k not in self._component_inputs:
                raise TypeError(
                    f'{self.name}() got an unexpected keyword argument "{k}".')

            if k in task_inputs:
                raise TypeError(
                    f'{self.name}() got multiple values for argument "{k}".')
            task_inputs[k] = v

        # Fill in default value if there was no user provided value
        for name, input_spec in (self.component_spec.inputs or {}).items():
            if input_spec.default is not None and name not in task_inputs:
                task_inputs[name] = input_spec.default

        missing_arguments = [
            name for name in (self.component_spec.inputs or {})
            if name not in task_inputs
        ]
        if missing_arguments:
            argument_or_arguments = 'argument' if len(
                missing_arguments) == 1 else 'arguments'
            arguments = ','.join(missing_arguments)

            raise TypeError(
                f'{self.name}() missing {len(missing_arguments)} required positional '
                f'{argument_or_arguments}: {arguments}.')

        return pipeline_task.create_pipeline_task(
            component_spec=self.component_spec,
            arguments=task_inputs,
        )

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Executes the component given the required inputs.

        Subclasses of BaseComponent must override this abstract method
        in order to be instantiated. For Python function-based
        component, the implementation of this method could be calling
        the function. For "Bring your own container" component, the
        implementation of this method could be `docker run`.
        """
        pass
