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
"""Definition for TasksGroup."""

import enum
from typing import Optional, Union

from kfp.v2.components.experimental import for_loop
from kfp.v2.components.experimental import pipeline
from kfp.v2.components.experimental import pipeline_channel
from kfp.v2.components.experimental import pipeline_task


class TasksGroupType(str, enum.Enum):
    """Types of TasksGroup."""
    PIPELINE = 'pipeline'
    CONDITION = 'condition'
    FOR_LOOP = 'for-loop'
    EXIT_HANDLER = 'exit-handler'


class TasksGroup:
    """Represents a logical group of tasks and groups of TasksGroups.

    This class is the base class for groups of tasks, such as tasks
    sharing an exit handler, a condition branch, or a loop. This class
    is not supposed to be used by pipeline authors. It is useful for
    implementing a compiler.

    Attributes:
        group_type: The type of the TasksGroup.
        tasks: A list of all PipelineTasks in this group.
        groups: A list of TasksGroups in this group.
        name: The optional user given name of the group.
        dependencies: A list of tasks or groups this group depends on.
    """

    def __init__(
        self,
        group_type: TasksGroupType,
        name: Optional[str] = None,
    ):
        """Create a new instance of TasksGroup.

        Args:
          group_type: The type of the group.
          name: Optional; the name of the group.
        """
        self.group_type = group_type
        self.tasks = list()
        self.groups = list()
        self.name = name
        self.dependencies = []

    def __enter__(self):
        if not pipeline.Pipeline.get_default_pipeline():
            raise ValueError('Default pipeline not defined.')

        self._make_name_unique()

        pipeline.Pipeline.get_default_pipeline().push_tasks_group(self)
        return self

    def __exit__(self, *unused_args):
        pipeline.Pipeline.get_default_pipeline().pop_tasks_group()

    def _make_name_unique(self):
        """Generates a unique TasksGroup name in the pipeline."""
        if not pipeline.Pipeline.get_default_pipeline():
            raise ValueError('Default pipeline not defined.')

        self.name = (
            self.group_type + '-' +
            ('' if self.name is None else self.name + '-') +
            pipeline.Pipeline.get_default_pipeline().get_next_group_id())
        self.name = self.name.replace('_', '-')

    def remove_task_recursive(self, task: pipeline_task.PipelineTask):
        """Removes a task from the group recursively."""
        if self.tasks and task in self.tasks:
            self.tasks.remove(task)
        for group in self.groups or []:
            group.remove_task_recursive(task)


class ExitHandler(TasksGroup):
    """Represents an exit handler that is invoked upon exiting a group of
    tasks.

    Example:
      ::

        exit_task = ExitComponent(...)
        with ExitHandler(exit_task):
            task1 = MyComponent1(...)
            task2 = MyComponent2(...)

    Attributes:
        exit_task: The exit handler task.
    """

    def __init__(
        self,
        exit_task: pipeline_task.PipelineTask,
        name: Optional[str] = None,
    ):
        """Initializes a Condition task group.

        Args:
            exit_task: An operator invoked at exiting a group of ops.
            name: Optional; the name of the exit handler group.

        Raises:
            ValueError: Raised if the exit_task is invalid.
        """
        super().__init__(group_type=TasksGroupType.EXIT_HANDLER, name=name)

        if exit_task.dependent_tasks:
            raise ValueError('exit_task cannot depend on any other tasks.')

        # Removing exit_task form any group
        pipeline.Pipeline.get_default_pipeline().remove_task_from_groups(
            exit_task)

        # Set is_exit_handler since the compiler might be using this attribute.
        exit_task.is_exit_handler = True

        self.exit_task = exit_task


class Condition(TasksGroup):
    """Represents an condition group with a condition.

    Example:
      ::

        with Condition(param1=='pizza', '[param1 is pizza]'):
            task1 = MyComponent1(...)
            task2 = MyComponent2(...)

    Attributes:
        condition: The condition expression.
    """

    def __init__(
        self,
        condition: pipeline_channel.ConditionOperator,
        name: Optional[str] = None,
    ):
        """Initializes a conditional task group.

        Args:
            condition: The condition expression.
            name: Optional; the name of the condition group.
        """
        super().__init__(group_type=TasksGroupType.CONDITION, name=name)
        self.condition = condition


class ParallelFor(TasksGroup):
    """Represents a parallel for loop over a static set of items.

    Example:
      ::

        with dsl.ParallelFor([{'a': 1, 'b': 10}, {'a': 2, 'b': 20}]) as item:
            task1 = MyComponent(..., item.a)
            task2 = MyComponent(..., item.b)

    In this case :code:`task1` would be executed twice, once with case
    :code:`args=['echo 1']` and once with case :code:`args=['echo 2']`::


    Attributes:
        loop_argument: The argument for each loop iteration.
        items_is_pipeline_channel: Whether the loop items is PipelineChannel
            instead of raw items.
    """

    def __init__(
        self,
        items: Union[for_loop.ItemList, pipeline_channel.PipelineChannel],
        name: Optional[str] = None,
    ):
        """Initializes a for loop task group.

        Args:
            items: The argument to loop over. It can be either a raw list or a
                pipeline channel.
            name: Optional; the name of the for loop group.
        """
        super().__init__(group_type=TasksGroupType.FOR_LOOP, name=name)

        if isinstance(items, pipeline_channel.PipelineChannel):
            self.loop_argument = for_loop.LoopArgument.from_pipeline_channel(
                items)
            self.items_is_pipeline_channel = True
        else:
            self.loop_argument = for_loop.LoopArgument.from_raw_items(
                raw_items=items,
                name_code=pipeline.Pipeline.get_default_pipeline()
                .get_next_group_id(),
            )
            self.items_is_pipeline_channel = False

    def __enter__(self) -> for_loop.LoopArgument:
        super().__enter__()
        return self.loop_argument
