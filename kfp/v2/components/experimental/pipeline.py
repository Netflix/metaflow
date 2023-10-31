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
"""Definition for Pipeline."""

from kfp.v2.components.experimental import pipeline_task
from kfp.v2.components.experimental import tasks_group
from kfp.v2.components import utils


class Pipeline:
    """A pipeline contains a list of tasks.

    This class is not supposed to be used by pipeline authors since pipeline
    authors can use pipeline functions (decorated with @pipeline) to reference
    their pipelines.
    This class is useful for implementing a compiler. For example, the compiler
    can use the following to get the pipeline object and its tasks:

    Example:
      ::

        with Pipeline() as p:
            pipeline_func(*args_list)

        traverse(p.tasks)

    Attributes:
        name:
        tasks:
        groups:
    """

    # _default_pipeline is set when the compiler runs "with Pipeline()"
    _default_pipeline = None

    @staticmethod
    def get_default_pipeline():
        """Gets the default pipeline."""
        return Pipeline._default_pipeline

    def __init__(self, name: str):
        """Creates a new instance of Pipeline.

        Args:
            name: The name of the pipeline.
        """
        self.name = name
        self.tasks = {}
        # Add the root group.
        self.groups = [
            tasks_group.TasksGroup(
                group_type=tasks_group.TasksGroupType.PIPELINE, name=name)
        ]
        self._group_id = 0

    def __enter__(self):

        if Pipeline._default_pipeline:
            raise Exception('Nested pipelines are not allowed.')

        Pipeline._default_pipeline = self

        def register_task_and_generate_id(task: pipeline_task.PipelineTask):
            return self.add_task(
                task=task,
                add_to_group=not getattr(task, 'is_exit_handler', False))

        self._old_register_task_handler = (
            pipeline_task.PipelineTask.register_task_handler)
        pipeline_task.PipelineTask.register_task_handler = (
            register_task_and_generate_id)
        return self

    def __exit__(self, *unused_args):

        Pipeline._default_pipeline = None
        pipeline_task.PipelineTask.register_task_handler = (
            self._old_register_task_handler)

    def add_task(
        self,
        task: pipeline_task.PipelineTask,
        add_to_group: bool,
    ) -> str:
        """Adds a new task.

        Args:
            task: A PipelineTask instance.
            add_to_group: Whether add the task into the current group. Expect
                True for all tasks expect for exit handler.

        Returns:
            A unique task name.
        """
        # Sanitizing the task name.
        # Technically this could be delayed to the compilation stage, but string
        # serialization of PipelineChannels make unsanitized names problematic.
        task_name = utils.maybe_rename_for_k8s(task.component_spec.name)
        #If there is an existing task with this name then generate a new name.
        task_name = utils.make_name_unique_by_adding_index(
            task_name, list(self.tasks.keys()), '-')
        if task_name == '':
            task_name = utils._make_name_unique_by_adding_index(
                'task', list(self.tasks.keys()), '-')

        self.tasks[task_name] = task
        if add_to_group:
            self.groups[-1].tasks.append(task)

        return task_name

    def push_tasks_group(self, group: 'tasks_group.TasksGroup'):
        """Pushes a TasksGroup into the stack.

        Args:
            group: A TasksGroup. Typically it is one of ExitHandler, Condition,
                and ParallelFor.
        """
        self.groups[-1].groups.append(group)
        self.groups.append(group)

    def pop_tasks_group(self):
        """Removes the current TasksGroup from the stack."""
        del self.groups[-1]

    def remove_task_from_groups(self, task: pipeline_task.PipelineTask):
        """Removes a task from the pipeline.

        This is useful for excluding exit handler from the pipeline.
        """
        for group in self.groups:
            group.remove_task_recursive(task)

    def get_next_group_id(self) -> str:
        """Gets the next id for a new group."""
        self._group_id += 1
        return str(self._group_id)
