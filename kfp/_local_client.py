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

import datetime
import json
import logging
import os
import re
import subprocess
import tempfile
import warnings
from collections import deque
from typing import Any, Callable, Dict, List, Mapping, Optional, Union, cast

from . import dsl
from .compiler.compiler import sanitize_k8s_name


class _Dag:
    """DAG stands for Direct Acyclic Graph.

    DAG here is used to decide the order to execute pipeline ops.

    For more information on DAG, please refer to `wiki <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
    """

    def __init__(self, nodes: List[str]) -> None:
        """

        Args::
          nodes: List of DAG nodes, each node is identified by an unique name.
        """
        self._graph = {node: [] for node in nodes}
        self._reverse_graph = {node: [] for node in nodes}

    @property
    def graph(self):
        return self._graph

    @property
    def reverse_graph(self):
        return self._reverse_graph

    def add_edge(self, edge_source: str, edge_target: str) -> None:
        """Add an edge between DAG nodes.

        Args::
          edge_source: the source node of the edge
          edge_target: the target node of the edge
        """
        self._graph[edge_source].append(edge_target)
        self._reverse_graph[edge_target].append(edge_source)

    def get_follows(self, source_node: str) -> List[str]:
        """Get all target nodes start from the specified source node.

        Args::
          source_node: the source node
        """
        return self._graph.get(source_node, [])

    def get_dependencies(self, target_node: str) -> List[str]:
        """Get all source nodes end with the specified target node.

        Args::
          target_node: the target node
        """
        return self._reverse_graph.get(target_node, [])

    def topological_sort(self) -> List[str]:
        """List DAG nodes in topological order."""

        in_degree = {node: 0 for node in self._graph.keys()}

        for i in self._graph:
            for j in self._graph[i]:
                in_degree[j] += 1

        queue = deque()
        for node, degree in in_degree.items():
            if degree == 0:
                queue.append(node)

        sorted_nodes = []

        while queue:
            u = queue.popleft()
            sorted_nodes.append(u)

            for node in self._graph[u]:
                in_degree[node] -= 1

                if in_degree[node] == 0:
                    queue.append(node)

        return sorted_nodes


def _extract_pipeline_param(param: str) -> dsl.PipelineParam:
    """Extract PipelineParam from string."""
    matches = re.findall(r"{{pipelineparam:op=([\w\s_-]*);name=([\w\s_-]+)}}",
                         param)
    op_dependency_name = matches[0][0]
    output_file_name = matches[0][1]
    return dsl.PipelineParam(output_file_name, op_dependency_name)


def _get_op(ops: List[dsl.ContainerOp],
            op_name: str) -> Union[dsl.ContainerOp, None]:
    """Get the first op with specified op name."""
    return next(filter(lambda op: op.name == op_name, ops), None)


def _get_subgroup(groups: List[dsl.OpsGroup],
                  group_name: str) -> Union[dsl.OpsGroup, None]:
    """Get the first OpsGroup with specified group name."""
    return next(filter(lambda g: g.name == group_name, groups), None)


class LocalClient:

    class ExecutionMode:
        """Configuration to decide whether the client executes a component in
        docker or in local process."""

        DOCKER = "docker"
        LOCAL = "local"

        def __init__(
            self,
            mode: str = DOCKER,
            images_to_exclude: List[str] = [],
            ops_to_exclude: List[str] = [],
            docker_options: List[str] = [],
        ) -> None:
            """Constructor.

            Args:
                mode: Default execution mode, default 'docker'
                images_to_exclude: If the image of op is in images_to_exclude, the op is
                    executed in the mode different from default_mode.
                ops_to_exclude: If the name of op is in ops_to_exclude, the op is
                    executed in the mode different from default_mode.
                docker_options: Docker options used in docker mode,
                    e.g. docker_options=["-e", "foo=bar"].
            """
            if mode not in [self.DOCKER, self.LOCAL]:
                raise Exception(
                    "Invalid execution mode, must be docker of local")
            self._mode = mode
            self._images_to_exclude = images_to_exclude
            self._ops_to_exclude = ops_to_exclude
            self._docker_options = docker_options

        @property
        def mode(self) -> str:
            return self._mode

        @property
        def images_to_exclude(self) -> List[str]:
            return self._images_to_exclude

        @property
        def ops_to_exclude(self) -> List[str]:
            return self._ops_to_exclude

        @property
        def docker_options(self) -> List[str]:
            return self._docker_options

    def __init__(self, pipeline_root: Optional[str] = None) -> None:
        """Construct the instance of LocalClient.

        Args：
            pipeline_root: The root directory where the output artifact of component
              will be saved.
        """
        warnings.warn(
            'LocalClient is an Alpha[1] feature. It may be deprecated in the future.\n'
            '[1] https://github.com/kubeflow/pipelines/blob/master/docs/release/feature-stages.md#alpha',
            category=FutureWarning,
        )

        pipeline_root = pipeline_root or tempfile.tempdir
        self._pipeline_root = pipeline_root

    def _find_base_group(self, groups: List[dsl.OpsGroup],
                         op_name: str) -> Union[dsl.OpsGroup, None]:
        """Find the base group of op in candidate group list."""
        if groups is None or len(groups) == 0:
            return None
        for group in groups:
            if _get_op(group.ops, op_name):
                return group
            else:
                _parent_group = self._find_base_group(group.groups, op_name)
                if _parent_group:
                    return group

        return None

    def _create_group_dag(self, pipeline_dag: _Dag,
                          group: dsl.OpsGroup) -> _Dag:
        """Create DAG within current group, it's a DAG of direct ops and direct
        subgroups.

        Each node of the DAG is either an op or a subgroup. For each
        node in current group, if one of its DAG follows is also an op
        in current group, add an edge to this follow op, otherwise, if
        this follow belongs to subgroups, add an edge to its subgroup.
        If this node has dependency from subgroups, then add an edge
        from this subgroup to current node.
        """
        group_dag = _Dag([op.name for op in group.ops] +
                         [g.name for g in group.groups])

        for op in group.ops:
            for follow in pipeline_dag.get_follows(op.name):
                if _get_op(group.ops, follow) is not None:
                    # add edge between direct ops
                    group_dag.add_edge(op.name, follow)
                else:
                    _base_group = self._find_base_group(group.groups, follow)
                    if _base_group:
                        # add edge to direct subgroup
                        group_dag.add_edge(op.name, _base_group.name)

            for dependency in pipeline_dag.get_dependencies(op.name):
                if _get_op(group.ops, dependency) is None:
                    _base_group = self._find_base_group(group.groups,
                                                        dependency)
                    if _base_group:
                        # add edge from direct subgroup
                        group_dag.add_edge(_base_group.name, op.name)

        return group_dag

    def _create_op_dag(self, p: dsl.Pipeline) -> _Dag:
        """Create the DAG of the pipeline ops."""
        dag = _Dag(p.ops.keys())

        for op in p.ops.values():
            # dependencies defined by inputs
            for input_value in op.inputs:
                if isinstance(input_value, dsl.PipelineParam):
                    input_param = _extract_pipeline_param(input_value.pattern)
                    if input_param.op_name:
                        dag.add_edge(input_param.op_name, op.name)
                    else:
                        logging.debug("%s depend on pipeline param", op.name)

            # explicit dependencies of current op
            for dependent in op.dependent_names:
                dag.add_edge(dependent, op.name)
        return dag

    def _make_output_file_path_unique(self, run_name: str, op_name: str,
                                      output_file: str) -> str:
        """Alter the file path of output artifact to make sure it's unique in
        local runner.

        kfp compiler will bound a tmp file for each component output,
        which is unique in kfp runtime, but not unique in local runner.
        We alter the file path of the name of current run and op, to
        make it unique in local runner.
        """
        if not output_file.startswith("/tmp/"):
            return output_file
        return f'{self._pipeline_root}/{run_name}/{op_name.lower()}/{output_file[len("/tmp/"):]}'

    def _get_output_file_path(
        self,
        run_name: str,
        pipeline: dsl.Pipeline,
        op_name: str,
        output_name: str = None,
    ) -> str:
        """Get the file path of component output."""

        op_dependency = pipeline.ops[op_name]
        if output_name is None and len(op_dependency.file_outputs) == 1:
            output_name = next(iter(op_dependency.file_outputs.keys()))
        output_file = op_dependency.file_outputs[output_name]
        unique_output_file = self._make_output_file_path_unique(
            run_name, op_name, output_file)
        return unique_output_file

    def _generate_cmd_for_subprocess_execution(
        self,
        run_name: str,
        pipeline: dsl.Pipeline,
        op: dsl.ContainerOp,
        stack: Dict[str, Any],
    ) -> List[str]:
        """Generate shell command to run the op locally."""
        cmd = op.command + op.arguments

        # In debug mode, for `python -c cmd` format command, pydev will insert code before
        # `cmd`, but there is no newline at the end of the inserted code, which will cause
        # syntax error, so we add newline before `cmd`.
        for i in range(len(cmd)):
            if cmd[i] == "-c":
                cmd[i + 1] = "\n" + cmd[i + 1]

        for index, cmd_item in enumerate(cmd):
            if cmd_item in stack:  # Argument is LoopArguments item
                cmd[index] = str(stack[cmd_item])
            elif cmd_item in op.file_outputs.values(
            ):  # Argument is output file
                output_name = next(
                    filter(lambda item: item[1] == cmd_item,
                           op.file_outputs.items()))[0]
                output_param = op.outputs[output_name]
                output_file = cmd_item
                output_file = self._make_output_file_path_unique(
                    run_name, output_param.op_name, output_file)

                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                cmd[index] = output_file
            elif (cmd_item in op.input_artifact_paths.values()
                 ):  # Argument is input artifact file
                input_name = next(
                    filter(
                        lambda item: item[1] == cmd_item,
                        op.input_artifact_paths.items(),
                    ))[0]
                input_param_pattern = op.artifact_arguments[input_name]
                pipeline_param = _extract_pipeline_param(input_param_pattern)
                input_file = self._get_output_file_path(run_name, pipeline,
                                                        pipeline_param.op_name,
                                                        pipeline_param.name)

                cmd[index] = input_file

        return cmd

    def _generate_cmd_for_docker_execution(
        self,
        run_name: str,
        pipeline: dsl.Pipeline,
        op: dsl.ContainerOp,
        stack: Dict[str, Any],
        docker_options: List[str] = []
    ) -> List[str]:
        """Generate the command to run the op in docker locally."""
        cmd = self._generate_cmd_for_subprocess_execution(
            run_name, pipeline, op, stack)

        docker_cmd = [
            "docker",
            "run",
            *docker_options,
            "-v",
            "{pipeline_root}:{pipeline_root}".format(
                pipeline_root=self._pipeline_root),
            op.image,
        ] + cmd
        return docker_cmd

    def _run_group_dag(
        self,
        run_name: str,
        pipeline: dsl.Pipeline,
        pipeline_dag: _Dag,
        current_group: dsl.OpsGroup,
        stack: Dict[str, Any],
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run ops in current group in topological order.

        Args:
            pipeline: kfp.dsl.Pipeline
            pipeline_dag: DAG of pipeline ops
            current_group: current ops group
            stack: stack to trace `LoopArguments`
            execution_mode: Configuration to decide whether the client executes
                component in docker or in local process.
        Returns:
            True if succeed to run the group dag.
        """
        group_dag = self._create_group_dag(pipeline_dag, current_group)

        for node in group_dag.topological_sort():
            subgroup = _get_subgroup(current_group.groups, node)
            if subgroup is not None:  # Node of DAG is subgroup
                success = self._run_group(run_name, pipeline, pipeline_dag, subgroup,
                                stack, execution_mode)
                if not success:
                    return False
            else:  # Node of DAG is op
                op = _get_op(current_group.ops, node)

                execution_mode = (
                    execution_mode
                    if execution_mode else LocalClient.ExecutionMode())
                can_run_locally = execution_mode.mode == LocalClient.ExecutionMode.LOCAL
                exclude = (
                    op.image in execution_mode.images_to_exclude or
                    op.name in execution_mode.ops_to_exclude)
                if exclude:
                    can_run_locally = not can_run_locally

                if can_run_locally:
                    cmd = self._generate_cmd_for_subprocess_execution(
                        run_name, pipeline, op, stack)
                else:
                    cmd = self._generate_cmd_for_docker_execution(
                        run_name, pipeline, op, stack, execution_mode.docker_options)
                process = subprocess.Popen(
                    cmd,
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                # TODO support async process
                logging.info("start task：%s", op.name)
                stdout, stderr = process.communicate()
                if stdout:
                    logging.info(stdout)
                if stderr:
                    logging.error(stderr)
                if process.returncode != 0:
                    logging.error(cmd)
                    return False

    def _run_group(
        self,
        run_name: str,
        pipeline: dsl.Pipeline,
        pipeline_dag: _Dag,
        current_group: dsl.OpsGroup,
        stack: Dict[str, Any],
        execution_mode: ExecutionMode,
    ) -> bool:
        """Run all ops in current group.

        Args:
            run_name: str, the name of this run, can be used to query the run result
            pipeline: kfp.dsl.Pipeline
            pipeline_dag: DAG of pipeline ops
            current_group: current ops group
            stack: stack to trace `LoopArguments`
            execution_mode: Configuration to decide whether the client executes
                component in docker or in local process.
        Returns:
            True if succeed to run the group.
        """
        if current_group.type == dsl.ParallelFor.TYPE_NAME:
            current_group = cast(dsl.ParallelFor, current_group)

            if current_group.items_is_pipeline_param:
                _loop_args = current_group.loop_args
                _param_name = _loop_args.name[:-len(_loop_args
                                                    .LOOP_ITEM_NAME_BASE) - 1]

                _op_dependency = pipeline.ops[_loop_args.op_name]
                _list_file = _op_dependency.file_outputs[_param_name]
                _altered_list_file = self._make_output_file_path_unique(
                    run_name, _loop_args.op_name, _list_file)
                with open(_altered_list_file, "r") as f:
                    _param_values = json.load(f)
                for index, _param_value in enumerate(_param_values):
                    if isinstance(_param_values, (dict, list)):
                        _param_value = json.dumps(_param_value)
                    stack[_loop_args.pattern] = _param_value
                    loop_run_name = "{run_name}/{loop_index}".format(
                        run_name=run_name, loop_index=index)
                    success = self._run_group_dag(
                        loop_run_name,
                        pipeline,
                        pipeline_dag,
                        current_group,
                        stack,
                        execution_mode,
                    )
                    del stack[_loop_args.pattern]
                    if not success:
                        return False
                return True
            else:
                raise Exception("Not implemented")
        else:
            return self._run_group_dag(run_name, pipeline, pipeline_dag, current_group,
                                stack, execution_mode)

    def create_run_from_pipeline_func(
            self,
            pipeline_func: Callable,
            arguments: Mapping[str, str],
            execution_mode: ExecutionMode = ExecutionMode(),
    ):
        """Runs a pipeline locally, either using Docker or in a local process.

        Parameters:
          pipeline_func: pipeline function
          arguments: Arguments to the pipeline function provided as a dict, reference
              to `kfp.client.create_run_from_pipeline_func`
          execution_mode: Configuration to decide whether the client executes component
              in docker or in local process.
        """

        class RunPipelineResult:

            def __init__(self, client: LocalClient, pipeline: dsl.Pipeline,
                         run_id: str, success: bool):
                self._client = client
                self._pipeline = pipeline
                self.run_id = run_id
                self._success = success

            def get_output_file(self, op_name: str, output: str = None):
                return self._client._get_output_file_path(
                    self.run_id, self._pipeline, op_name, output)

            def success(self) -> bool:
                return self._success

            def __repr__(self):
                return "RunPipelineResult(run_id={})".format(self.run_id)

        pipeline_name = sanitize_k8s_name(
            getattr(pipeline_func, "_component_human_name", None) or
            pipeline_func.__name__)
        with dsl.Pipeline(pipeline_name) as pipeline:
            pipeline_func(**arguments)

        run_version = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        run_name = pipeline.name.replace(" ", "_").lower() + "_" + run_version

        pipeline_dag = self._create_op_dag(pipeline)
        success = self._run_group(run_name, pipeline, pipeline_dag, pipeline.groups[0],
                        {}, execution_mode)

        return RunPipelineResult(self, pipeline, run_name, success=success)
