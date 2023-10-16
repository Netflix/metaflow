import os
from typing import Callable

from metaflow.graph import DAGNode, FlowGraph
from metaflow.plugins.aip.aip_constants import (
    INPUT_PATHS_ENV_NAME,
    PASSED_IN_SPLIT_INDEXES_ENV_NAME,
    SPLIT_INDEX_ENV_NAME,
    STEP_ENVIRONMENT_VARIABLES,
    TASK_ID_ENV_NAME,
)
from metaflow.plugins.aip.aip_foreach_splits import AIPForEachSplits


def save_step_environment_variables(
    flow_datastore,
    graph: FlowGraph,
    run_id: str,
    step_name: str,
    passed_in_split_indexes: str,
    task_id: str,
    logger: Callable,
):
    """
    Persists a STEP_ENVIRONMENT_VARIABLES file with following variables:
    - TASK_ID_ENV_NAME
    - SPLIT_INDEX_ENV_NAME
    - INPUT_PATHS_ENV_NAME

    These will be loaded in the step_op_func bash command to be used
    by Metaflow step command line arguments.
    """
    with AIPForEachSplits(
        graph, step_name, run_id, flow_datastore, logger
    ) as split_contexts:
        environment_exports = {
            # The step task_id
            TASK_ID_ENV_NAME: AIPForEachSplits.get_step_task_id(
                task_id, passed_in_split_indexes
            ),
            # The current split index if this node is_inside_foreach
            SPLIT_INDEX_ENV_NAME: split_contexts.get_current_step_split_index(
                passed_in_split_indexes
            ),
            # PASSED_IN_SPLIT_INDEXES is used by build_foreach_splits in aip_decorator to:
            #   - pass along parent split_indexes to nested foreaches
            #   - get a parent foreach (in nested foreach case) split_index path
            #   - compute the task_id
            #   - get current foreach split index
            # see: nested_parallelfor.ipynb for a visual description
            PASSED_IN_SPLIT_INDEXES_ENV_NAME: passed_in_split_indexes,  # for aip_decorator.py
        }

        if len(passed_in_split_indexes) > 0:
            logger(passed_in_split_indexes, head="PASSED_IN_SPLIT_INDEXES: ")

        environment_exports[INPUT_PATHS_ENV_NAME] = _compute_input_paths(
            graph, run_id, step_name, split_contexts, passed_in_split_indexes
        )

        with open(STEP_ENVIRONMENT_VARIABLES, "w") as file:
            for key, value in environment_exports.items():
                file.write(f"export {key}={value}\n")
        os.chmod(STEP_ENVIRONMENT_VARIABLES, 644)


def _compute_input_paths(
    graph: FlowGraph,
    run_id: str,
    step_name: str,
    split_contexts: AIPForEachSplits,
    passed_in_split_indexes: str,
) -> str:
    """
    Computes and returns the Metaflow step input_paths.

    Access the appropriate parent_context (using split_contexts.get_foreach_splits())
    in S3 to get the parent context task_id and in the case of a foreach join
    the foreach_splits.

    For an illustration of how the input paths are created run
      ```
      $ export METAFLOW_DEBUG_SUBCOMMAND=1
      $ python 02-statistics/stats.py run
      ```
      And inspect the "--input-paths" parameters.

    A summary:
      non-join nodes: "run-id/parent-step/parent-task-id",
      branch-join node: "run-id/:p1/p1-task-id,p2/p2-task-id,..."
      foreach-split node:
          --input-paths 1/start/1
          --split-index 0 (use --split-index 1,2 and 3 to cover the remaining splits)
      foreach-join node:
          run-id/foreach-parent/:2,3,4,5
          (where 2,3,4,5 are the task_ids of the explore steps that were previously executed)
    """
    node: DAGNode = graph[step_name]

    # a closure
    def get_parent_context_task_id(parent_context_step_name: str) -> str:
        return split_contexts.get_parent_context_task_id(
            parent_context_step_name, node, passed_in_split_indexes
        )

    # Create input_paths
    input_paths = f"{run_id}"  # begins with run_id, filled in by step_op_func
    if node.type == "join":
        # load from s3 the context outs foreach
        if graph[node.split_parents[-1]].type == "foreach":
            parent_step_name = node.in_funcs[0]
            parent_task_id = str(split_contexts.step_to_task_id[parent_step_name])
            foreach_splits = split_contexts.get_foreach_splits(
                node.split_parents[-1], node, passed_in_split_indexes
            )
            parent_task_ids = [f"{parent_task_id}.{split}" for split in foreach_splits]
            input_paths += f"/{parent_step_name}/:{','.join(parent_task_ids)}"
        else:
            input_paths += "/:"
            for parent_step_name in node.in_funcs:
                parent_task_id = get_parent_context_task_id(parent_step_name)
                input_paths += f"{parent_step_name}/{parent_task_id},"
    else:
        if step_name != "start":
            parent_step_name = node.in_funcs[0]
            parent_task_id = get_parent_context_task_id(parent_step_name)
            input_paths += f"/{parent_step_name}/{parent_task_id}"
    return input_paths.strip(",")
