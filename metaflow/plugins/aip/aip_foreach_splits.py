import json
import os
from typing import Callable, Dict, List

from metaflow import S3, FlowSpec, current
from metaflow.datastore import FlowDataStore, S3Storage
from metaflow.graph import DAGNode, FlowGraph
from metaflow.plugins.aip.aip_constants import (
    AIP_METAFLOW_FOREACH_SPLITS_PATH,
    PASSED_IN_SPLIT_INDEXES_ENV_NAME,
    SPLIT_INDEX_SEPARATOR,
)


def graph_to_task_ids(graph: FlowGraph) -> Dict[str, str]:
    """
    Traverses the graph DAG in level order assigning each node
    a monotonically incrementing task_id.
    Args:
        graph: graph of Metaflow

    Returns: node.name, or step_name -> task_id
    """
    step_to_task_id: Dict[str, str] = {}
    steps_queue = ["start"]  # Queue to process the DAG in level order
    seen_steps = {"start"}  # Set of seen steps
    task_id = 0
    while len(steps_queue) > 0:
        current_step = steps_queue.pop(0)
        node = graph.nodes[current_step]
        task_id += 1
        step_to_task_id[current_step] = f"{node.name}-{task_id}"

        for step in node.out_funcs:
            if step not in seen_steps:
                steps_queue.append(step)
                seen_steps.add(step)

    return step_to_task_id


class AIPForEachSplits(object):
    """
    passed_in_split_indexes is a string of foreach split_index ordinals.
    A nested foreach appends the new split index ordinal with a "_" separator.
    Example:
        0_1 -> 0th index of outer foreach and 1th index of inner foreach
        1_0 -> 1th index of outer foreach and 0th index of inner foreach

    Please see metaflow_nested_foreach.ipynb for more.
    """

    def __init__(
        self,
        graph: FlowGraph,
        step_name: str,
        run_id: str,
        flow_datastore: FlowDataStore,
        logger: Callable,
    ):
        self.graph = graph
        self.step_name = step_name
        self.run_id = run_id
        self.logger = logger
        self.node = graph[step_name]
        self.flow_datastore = flow_datastore
        self.step_to_task_id: Dict[str, str] = graph_to_task_ids(graph)
        self.s3 = S3()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        try:
            self.s3.close()
        except:
            pass

    def build_foreach_splits(self, flow: FlowSpec) -> Dict:
        """
        Returns a dict with
        {
            task_id: The task_id of this step
            foreach_splits: <PASSED_IN_SPLIT_INDEXES>_index where index is ordinal of the split
        }
        """
        assert self.node.type == "foreach"
        passed_in_split_indexes = os.environ[PASSED_IN_SPLIT_INDEXES_ENV_NAME]

        # The splits are fed to aip.ParallelFor to downstream steps as
        # "passed_in_split_indexes" variable and become the step task_id
        # Example: 0_3_1
        foreach_splits = [
            f"{passed_in_split_indexes}{SPLIT_INDEX_SEPARATOR}{split_index}".strip(
                SPLIT_INDEX_SEPARATOR
            )
            for split_index in range(0, flow._foreach_num_splits)
        ]

        return {
            "foreach_splits": foreach_splits,
        }

    def get_foreach_splits(
        self,
        parent_context_step_name: str,
        current_node: DAGNode,
        passed_in_split_indexes: str,
    ) -> List[str]:
        """
        Used by compute_input_paths() to access the parent context saved in foreach_splits_path
        Only use on a foreach node type!

        Returns:
            Task context dict built by build_foreach_splits() that was saved to S3 by aip_decorator.
        """
        assert self.graph[parent_context_step_name].type == "foreach"

        context_node_task_id = self.get_parent_context_task_id(
            parent_context_step_name, current_node, passed_in_split_indexes
        )

        # datastore version of `input_context = json.loads(self.s3.get(foreach_splits_path).text)`
        foreach_splits_path = self._build_foreach_splits_prefix(
            parent_context_step_name, context_node_task_id
        )
        s3_datastore: S3Storage = self.flow_datastore._storage_impl
        with s3_datastore.load_bytes([foreach_splits_path]) as loaded:
            for _, local_file_path, _ in loaded:  # Expect only one file per split index
                with open(local_file_path, "r") as f:
                    input_context = json.load(f)

        return input_context["foreach_splits"]

    def get_parent_context_task_id(
        self,
        parent_context_step_name: str,
        current_node: DAGNode,
        passed_in_split_indexes: str,
    ) -> str:
        context_node_task_id = str(self.step_to_task_id[parent_context_step_name])
        if self.graph[parent_context_step_name].is_inside_foreach:
            if (
                self.graph[parent_context_step_name].type == "foreach"
                and parent_context_step_name
                == current_node.in_funcs[0]  # and is a direct parent
            ):
                # if the direct parent node is a foreach
                # and current_node.is_inside_foreach (we are in a foreach) then:
                #   it's context_node_task_id is passed_in_split_indexes
                #   minus the last split_index which is for the inner loop
                split_indices_but_last_one = passed_in_split_indexes.split(
                    SPLIT_INDEX_SEPARATOR
                )[:-1]
                context_split_indexes = SPLIT_INDEX_SEPARATOR.join(
                    split_indices_but_last_one
                )
            else:
                context_split_indexes = passed_in_split_indexes

            context_node_task_id = f"{context_node_task_id}.{context_split_indexes}"
        else:
            # not is_inside_foreach, hence context_node_task_id is None
            # and the foreach_splits_path doesn't have a file.
            pass

        return context_node_task_id

    def get_current_step_split_index(self, passed_in_split_indexes: str) -> str:
        if self.node.is_inside_foreach:
            # the index is the last appended split ordinal
            return passed_in_split_indexes.split(SPLIT_INDEX_SEPARATOR)[-1]
        else:
            return ""

    @staticmethod
    def save_foreach_splits_to_local_fs(foreach_splits: Dict):
        """
        Used by aip_decorator.py to save the context to disk.
        step_op_func opens this file to read out and return foreach_splits
        """
        # write: context_dict to local FS to return
        with open(AIP_METAFLOW_FOREACH_SPLITS_PATH, "w") as file:
            json.dump(foreach_splits, file)

    def upload_foreach_splits_to_flow_root(self, foreach_splits: Dict):
        # Only S3_datastore is supported for AIP plug-in.
        # Safely assume _storage_impl is of type S3Storage here
        s3_datastore: S3Storage = self.flow_datastore._storage_impl
        foreach_splits_path: str = self._build_foreach_splits_prefix(
            self.step_name, current.task_id
        )
        s3_datastore.save_bytes(
            path_and_bytes_iter=[
                (
                    foreach_splits_path,
                    json.dumps(foreach_splits),
                )
            ],
            overwrite=True,
            len_hint=1,
        )

    @staticmethod
    def get_step_task_id(task_id: str, passed_in_split_indexes: str) -> str:
        return f"{task_id}.{passed_in_split_indexes}".strip(".")

    def _build_foreach_splits_prefix(self, step_name: str, task_id: str) -> str:
        """For foreach splits generate file prefix used for datastore"""
        # Save to `<s3_ds_root>/<flow>/<run_id>/foreach_splits/{task_id}.{step_name}.json`
        #   S3Storage.datastore_root: `s3://<ds_root>`
        #   Key: `<flow>/<run_id>/foreach_splits/{task_id}.{step_name}.json`
        return os.path.join(
            self.flow_datastore.flow_name,
            self.run_id,
            "foreach_splits",
            f"{task_id}.{step_name}.json",
        )
