import os
import time

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

from .dynamo_db_client import DynamoDbClient


class StepFunctionsInternalDecorator(StepDecorator):
    name = "step_functions_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        meta = {}
        meta["aws-step-functions-execution"] = os.environ["METAFLOW_RUN_ID"]
        meta["aws-step-functions-state-machine"] = os.environ["SFN_STATE_MACHINE"]
        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return

        # For `foreach`s, we need to dump the cardinality of the fanout
        # into AWS DynamoDb so that AWS Step Functions can properly configure
        # the Map job, in the absence of any better message passing feature
        # between the states.
        if graph[step_name].type == "foreach":
            # Since we can't generate the full path spec within AWS Step
            # Function DynamoDb Get task, we will just key by task id for now.
            # Also, we can afford to set the ttl only once for the key in AWS
            # DynamoDB here. AWS Step Functions can execute for up to a year
            # and execution history is available for 90 days after the
            # execution.
            self._save_foreach_cardinality(
                os.environ["AWS_BATCH_JOB_ID"], flow._foreach_num_splits, self._ttl()
            )
        # The parent task ids need to be available in a foreach join so that
        # we can construct the input path. Unfortunately, while AWS Step
        # Function provides access to an array of parent task ids, we can't
        # make use of them since AWS Batch job spec only accepts strings. We
        # instead write the task ids from the parent task to DynamoDb and read
        # it back in the foreach join
        elif graph[step_name].is_inside_foreach and any(
            graph[n].type == "join"
            and graph[graph[n].split_parents[-1]].type == "foreach"
            for n in graph[step_name].out_funcs
        ):
            self._save_parent_task_id_for_foreach_join(
                os.environ["METAFLOW_SPLIT_PARENT_TASK_ID_FOR_FOREACH_JOIN"],
                os.environ["AWS_BATCH_JOB_ID"],
            )

    def _save_foreach_cardinality(
        self, foreach_split_task_id, for_each_cardinality, ttl
    ):
        DynamoDbClient().save_foreach_cardinality(
            foreach_split_task_id, for_each_cardinality, ttl
        )

    def _save_parent_task_id_for_foreach_join(
        self, foreach_split_task_id, foreach_join_parent_task_id
    ):
        DynamoDbClient().save_parent_task_id_for_foreach_join(
            foreach_split_task_id, foreach_join_parent_task_id
        )

    def _ttl(self):
        # Default is 1 year.
        delta = 366 * 24 * 60 * 60
        delta = int(os.environ.get("METAFLOW_SFN_WORKFLOW_TIMEOUT", delta))
        # Add 90 days since AWS Step Functions maintains execution history for
        # that long.
        return delta + (90 * 24 * 60 * 60) + int(time.time())
