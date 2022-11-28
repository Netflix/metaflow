import json
import os

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum


class ArgoWorkflowsInternalDecorator(StepDecorator):
    name = "argo_workflows_internal"

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
        self.task_id = task_id
        meta = {}
        meta["argo-workflow-template"] = os.environ["ARGO_WORKFLOW_TEMPLATE"]
        meta["argo-workflow-name"] = os.environ["ARGO_WORKFLOW_NAME"]
        meta["argo-workflow-namespace"] = os.environ["ARGO_WORKFLOW_NAMESPACE"]
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
        # into a file so that Argo Workflows can properly configure
        # the subsequent fanout task via an Output parameter
        #
        # Docker and PNS workflow executors can get output parameters from the base
        # layer (e.g. /tmp), but the Kubelet nor the K8SAPI nor the emissary executors
        # can. It is also unlikely we can get output parameters from the base layer if
        # we run pods with a security context. We work around this constraint by
        # mounting an emptyDir volume.
        if graph[step_name].type == "foreach":
            with open("/mnt/out/splits", "w") as file:
                json.dump(list(range(flow._foreach_num_splits)), file)
        # Unfortunately, we can't always use pod names as task-ids since the pod names
        # are not static across retries. We write the task-id to a file that is read
        # by the next task here.
        with open("/mnt/out/task_id", "w") as file:
            file.write(self.task_id)
