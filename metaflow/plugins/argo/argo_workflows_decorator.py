import json
import os
import time

from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from metaflow import current

from .argo_events import ArgoEvent


class ArgoWorkflowsInternalDecorator(StepDecorator):
    name = "argo_workflows_internal"

    defaults = {"auto-emit-argo-events": True}

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
        self.run_id = run_id
        self.argo_workflow_template = os.environ["ARGO_WORKFLOW_TEMPLATE"]

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

        # TODO (savin): Also register Argo Events metadata if the flow was triggered
        #               through Argo Events.
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

        # Emit Argo Events given that the flow has succeeded. Given that we only
        # emit events when the flow succeeds, we can piggy back on this decorator
        # hook which is guaranteed to execute only after rest of the task has
        # finished execution.
        if step_name == "end" and self.attributes["auto-emit-argo-events"]:
            # Auto generated flow level events have the same name as the Argo Workflow
            # Template that emitted them (which includes project/branch information).
            name = self.argo_workflow_template
            pathspec = "%s/%s" % (flow.name, self.run_id)

            event = ArgoEvent(name=name)
            event.add_to_payload("pathspec", "%s/%s" % (flow.name, self.run_id))
            event.add_to_payload("flow_name", flow.name)
            event.add_to_payload("run_id", self.run_id)
            # Add @project decorator related fields. These are used to subset
            # @trigger_on_finish related filters.
            for key in (
                "project_name",
                "branch_name",
                "is_user_branch",
                "is_production",
                "project_flow_name",
            ):
                if current.get(key):
                    event.add_to_payload(key, current.get(key))
            event.add_to_payload("auto-generated-by-metaflow", True)
            event.add_to_payload("timestamp", int(time.time()))
            # TODO: Add more fields
            event.publish()
