import json
import os
import time

from metaflow import current
from metaflow.decorators import StepDecorator
from metaflow.events import MetaflowEvent
from metaflow.metadata import MetaDatum

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

        meta = {}
        meta["argo-workflow-template"] = os.environ["ARGO_WORKFLOW_TEMPLATE"]
        meta["argo-workflow-name"] = os.environ["ARGO_WORKFLOW_NAME"]
        meta["argo-workflow-namespace"] = os.environ["ARGO_WORKFLOW_NAMESPACE"]
        meta["auto-emit-argo-events"] = self.attributes["auto-emit-argo-events"]
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

        # Expose events through current singleton
        if flow._flow_decorators.get("trigger"):
            # TODO: Introduce MetaflowTrigger instead of trigger dict
            trigger = {}
            for event in flow._flow_decorators.get("trigger")[0].events:
                payload = os.environ.get("METAFLOW_ARGO_EVENT_%s" % event["name"])
                if payload and payload != "null":  # Argo-Workflow's None
                    try:
                        payload = json.loads(payload)
                    except (TypeError, ValueError):
                        payload = {}
                    trigger[event["name"]] = MetaflowEvent(
                        **{
                            "timestamp": payload.get("timestamp"),
                            "id": payload.get("id"),
                            "name": event["name"]
                            # Add more event metadata here
                        }
                    )
            if trigger:
                current._update_env({"trigger": trigger})

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
        # emit events when the task succeeds, we can piggy back on this decorator
        # hook which is guaranteed to execute only after rest of the task has
        # finished execution.

        if self.attributes["auto-emit-argo-events"]:
            # Event name is set to flow name. The expectation is that every downstream
            # consumer will primarily filter on flow name or flow name & step name.
            event = ArgoEvent(name=flow.name)
            event.add_to_payload("pathspec", current.pathspec)
            event.add_to_payload("flow_name", flow.name)
            event.add_to_payload("run_id", self.run_id)
            event.add_to_payload("step_name", step_name)
            event.add_to_payload("task_id", self.task_id)
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
            # Add more fields here...
            event.add_to_payload("auto-generated-by-metaflow", True)
            # Keep in mind that any errors raised here will fail the run but the task
            # will still be marked as success. That's why we explicitly swallow any
            # errors and instead print them to std.err.
            event.publish(ignore_errors=True)
