import base64
import json
import os
import re
import shlex
import sys
from collections import defaultdict
from hashlib import sha1
from math import inf
from typing import List

from metaflow import JSONType, current
from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException
from metaflow.graph import FlowGraph
from metaflow.includefile import FilePathClass
from metaflow.metaflow_config import (
    ARGO_EVENTS_EVENT,
    ARGO_EVENTS_EVENT_BUS,
    ARGO_EVENTS_EVENT_SOURCE,
    ARGO_EVENTS_INTERNAL_WEBHOOK_URL,
    ARGO_EVENTS_SERVICE_ACCOUNT,
    ARGO_EVENTS_WEBHOOK_AUTH,
    ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT,
    ARGO_WORKFLOWS_ENV_VARS_TO_SKIP,
    ARGO_WORKFLOWS_KUBERNETES_SECRETS,
    ARGO_WORKFLOWS_UI_URL,
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    AZURE_KEY_VAULT_PREFIX,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    CARD_AZUREROOT,
    CARD_GSROOT,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_METADATA,
    DEFAULT_SECRETS_BACKEND_TYPE,
    GCP_SECRET_MANAGER_PREFIX,
    KUBERNETES_FETCH_EC2_METADATA,
    KUBERNETES_NAMESPACE,
    KUBERNETES_SANDBOX_INIT_SCRIPT,
    KUBERNETES_SECRETS,
    S3_ENDPOINT_URL,
    S3_SERVER_SIDE_ENCRYPTION,
    SERVICE_HEADERS,
    SERVICE_INTERNAL_URL,
    UI_URL,
)
from metaflow.metaflow_config_funcs import config_values
from metaflow.mflog import BASH_SAVE_LOGS, bash_capture_logs, export_mflog_env_vars
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.kubernetes.kube_utils import qos_requests_and_limits

from metaflow.plugins.kubernetes.kubernetes_jobsets import KubernetesArgoJobSet
from metaflow.unbounded_foreach import UBF_CONTROL, UBF_TASK
from metaflow.user_configs.config_options import ConfigInput
from metaflow.util import (
    compress_list,
    dict_to_cli_options,
    to_bytes,
    to_camelcase,
    to_unicode,
)

from .argo_client import ArgoClient
from .exit_hooks import ExitHookHack, HttpExitHook, ContainerHook
from metaflow.util import resolve_identity


class ArgoWorkflowsException(MetaflowException):
    headline = "Argo Workflows error"


class ArgoWorkflowsSchedulingException(MetaflowException):
    headline = "Argo Workflows scheduling error"


# List of future enhancements -
#     1. Configure Argo metrics.
#     2. Support resuming failed workflows within Argo Workflows.
#     3. Add Metaflow tags to labels/annotations.
#     4. Support R lang.
#     5. Ping @savin at slack.outerbounds.co for any feature request


class ArgoWorkflows(object):
    def __init__(
        self,
        name,
        graph: FlowGraph,
        flow,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        production_token,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        workflow_timeout=None,
        workflow_priority=None,
        auto_emit_argo_events=False,
        notify_on_error=False,
        notify_on_success=False,
        notify_slack_webhook_url=None,
        notify_pager_duty_integration_key=None,
        notify_incident_io_api_key=None,
        incident_io_alert_source_config_id=None,
        incident_io_metadata: List[str] = None,
        enable_heartbeat_daemon=True,
        enable_error_msg_capture=False,
    ):
        # Some high-level notes -
        #
        # Fail-fast behavior for Argo Workflows - Argo stops
        # scheduling new steps as soon as it detects that one of the DAG nodes
        # has failed. After waiting for all the scheduled DAG nodes to run till
        # completion, Argo with fail the DAG. This implies that after a node
        # has failed, it may be awhile before the entire DAG is marked as
        # failed. There is nothing Metaflow can do here for failing even
        # faster (as of Argo 3.2).
        #
        # argo stop` vs `argo terminate` - since we don't currently
        # rely on any exit handlers, it's safe to either stop or terminate any running
        # argo workflow deployed through Metaflow. This may not hold true, once we
        # integrate with Argo Events.
        #
        # Currently, an Argo Workflow can only execute entirely within a single
        # Kubernetes namespace. Multi-cluster / Multi-namespace execution is on the
        # deck for v3.4 release for Argo Workflows; beyond which point, we will be
        # able to support them natively.
        #
        # Since this implementation generates numerous templates on the fly, please
        # ensure that your Argo Workflows controller doesn't restrict
        # templateReferencing.

        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_metadata = code_package_metadata
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.production_token = production_token
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.workflow_timeout = workflow_timeout
        self.workflow_priority = workflow_priority
        self.auto_emit_argo_events = auto_emit_argo_events
        self.notify_on_error = notify_on_error
        self.notify_on_success = notify_on_success
        self.notify_slack_webhook_url = notify_slack_webhook_url
        self.notify_pager_duty_integration_key = notify_pager_duty_integration_key
        self.notify_incident_io_api_key = notify_incident_io_api_key
        self.incident_io_alert_source_config_id = incident_io_alert_source_config_id
        self.incident_io_metadata = self.parse_incident_io_metadata(
            incident_io_metadata
        )
        self.enable_heartbeat_daemon = enable_heartbeat_daemon
        self.enable_error_msg_capture = enable_error_msg_capture
        self.parameters = self._process_parameters()
        self.config_parameters = self._process_config_parameters()
        self.triggers, self.trigger_options = self._process_triggers()
        self._schedule, self._timezone = self._get_schedule()

        self._base_labels = self._base_kubernetes_labels()
        self._base_annotations = self._base_kubernetes_annotations()
        self._workflow_template = self._compile_workflow_template()
        self._sensor = self._compile_sensor()

    def __str__(self):
        return str(self._workflow_template)

    def deploy(self):
        try:
            # Register workflow template.
            ArgoClient(namespace=KUBERNETES_NAMESPACE).register_workflow_template(
                self.name, self._workflow_template.to_json()
            )
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    @staticmethod
    def _sanitize(name):
        # Metaflow allows underscores in node names, which are disallowed in Argo
        # Workflow template names - so we swap them with hyphens which are not
        # allowed by Metaflow - guaranteeing uniqueness.
        return name.replace("_", "-")

    @staticmethod
    def _sensor_name(name):
        # Unfortunately, Argo Events Sensor names don't allow for
        # dots (sensors run into an error) which rules out self.name :(
        return name.replace(".", "-")

    @staticmethod
    def list_templates(flow_name, all=False):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)

        templates = client.get_workflow_templates()
        if templates is None:
            return []

        template_names = [
            template["metadata"]["name"]
            for template in templates
            if all
            or flow_name
            == template["metadata"]
            .get("annotations", {})
            .get("metaflow/flow_name", None)
        ]
        return template_names

    @staticmethod
    def delete(name):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)

        # Always try to delete the schedule. Failure in deleting the schedule should not
        # be treated as an error, due to any of the following reasons
        # - there might not have been a schedule, or it was deleted by some other means
        # - retaining these resources should have no consequences as long as the workflow deletion succeeds.
        # - regarding cost and compute, the significant resources are part of the workflow teardown, not the schedule.
        schedule_deleted = client.delete_cronworkflow(name)

        # The workflow might have sensors attached to it, which consume actual resources.
        # Try to delete these as well.
        sensor_deleted = client.delete_sensor(ArgoWorkflows._sensor_name(name))

        # After cleaning up related resources, delete the workflow in question.
        # Failure in deleting is treated as critical and will be made visible to the user
        # for further action.
        workflow_deleted = client.delete_workflow_template(name)
        if workflow_deleted is None:
            raise ArgoWorkflowsException(
                "The workflow *%s* doesn't exist on Argo Workflows." % name
            )

        return schedule_deleted, sensor_deleted, workflow_deleted

    @classmethod
    def terminate(cls, flow_name, name):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)

        response = client.terminate_workflow(name)
        if response is None:
            raise ArgoWorkflowsException(
                "No execution found for {flow_name}/{run_id} in Argo Workflows.".format(
                    flow_name=flow_name, run_id=name
                )
            )

    @staticmethod
    def get_workflow_status(flow_name, name):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)
        # TODO: Only look for workflows for the specified flow
        workflow = client.get_workflow(name)
        if workflow:
            # return workflow phase for now
            status = workflow.get("status", {}).get("phase")
            return status
        else:
            raise ArgoWorkflowsException(
                "No execution found for {flow_name}/{run_id} in Argo Workflows.".format(
                    flow_name=flow_name, run_id=name
                )
            )

    @staticmethod
    def suspend(name):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)

        client.suspend_workflow(name)

        return True

    @staticmethod
    def unsuspend(name):
        client = ArgoClient(namespace=KUBERNETES_NAMESPACE)

        client.unsuspend_workflow(name)

        return True

    @staticmethod
    def parse_incident_io_metadata(metadata: List[str] = None):
        "parse key value pairs into a dict for incident.io metadata if given"
        parsed_metadata = None
        if metadata is not None:
            parsed_metadata = {}
            for kv in metadata:
                key, value = kv.split("=", 1)
                if key in parsed_metadata:
                    raise MetaflowException(
                        "Incident.io Metadata *%s* provided multiple times" % key
                    )
                parsed_metadata[key] = value
        return parsed_metadata

    @classmethod
    def trigger(cls, name, parameters=None):
        if parameters is None:
            parameters = {}
        try:
            workflow_template = ArgoClient(
                namespace=KUBERNETES_NAMESPACE
            ).get_workflow_template(name)
        except Exception as e:
            raise ArgoWorkflowsException(str(e))
        if workflow_template is None:
            raise ArgoWorkflowsException(
                "The workflow *%s* doesn't exist on Argo Workflows in namespace *%s*. "
                "Please deploy your flow first." % (name, KUBERNETES_NAMESPACE)
            )
        else:
            try:
                # Check that the workflow was deployed through Metaflow
                workflow_template["metadata"]["annotations"]["metaflow/owner"]
            except KeyError:
                raise ArgoWorkflowsException(
                    "An existing non-metaflow workflow with the same name as "
                    "*%s* already exists in Argo Workflows. \nPlease modify the "
                    "name of this flow or delete your existing workflow on Argo "
                    "Workflows before proceeding." % name
                )
        try:
            id_parts = resolve_identity().split(":")
            parts_size = len(id_parts)
            usertype = id_parts[0] if parts_size > 0 else "unknown"
            username = id_parts[1] if parts_size > 1 else "unknown"

            return ArgoClient(namespace=KUBERNETES_NAMESPACE).trigger_workflow_template(
                name,
                usertype,
                username,
                parameters,
            )
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    def _base_kubernetes_labels(self):
        """
        Get shared Kubernetes labels for Argo resources.
        """
        # TODO: Add configuration through an environment variable or Metaflow config in the future if required.
        labels = {"app.kubernetes.io/part-of": "metaflow"}

        return labels

    def _base_kubernetes_annotations(self):
        """
        Get shared Kubernetes annotations for Argo resources.
        """
        from datetime import datetime, timezone

        # TODO: Add configuration through an environment variable or Metaflow config in the future if required.
        # base annotations
        annotations = {
            "metaflow/production_token": self.production_token,
            "metaflow/owner": self.username,
            "metaflow/user": "argo-workflows",
            "metaflow/flow_name": self.flow.name,
            "metaflow/deployment_timestamp": str(
                datetime.now(timezone.utc).isoformat()
            ),
        }

        if current.get("project_name"):
            annotations.update(
                {
                    "metaflow/project_name": current.project_name,
                    "metaflow/branch_name": current.branch_name,
                    "metaflow/project_flow_name": current.project_flow_name,
                }
            )
        return annotations

    def _get_schedule(self):
        schedule = self.flow._flow_decorators.get("schedule")
        if schedule:
            # Remove the field "Year" if it exists
            schedule = schedule[0]
            return " ".join(schedule.schedule.split()[:5]), schedule.timezone
        return None, None

    def schedule(self):
        try:
            argo_client = ArgoClient(namespace=KUBERNETES_NAMESPACE)
            argo_client.schedule_workflow_template(
                self.name, self._schedule, self._timezone
            )
            # Register sensor.
            # Metaflow will overwrite any existing sensor.
            sensor_name = ArgoWorkflows._sensor_name(self.name)
            if self._sensor:
                argo_client.register_sensor(sensor_name, self._sensor.to_json())
            else:
                # Since sensors occupy real resources, delete existing sensor if needed
                # Deregister sensors that might have existed before this deployment
                argo_client.delete_sensor(sensor_name)
        except Exception as e:
            raise ArgoWorkflowsSchedulingException(str(e))

    def trigger_explanation(self):
        # Trigger explanation for cron workflows
        if self.flow._flow_decorators.get("schedule"):
            return (
                "This workflow triggers automatically via the CronWorkflow *%s*."
                % self.name
            )

        # Trigger explanation for @trigger
        elif self.flow._flow_decorators.get("trigger"):
            return (
                "This workflow triggers automatically when the upstream %s "
                "is/are published."
                % self.list_to_prose(
                    [event["name"] for event in self.triggers], "event"
                )
            )

        # Trigger explanation for @trigger_on_finish
        elif self.flow._flow_decorators.get("trigger_on_finish"):
            return (
                "This workflow triggers automatically when the upstream %s succeed(s)"
                % self.list_to_prose(
                    [
                        # Truncate prefix `metaflow.` and suffix `.end` from event name
                        event["name"][len("metaflow.") : -len(".end")]
                        for event in self.triggers
                    ],
                    "flow",
                )
            )

        else:
            return "No triggers defined. You need to launch this workflow manually."

    @classmethod
    def get_existing_deployment(cls, name):
        workflow_template = ArgoClient(
            namespace=KUBERNETES_NAMESPACE
        ).get_workflow_template(name)
        if workflow_template is not None:
            try:
                return (
                    workflow_template["metadata"]["annotations"]["metaflow/owner"],
                    workflow_template["metadata"]["annotations"][
                        "metaflow/production_token"
                    ],
                )
            except KeyError:
                raise ArgoWorkflowsException(
                    "An existing non-metaflow workflow with the same name as "
                    "*%s* already exists in Argo Workflows. \nPlease modify the "
                    "name of this flow or delete your existing workflow on Argo "
                    "Workflows before proceeding." % name
                )
        return None

    @classmethod
    def get_execution(cls, name):
        workflow = ArgoClient(namespace=KUBERNETES_NAMESPACE).get_workflow(name)
        if workflow is not None:
            try:
                return (
                    workflow["metadata"]["annotations"]["metaflow/owner"],
                    workflow["metadata"]["annotations"]["metaflow/production_token"],
                    workflow["metadata"]["annotations"]["metaflow/flow_name"],
                    workflow["metadata"]["annotations"].get(
                        "metaflow/branch_name", None
                    ),
                    workflow["metadata"]["annotations"].get(
                        "metaflow/project_name", None
                    ),
                )
            except KeyError:
                raise ArgoWorkflowsException(
                    "A non-metaflow workflow *%s* already exists in Argo Workflows."
                    % name
                )
        return None

    def _process_parameters(self):
        parameters = {}
        has_schedule = self.flow._flow_decorators.get("schedule") is not None
        seen = set()
        for var, param in self.flow._get_parameters():
            # Throw an exception if the parameter is specified twice.
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)
            # NOTE: We skip config parameters as these do not have dynamic values,
            # and need to be treated differently.
            if param.IS_CONFIG_PARAMETER:
                continue

            extra_attrs = {}
            if param.kwargs.get("type") == JSONType:
                param_type = str(param.kwargs.get("type").name)
            elif isinstance(param.kwargs.get("type"), FilePathClass):
                param_type = str(param.kwargs.get("type").name)
                extra_attrs["is_text"] = getattr(
                    param.kwargs.get("type"), "_is_text", True
                )
                extra_attrs["encoding"] = getattr(
                    param.kwargs.get("type"), "_encoding", "utf-8"
                )
            else:
                param_type = str(param.kwargs.get("type").__name__)

            is_required = param.kwargs.get("required", False)
            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults. We currently don't have any notion
            # of data triggers in Argo Workflows.

            if "default" not in param.kwargs and is_required and has_schedule:
                raise MetaflowException(
                    "The parameter *%s* does not have a default and is required. "
                    "Scheduling such parameters via Argo CronWorkflows is not "
                    "currently supported." % param.name
                )
            default_value = deploy_time_eval(param.kwargs.get("default"))
            # If the value is not required and the value is None, we set the value to
            # the JSON equivalent of None to please argo-workflows. Unfortunately it
            # has the side effect of casting the parameter value to string null during
            # execution - which needs to be fixed imminently.
            if not is_required or default_value is not None:
                default_value = json.dumps(default_value)

            parameters[param.name] = dict(
                python_var_name=var,
                name=param.name,
                value=default_value,
                type=param_type,
                description=param.kwargs.get("help"),
                is_required=is_required,
                **extra_attrs,
            )
        return parameters

    def _process_config_parameters(self):
        parameters = []
        seen = set()
        for var, param in self.flow._get_parameters():
            if not param.IS_CONFIG_PARAMETER:
                continue
            # Throw an exception if the parameter is specified twice.
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)

            parameters.append(
                dict(name=param.name, kv_name=ConfigInput.make_key_name(param.name))
            )
        return parameters

    def _process_triggers(self):
        # Impute triggers for Argo Workflow Template specified through @trigger and
        # @trigger_on_finish decorators

        # Disallow usage of @trigger and @trigger_on_finish together for now.
        if self.flow._flow_decorators.get("trigger") and self.flow._flow_decorators.get(
            "trigger_on_finish"
        ):
            raise ArgoWorkflowsException(
                "Argo Workflows doesn't support both *@trigger* and "
                "*@trigger_on_finish* decorators concurrently yet. Use one or the "
                "other for now."
            )
        triggers = []
        options = None

        # @trigger decorator
        if self.flow._flow_decorators.get("trigger"):
            # Parameters are not duplicated, and exist in the flow. Additionally,
            # convert them to lower case since Metaflow parameters are case
            # insensitive.
            seen = set()
            # NOTE: We skip config parameters as their values can not be set through event payloads
            params = set(
                [
                    param.name.lower()
                    for var, param in self.flow._get_parameters()
                    if not param.IS_CONFIG_PARAMETER
                ]
            )
            trigger_deco = self.flow._flow_decorators.get("trigger")[0]
            trigger_deco.format_deploytime_value()
            for event in trigger_deco.triggers:
                parameters = {}
                # TODO: Add a check to guard against names starting with numerals(?)
                if not re.match(r"^[A-Za-z0-9_.-]+$", event["name"]):
                    raise ArgoWorkflowsException(
                        "Invalid event name *%s* in *@trigger* decorator. Only "
                        "alphanumeric characters, underscores(_), dashes(-) and "
                        "dots(.) are allowed." % event["name"]
                    )
                for key, value in event.get("parameters", {}).items():
                    if not re.match(r"^[A-Za-z0-9_]+$", value):
                        raise ArgoWorkflowsException(
                            "Invalid event payload key *%s* for event *%s* in "
                            "*@trigger* decorator. Only alphanumeric characters and "
                            "underscores(_) are allowed." % (value, event["name"])
                        )
                    if key.lower() not in params:
                        raise ArgoWorkflowsException(
                            "Parameter *%s* defined in the event mappings for "
                            "*@trigger* decorator not found in the flow." % key
                        )
                    if key.lower() in seen:
                        raise ArgoWorkflowsException(
                            "Duplicate entries for parameter *%s* defined in the "
                            "event mappings for *@trigger* decorator." % key.lower()
                        )
                    seen.add(key.lower())
                    parameters[key.lower()] = value
                event["parameters"] = parameters
                event["type"] = "event"
            triggers.extend(self.flow._flow_decorators.get("trigger")[0].triggers)

            # Set automatic parameter mapping iff only a single event dependency is
            # specified with no explicit parameter mapping.
            if len(triggers) == 1 and not triggers[0].get("parameters"):
                triggers[0]["parameters"] = dict(zip(params, params))
            options = self.flow._flow_decorators.get("trigger")[0].options

        # @trigger_on_finish decorator
        if self.flow._flow_decorators.get("trigger_on_finish"):
            trigger_on_finish_deco = self.flow._flow_decorators.get(
                "trigger_on_finish"
            )[0]
            trigger_on_finish_deco.format_deploytime_value()
            for event in trigger_on_finish_deco.triggers:
                # Actual filters are deduced here since we don't have access to
                # the current object in the @trigger_on_finish decorator.
                project_name = event.get("project") or current.get("project_name")
                branch_name = event.get("branch") or current.get("branch_name")
                # validate that we have complete project info for an event name
                if project_name or branch_name:
                    if not (project_name and branch_name):
                        # if one of the two is missing, we would end up listening to an event that will never be broadcast.
                        raise ArgoWorkflowsException(
                            "Incomplete project info. Please specify both 'project' and 'project_branch' or use the @project decorator"
                        )

                triggers.append(
                    {
                        # Make sure this remains consistent with the event name format
                        # in ArgoWorkflowsInternalDecorator.
                        "name": "metaflow.%s.end"
                        % ".".join(
                            v
                            for v in [
                                project_name,
                                branch_name,
                                event["flow"],
                            ]
                            if v
                        ),
                        "filters": {
                            "auto-generated-by-metaflow": True,
                            "project_name": project_name,
                            "branch_name": branch_name,
                            # TODO: Add a time filters to guard against cached events
                        },
                        "type": "run",
                        "flow": event["flow"],
                    }
                )
            options = self.flow._flow_decorators.get("trigger_on_finish")[0].options

        for event in triggers:
            # Assign a sanitized name since we need this at many places to please
            # Argo Events sensors. There is a slight possibility of name collision
            # but quite unlikely for us to worry about at this point.
            event["sanitized_name"] = "%s_%s" % (
                event["name"]
                .replace(".", "")
                .replace("-", "")
                .replace("@", "")
                .replace("+", ""),
                to_unicode(base64.b32encode(sha1(to_bytes(event["name"])).digest()))[
                    :4
                ].lower(),
            )
        return triggers, options

    def _compile_workflow_template(self):
        # This method compiles a Metaflow FlowSpec into Argo WorkflowTemplate
        #
        # WorkflowTemplate
        #   |
        #    -- WorkflowSpec
        #         |
        #          -- Array<Template>
        #                     |
        #                      -- DAGTemplate, ContainerTemplate
        #                           |                  |
        #                            -- Array<DAGTask> |
        #                                       |      |
        #                                        -- Template
        #
        # Steps in FlowSpec are represented as DAGTasks.
        # A DAGTask can reference to -
        #     a ContainerTemplate (for linear steps..) or
        #     another DAGTemplate (for nested `foreach`s).
        #
        # While we could have very well inlined container templates inside a DAGTask,
        # unfortunately Argo variable substitution ({{pod.name}}) doesn't work as
        # expected within DAGTasks
        # (https://github.com/argoproj/argo-workflows/issues/7432) and we are forced to
        # generate container templates at the top level (in WorkflowSpec) and maintain
        # references to them within the DAGTask.

        annotations = {}
        if self._schedule is not None:
            # timezone is an optional field and json dumps on None will result in null
            # hence configuring it to an empty string
            if self._timezone is None:
                self._timezone = ""
            cron_info = {"schedule": self._schedule, "tz": self._timezone}
            annotations.update({"metaflow/cron": json.dumps(cron_info)})

        if self.parameters:
            annotations.update({"metaflow/parameters": json.dumps(self.parameters)})

        # Some more annotations to populate the Argo UI nicely
        if self.tags:
            annotations.update({"metaflow/tags": json.dumps(self.tags)})
        if self.triggers:
            annotations.update(
                {
                    "metaflow/triggers": json.dumps(
                        [
                            {key: trigger.get(key) for key in ["name", "type"]}
                            for trigger in self.triggers
                        ]
                    )
                }
            )
        if self.notify_on_error:
            annotations.update(
                {
                    "metaflow/notify_on_error": json.dumps(
                        {
                            "slack": bool(self.notify_slack_webhook_url),
                            "pager_duty": bool(self.notify_pager_duty_integration_key),
                            "incident_io": bool(self.notify_incident_io_api_key),
                        }
                    )
                }
            )
        if self.notify_on_success:
            annotations.update(
                {
                    "metaflow/notify_on_success": json.dumps(
                        {
                            "slack": bool(self.notify_slack_webhook_url),
                            "pager_duty": bool(self.notify_pager_duty_integration_key),
                            "incident_io": bool(self.notify_incident_io_api_key),
                        }
                    )
                }
            )
        try:
            # Build the DAG based on the DAGNodes given by the FlowGraph for the found FlowSpec class.
            _steps_info, graph_structure = self.graph.output_steps()
            graph_info = {
                # for the time being, we only need the graph_structure. Being mindful of annotation size limits we do not include anything extra.
                "graph_structure": graph_structure
            }
        except Exception:
            graph_info = None

        dag_annotation = {"metaflow/dag": json.dumps(graph_info)}

        lifecycle_hooks = self._lifecycle_hooks()
        return (
            WorkflowTemplate()
            .metadata(
                # Workflow Template metadata.
                ObjectMeta()
                .name(self.name)
                # Argo currently only supports Workflow-level namespaces. When v3.4.0
                # is released, we should be able to support multi-namespace /
                # multi-cluster scheduling.
                .namespace(KUBERNETES_NAMESPACE)
                .annotations(annotations)
                .annotations(self._base_annotations)
                .labels(self._base_labels)
                .label("app.kubernetes.io/name", "metaflow-flow")
                .annotations(dag_annotation)
            )
            .spec(
                WorkflowSpec()
                # Set overall workflow timeout.
                .active_deadline_seconds(self.workflow_timeout)
                # TODO: Allow Argo to optionally archive all workflow execution logs
                #       It's disabled for now since it requires all Argo installations
                #       to enable an artifactory repository. If log archival is
                #       enabled in workflow controller, the logs for this workflow will
                #       automatically get archived.
                # .archive_logs()
                # Don't automount service tokens for now - https://github.com/kubernetes/kubernetes/issues/16779#issuecomment-159656641
                # TODO: Service account names are currently set in the templates. We
                #       can specify the default service account name here to reduce
                #       the size of the generated YAML by a tiny bit.
                # .automount_service_account_token()
                # TODO: Support ImagePullSecrets for Argo & Kubernetes
                #       Not strictly needed since a very valid workaround exists
                #       https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#add-imagepullsecrets-to-a-service-account
                # .image_pull_secrets(...)
                # Limit workflow parallelism
                .parallelism(self.max_workers)
                # TODO: Support Prometheus metrics for Argo
                # .metrics(...)
                # TODO: Support PodGC and DisruptionBudgets
                .priority(self.workflow_priority)
                # Set workflow metadata
                .workflow_metadata(
                    Metadata()
                    .labels(self._base_labels)
                    .label("app.kubernetes.io/name", "metaflow-run")
                    .annotations(
                        {
                            **annotations,
                            **self._base_annotations,
                            **{"metaflow/run_id": "argo-{{workflow.name}}"},
                        }
                    )
                    # TODO: Set dynamic labels using labels_from. Ideally, we would
                    #       want to expose run_id as a label. It's easy to add labels,
                    #       but very difficult to remove them - let's err on the
                    #       conservative side and only add labels when we come across
                    #       use-cases for them.
                )
                # Handle parameters
                .arguments(
                    Arguments().parameters(
                        [
                            Parameter(parameter["name"])
                            .value(
                                "'%s'" % parameter["value"]
                                if parameter["type"] == "JSON"
                                else parameter["value"]
                            )
                            .description(parameter.get("description"))
                            # TODO: Better handle IncludeFile in Argo Workflows UI.
                            for parameter in self.parameters.values()
                        ]
                        + [
                            # Introduce non-required parameters for argo events so
                            # that the entire event payload can be accessed within the
                            # run. The parameter name is hashed to ensure that
                            # there won't be any collisions with Metaflow parameters.
                            Parameter(event["sanitized_name"])
                            .value(json.dumps(None))  # None in Argo Workflows world.
                            .description("auto-set by metaflow. safe to ignore.")
                            for event in self.triggers
                        ]
                    )
                )
                # Set common pod metadata.
                .pod_metadata(
                    Metadata()
                    .labels(self._base_labels)
                    .label("app.kubernetes.io/name", "metaflow-task")
                    .annotations(
                        {
                            **annotations,
                            **self._base_annotations,
                            **{
                                "metaflow/run_id": "argo-{{workflow.name}}"
                            },  # we want pods of the workflow to have the run_id as an annotation as well
                        }
                    )
                )
                # Set the entrypoint to flow name
                .entrypoint(self.flow.name)
                # OnExit hooks
                .onExit(
                    "capture-error-hook-fn-preflight"
                    if self.enable_error_msg_capture
                    else None
                )
                # Set lifecycle hooks if notifications are enabled
                .hooks(
                    {
                        lifecycle.name: lifecycle
                        for hook in lifecycle_hooks
                        for lifecycle in hook.lifecycle_hooks
                    }
                )
                # Top-level DAG template(s)
                .templates(self._dag_templates())
                # Container templates
                .templates(self._container_templates())
                # Lifecycle hook template(s)
                .templates([hook.template for hook in lifecycle_hooks])
                # Exit hook template(s)
                .templates(self._exit_hook_templates())
                # Sidecar templates (Daemon Containers)
                .templates(self._daemon_templates())
            )
        )

    # Visit every node and yield the uber DAGTemplate(s).
    def _dag_templates(self):
        def _visit(
            node,
            exit_node=None,
            templates=None,
            dag_tasks=None,
            parent_foreach=None,
        ):  # Returns Tuple[List[Template], List[DAGTask]]
            """ """
            # Every for-each node results in a separate subDAG and an equivalent
            # DAGTemplate rooted at the child of the for-each node. Each DAGTemplate
            # has a unique name - the top-level DAGTemplate is named as the name of
            # the flow and the subDAG DAGTemplates are named after the (only) descendant
            # of the for-each node.

            # Emit if we have reached the end of the sub workflow
            if dag_tasks is None:
                dag_tasks = []
            if templates is None:
                templates = []
            if exit_node is not None and exit_node is node.name:
                return templates, dag_tasks
            if node.name == "start":
                # Start node has no dependencies.
                dag_task = DAGTask(self._sanitize(node.name)).template(
                    self._sanitize(node.name)
                )
            elif (
                node.is_inside_foreach
                and self.graph[node.in_funcs[0]].type == "foreach"
                and not self.graph[node.in_funcs[0]].parallel_foreach
                # We need to distinguish what is a "regular" foreach (i.e something that doesn't care about to gang semantics)
                # vs what is a "num_parallel" based foreach (i.e. something that follows gang semantics.)
                # A `regular` foreach is basically any arbitrary kind of foreach.
            ):
                # Child of a foreach node needs input-paths as well as split-index
                # This child is the first node of the sub workflow and has no dependency

                parameters = [
                    Parameter("input-paths").value("{{inputs.parameters.input-paths}}"),
                    Parameter("split-index").value("{{inputs.parameters.split-index}}"),
                ]
                dag_task = (
                    DAGTask(self._sanitize(node.name))
                    .template(self._sanitize(node.name))
                    .arguments(Arguments().parameters(parameters))
                )
            elif node.parallel_step:
                # This is the step where the @parallel decorator is defined.
                # Since this DAGTask will call the for the `resource` [based templates]
                # (https://argo-workflows.readthedocs.io/en/stable/walk-through/kubernetes-resources/)
                # we have certain constraints on the way we can pass information inside the Jobset manifest
                # [All templates will have access](https://argo-workflows.readthedocs.io/en/stable/variables/#all-templates)
                # to the `inputs.parameters` so we will pass down ANY/ALL information using the
                # input parameters.
                # We define the usual parameters like input-paths/split-index etc. but we will also
                # define the following:
                # - `workerCount`:  parameter which will be used to determine the number of
                #                   parallel worker jobs
                # - `jobset-name`:  parameter which will be used to determine the name of the jobset.
                #                   This parameter needs to be dynamic so that when we have retries we don't
                #                   end up using the name of the jobset again (if we do, it will crash since k8s wont allow duplicated job names)
                # - `retryCount`:   parameter which will be used to determine the number of retries
                #                   This parameter will *only* be available within the container templates like we
                #                   have it for all other DAGTasks and NOT for custom kubernetes resource templates.
                #                   So as a work-around, we will set it as the `retryCount` parameter instead of
                #                   setting it as a {{ retries }} in the CLI code. Once set as a input parameter,
                #                   we can use it in the Jobset Manifest templates as `{{inputs.parameters.retryCount}}`
                # - `task-id-entropy`: This is a parameter which will help derive task-ids and jobset names. This parameter
                #                   contains the relevant amount of entropy to ensure that task-ids and jobset names
                #                   are uniquish. We will also use this in the join task to construct the task-ids of
                #                   all parallel tasks since the task-ids for parallel task are minted formulaically.
                parameters = [
                    Parameter("input-paths").value("{{inputs.parameters.input-paths}}"),
                    Parameter("num-parallel").value(
                        "{{inputs.parameters.num-parallel}}"
                    ),
                    Parameter("split-index").value("{{inputs.parameters.split-index}}"),
                    Parameter("task-id-entropy").value(
                        "{{inputs.parameters.task-id-entropy}}"
                    ),
                    # we cant just use hyphens with sprig.
                    # https://github.com/argoproj/argo-workflows/issues/10567#issuecomment-1452410948
                    Parameter("workerCount").value(
                        "{{=sprig.int(sprig.sub(sprig.int(inputs.parameters['num-parallel']),1))}}"
                    ),
                ]
                if any(d.name == "retry" for d in node.decorators):
                    parameters.extend(
                        [
                            Parameter("retryCount").value("{{retries}}"),
                            # The job-setname needs to be unique for each retry
                            # and we cannot use the `generateName` field in the
                            # Jobset Manifest since we need to construct the subdomain
                            # and control pod domain name pre-hand. So we will use
                            # the retry count to ensure that the jobset name is unique
                            Parameter("jobset-name").value(
                                "js-{{inputs.parameters.task-id-entropy}}{{retries}}",
                            ),
                        ]
                    )
                else:
                    parameters.extend(
                        [
                            Parameter("jobset-name").value(
                                "js-{{inputs.parameters.task-id-entropy}}",
                            )
                        ]
                    )

                dag_task = (
                    DAGTask(self._sanitize(node.name))
                    .template(self._sanitize(node.name))
                    .arguments(Arguments().parameters(parameters))
                )
            else:
                # Every other node needs only input-paths
                parameters = [
                    Parameter("input-paths").value(
                        compress_list(
                            [
                                "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                % (n, self._sanitize(n))
                                for n in node.in_funcs
                            ],
                            # NOTE: We set zlibmin to infinite because zlib compression for the Argo input-paths breaks template value substitution.
                            zlibmin=inf,
                        )
                    )
                ]
                # NOTE: Due to limitations with Argo Workflows Parameter size we
                #       can not pass arbitrarily large lists of task id's to join tasks.
                #       Instead we ensure that task id's for foreach tasks can be
                #       deduced deterministically and pass the relevant information to
                #       the join task.
                #
                #       We need to add the split-index and root-input-path for the last
                #       step in any foreach scope and use these to generate the task id,
                #       as the join step uses the root and the cardinality of the
                #       foreach scope to generate the required id's.
                if (
                    node.is_inside_foreach
                    and self.graph[node.out_funcs[0]].type == "join"
                ):
                    if any(
                        self.graph[parent].matching_join
                        == self.graph[node.out_funcs[0]].name
                        and self.graph[parent].type == "foreach"
                        for parent in self.graph[node.out_funcs[0]].split_parents
                    ):
                        parameters.extend(
                            [
                                Parameter("split-index").value(
                                    "{{inputs.parameters.split-index}}"
                                ),
                                Parameter("root-input-path").value(
                                    "{{inputs.parameters.input-paths}}"
                                ),
                            ]
                        )

                dag_task = (
                    DAGTask(self._sanitize(node.name))
                    .dependencies(
                        [self._sanitize(in_func) for in_func in node.in_funcs]
                    )
                    .template(self._sanitize(node.name))
                    .arguments(Arguments().parameters(parameters))
                )

            dag_tasks.append(dag_task)
            # End the workflow if we have reached the end of the flow
            if node.type == "end":
                return [
                    Template(self.flow.name).dag(
                        DAGTemplate().fail_fast().tasks(dag_tasks)
                    )
                ] + templates, dag_tasks
            # For split nodes traverse all the children
            if node.type == "split":
                for n in node.out_funcs:
                    _visit(
                        self.graph[n],
                        node.matching_join,
                        templates,
                        dag_tasks,
                        parent_foreach,
                    )
                return _visit(
                    self.graph[node.matching_join],
                    exit_node,
                    templates,
                    dag_tasks,
                    parent_foreach,
                )
            # For foreach nodes generate a new sub DAGTemplate
            # We do this for "regular" foreaches (ie. `self.next(self.a, foreach=)`)
            elif node.type == "foreach":
                foreach_template_name = self._sanitize(
                    "%s-foreach-%s"
                    % (
                        node.name,
                        "parallel" if node.parallel_foreach else node.foreach_param,
                        # Since foreach's are derived based on `self.next(self.a, foreach="<varname>")`
                        # vs @parallel foreach are done based on `self.next(self.a, num_parallel="<some-number>")`,
                        # we need to ensure that `foreach_template_name` suffix is appropriately set based on the kind
                        # of foreach.
                    )
                )

                # There are two separate "DAGTask"s created for the foreach node.
                # - The first one is a "jump-off" DAGTask where we propagate the
                # input-paths and split-index. This thing doesn't create
                # any actual containers and it responsible for only propagating
                # the parameters.
                # - The DAGTask that follows first DAGTask is the one
                # that uses the ContainerTemplate. This DAGTask is named the same
                # thing as the foreach node. We will leverage a similar pattern for the
                # @parallel tasks.
                #
                foreach_task = (
                    DAGTask(foreach_template_name)
                    .dependencies([self._sanitize(node.name)])
                    .template(foreach_template_name)
                    .arguments(
                        Arguments().parameters(
                            [
                                Parameter("input-paths").value(
                                    "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                    % (node.name, self._sanitize(node.name))
                                ),
                                Parameter("split-index").value("{{item}}"),
                            ]
                            + (
                                [
                                    Parameter("root-input-path").value(
                                        "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                        % (node.name, self._sanitize(node.name))
                                    ),
                                ]
                                if parent_foreach
                                else []
                            )
                            + (
                                # Disabiguate parameters for a regular `foreach` vs a `@parallel` foreach
                                [
                                    Parameter("num-parallel").value(
                                        "{{tasks.%s.outputs.parameters.num-parallel}}"
                                        % self._sanitize(node.name)
                                    ),
                                    Parameter("task-id-entropy").value(
                                        "{{tasks.%s.outputs.parameters.task-id-entropy}}"
                                        % self._sanitize(node.name)
                                    ),
                                ]
                                if node.parallel_foreach
                                else []
                            )
                        )
                    )
                    .with_param(
                        # For @parallel workloads `num-splits` will be explicitly set to one so that
                        # we can piggyback on the current mechanism with which we leverage argo.
                        "{{tasks.%s.outputs.parameters.num-splits}}"
                        % self._sanitize(node.name)
                    )
                )
                dag_tasks.append(foreach_task)
                templates, dag_tasks_1 = _visit(
                    self.graph[node.out_funcs[0]],
                    node.matching_join,
                    templates,
                    [],
                    node.name,
                )

                # How do foreach's work on Argo:
                # Lets say you have the following dag: (start[sets `foreach="x"`]) --> (task-a [actual foreach]) --> (join) --> (end)
                # With argo we will :
                # (start [sets num-splits]) --> (task-a-foreach-(0,0) [dummy task]) --> (task-a) --> (join) --> (end)
                # The (task-a-foreach-(0,0) [dummy task]) propagates the values of the `split-index` and the input paths.
                # to the actual foreach task.
                templates.append(
                    Template(foreach_template_name)
                    .inputs(
                        Inputs().parameters(
                            [Parameter("input-paths"), Parameter("split-index")]
                            + ([Parameter("root-input-path")] if parent_foreach else [])
                            + (
                                [
                                    Parameter("num-parallel"),
                                    Parameter("task-id-entropy"),
                                    # Parameter("workerCount")
                                ]
                                if node.parallel_foreach
                                else []
                            )
                        )
                    )
                    .outputs(
                        Outputs().parameters(
                            [
                                # non @parallel tasks set task-ids as outputs
                                Parameter("task-id").valueFrom(
                                    {
                                        "parameter": "{{tasks.%s.outputs.parameters.task-id}}"
                                        % self._sanitize(
                                            self.graph[node.matching_join].in_funcs[0]
                                        )
                                    }
                                )
                            ]
                            if not node.parallel_foreach
                            else [
                                # @parallel tasks set `task-id-entropy` and `num-parallel`
                                # as outputs so task-ids can be derived in the join step.
                                # Both of these values should be propagated from the
                                # jobset labels.
                                Parameter("num-parallel").valueFrom(
                                    {
                                        "parameter": "{{tasks.%s.outputs.parameters.num-parallel}}"
                                        % self._sanitize(
                                            self.graph[node.matching_join].in_funcs[0]
                                        )
                                    }
                                ),
                                Parameter("task-id-entropy").valueFrom(
                                    {
                                        "parameter": "{{tasks.%s.outputs.parameters.task-id-entropy}}"
                                        % self._sanitize(
                                            self.graph[node.matching_join].in_funcs[0]
                                        )
                                    }
                                ),
                            ]
                        )
                    )
                    .dag(DAGTemplate().fail_fast().tasks(dag_tasks_1))
                )

                join_foreach_task = (
                    DAGTask(self._sanitize(self.graph[node.matching_join].name))
                    .template(self._sanitize(self.graph[node.matching_join].name))
                    .dependencies([foreach_template_name])
                    .arguments(
                        Arguments().parameters(
                            (
                                [
                                    Parameter("input-paths").value(
                                        "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                        % (node.name, self._sanitize(node.name))
                                    ),
                                    Parameter("split-cardinality").value(
                                        "{{tasks.%s.outputs.parameters.split-cardinality}}"
                                        % self._sanitize(node.name)
                                    ),
                                ]
                                if not node.parallel_foreach
                                else [
                                    Parameter("num-parallel").value(
                                        "{{tasks.%s.outputs.parameters.num-parallel}}"
                                        % self._sanitize(node.name)
                                    ),
                                    Parameter("task-id-entropy").value(
                                        "{{tasks.%s.outputs.parameters.task-id-entropy}}"
                                        % self._sanitize(node.name)
                                    ),
                                ]
                            )
                            + (
                                [
                                    Parameter("split-index").value(
                                        # TODO : Pass down these parameters to the jobset stuff.
                                        "{{inputs.parameters.split-index}}"
                                    ),
                                    Parameter("root-input-path").value(
                                        "{{inputs.parameters.input-paths}}"
                                    ),
                                ]
                                if parent_foreach
                                else []
                            )
                        )
                    )
                )
                dag_tasks.append(join_foreach_task)
                return _visit(
                    self.graph[self.graph[node.matching_join].out_funcs[0]],
                    exit_node,
                    templates,
                    dag_tasks,
                    parent_foreach,
                )
            # For linear nodes continue traversing to the next node
            if node.type in ("linear", "join", "start"):
                return _visit(
                    self.graph[node.out_funcs[0]],
                    exit_node,
                    templates,
                    dag_tasks,
                    parent_foreach,
                )
            else:
                raise ArgoWorkflowsException(
                    "Node type *%s* for step *%s* is not currently supported by "
                    "Argo Workflows." % (node.type, node.name)
                )

        # Generate daemon tasks
        daemon_tasks = [
            DAGTask("%s-task" % daemon_template.name).template(daemon_template.name)
            for daemon_template in self._daemon_templates()
        ]

        templates, _ = _visit(node=self.graph["start"], dag_tasks=daemon_tasks)
        return templates

    # Visit every node and yield ContainerTemplates.
    def _container_templates(self):
        try:
            # Kubernetes is a soft dependency for generating Argo objects.
            # We can very well remove this dependency for Argo with the downside of
            # adding a bunch more json bloat classes (looking at you... V1Container)
            from kubernetes import client as kubernetes_sdk
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import Python package 'kubernetes'. Install kubernetes "
                "sdk (https://pypi.org/project/kubernetes/) first."
            )
        for node in self.graph:
            # Resolve entry point for pod container.
            script_name = os.path.basename(sys.argv[0])
            executable = self.environment.executable(node.name)
            # TODO: Support R someday. Quite a few people will be happy.
            entrypoint = [executable, script_name]

            # The values with curly braces '{{}}' are made available by Argo
            # Workflows. Unfortunately, there are a few bugs in Argo which prevent
            # us from accessing these values as liberally as we would like to - e.g,
            # within inline templates - so we are forced to generate container templates
            run_id = "argo-{{workflow.name}}"

            # Unfortunately, we don't have any easy access to unique ids that remain
            # stable across task attempts through Argo Workflows. So, we are forced to
            # stitch them together ourselves. The task ids are a function of step name,
            # split index and the parent task id (available from input path name).
            # Ideally, we would like these task ids to be the same as node name
            # (modulo retry suffix) on Argo Workflows but that doesn't seem feasible
            # right now.

            task_idx = ""
            input_paths = ""
            root_input = None
            # export input_paths as it is used multiple times in the container script
            # and we do not want to repeat the values.
            input_paths_expr = "export INPUT_PATHS=''"
            # If node is not a start step or a @parallel join then we will set the input paths.
            # To set the input-paths as a parameter, we need to ensure that the node
            # is not (a start node or a parallel join node). Start nodes will have no
            # input paths and parallel join will derive input paths based on a
            # formulaic approach using `num-parallel` and `task-id-entropy`.
            if not (
                node.name == "start"
                or (node.type == "join" and self.graph[node.in_funcs[0]].parallel_step)
            ):
                # For parallel joins we don't pass the INPUT_PATHS but are dynamically constructed.
                # So we don't need to set the input paths.
                input_paths_expr = (
                    "export INPUT_PATHS={{inputs.parameters.input-paths}}"
                )
                input_paths = "$(echo $INPUT_PATHS)"
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                task_idx = "{{inputs.parameters.split-index}}"
            if node.is_inside_foreach and self.graph[node.out_funcs[0]].type == "join":
                if any(
                    self.graph[parent].matching_join
                    == self.graph[node.out_funcs[0]].name
                    for parent in self.graph[node.out_funcs[0]].split_parents
                    if self.graph[parent].type == "foreach"
                ) and any(not self.graph[f].type == "foreach" for f in node.in_funcs):
                    # we need to propagate the split-index and root-input-path info for
                    # the last step inside a foreach for correctly joining nested
                    # foreaches
                    task_idx = "{{inputs.parameters.split-index}}"
                    root_input = "{{inputs.parameters.root-input-path}}"

            # Task string to be hashed into an ID
            task_str = "-".join(
                [
                    node.name,
                    "{{workflow.creationTimestamp}}",
                    root_input or input_paths,
                    task_idx,
                ]
            )
            if node.parallel_step:
                task_str = "-".join(
                    [
                        "$TASK_ID_PREFIX",
                        "{{inputs.parameters.task-id-entropy}}",
                        "$TASK_ID_SUFFIX",
                    ]
                )
            else:
                # Generated task_ids need to be non-numeric - see register_task_id in
                # service.py. We do so by prefixing `t-`
                _task_id_base = (
                    "$(echo %s | md5sum | cut -d ' ' -f 1 | tail -c 9)" % task_str
                )
                task_str = "(t-%s)" % _task_id_base

            task_id_expr = "export METAFLOW_TASK_ID=" "%s" % task_str
            task_id = "$METAFLOW_TASK_ID"

            # Resolve retry strategy.
            max_user_code_retries = 0
            max_error_retries = 0
            minutes_between_retries = "2"
            for decorator in node.decorators:
                if decorator.name == "retry":
                    minutes_between_retries = decorator.attributes.get(
                        "minutes_between_retries", minutes_between_retries
                    )
                user_code_retries, error_retries = decorator.step_task_retry_count()
                max_user_code_retries = max(max_user_code_retries, user_code_retries)
                max_error_retries = max(max_error_retries, error_retries)

            user_code_retries = max_user_code_retries
            total_retries = max_user_code_retries + max_error_retries
            # {{retries}} is only available if retryStrategy is specified
            # For custom kubernetes manifests, we will pass the retryCount as a parameter
            # and use that in the manifest.
            retry_count = (
                (
                    "{{retries}}"
                    if not node.parallel_step
                    else "{{inputs.parameters.retryCount}}"
                )
                if total_retries
                else 0
            )

            minutes_between_retries = int(minutes_between_retries)

            # Configure log capture.
            mflog_expr = export_mflog_env_vars(
                datastore_type=self.flow_datastore.TYPE,
                stdout_path="$PWD/.logs/mflog_stdout",
                stderr_path="$PWD/.logs/mflog_stderr",
                flow_name=self.flow.name,
                run_id=run_id,
                step_name=node.name,
                task_id=task_id,
                retry_count=retry_count,
            )

            init_cmds = " && ".join(
                [
                    # For supporting sandboxes, ensure that a custom script is executed
                    # before anything else is executed. The script is passed in as an
                    # env var.
                    '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                    "mkdir -p $PWD/.logs",
                    input_paths_expr,
                    task_id_expr,
                    mflog_expr,
                ]
                + self.environment.get_package_commands(
                    self.code_package_url,
                    self.flow_datastore.TYPE,
                    self.code_package_metadata,
                )
            )
            step_cmds = self.environment.bootstrap_commands(
                node.name, self.flow_datastore.TYPE
            )

            top_opts_dict = {
                "with": [
                    decorator.make_decorator_spec()
                    for decorator in node.decorators
                    if not decorator.statically_defined
                    and decorator.inserted_by is None
                ]
            }
            # FlowDecorators can define their own top-level options. They are
            # responsible for adding their own top-level options and values through
            # the get_top_level_options() hook. See similar logic in runtime.py.
            for deco in flow_decorators(self.flow):
                top_opts_dict.update(deco.get_top_level_options())

            top_level = list(dict_to_cli_options(top_opts_dict)) + [
                "--quiet",
                "--metadata=%s" % self.metadata.TYPE,
                "--environment=%s" % self.environment.TYPE,
                "--datastore=%s" % self.flow_datastore.TYPE,
                "--datastore-root=%s" % self.flow_datastore.datastore_root,
                "--event-logger=%s" % self.event_logger.TYPE,
                "--monitor=%s" % self.monitor.TYPE,
                "--no-pylint",
                "--with=argo_workflows_internal:auto-emit-argo-events=%i"
                % self.auto_emit_argo_events,
            ]

            if node.name == "start":
                # Execute `init` before any step of the workflow executes
                task_id_params = "%s-params" % task_id
                init = (
                    entrypoint
                    + top_level
                    + [
                        "init",
                        "--run-id %s" % run_id,
                        "--task-id %s" % task_id_params,
                    ]
                    + [
                        # Parameter names can be hyphenated, hence we use
                        # {{foo.bar['param_name']}}.
                        # https://argoproj.github.io/argo-events/tutorials/02-parameterization/
                        # http://masterminds.github.io/sprig/strings.html
                        "--%s={{workflow.parameters.%s}}"
                        % (parameter["name"], parameter["name"])
                        for parameter in self.parameters.values()
                    ]
                )
                if self.tags:
                    init.extend("--tag %s" % tag for tag in self.tags)
                # if the start step gets retried, we must be careful
                # not to regenerate multiple parameters tasks. Hence,
                # we check first if _parameters exists already.
                exists = entrypoint + [
                    "dump",
                    "--max-value-size=0",
                    "%s/_parameters/%s" % (run_id, task_id_params),
                ]
                step_cmds.extend(
                    [
                        "if ! %s >/dev/null 2>/dev/null; then %s; fi"
                        % (" ".join(exists), " ".join(init))
                    ]
                )
                input_paths = "%s/_parameters/%s" % (run_id, task_id_params)
            elif (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            ):
                # Set aggregated input-paths for a for-each join
                foreach_step = next(
                    n for n in node.in_funcs if self.graph[n].is_inside_foreach
                )
                if not self.graph[node.split_parents[-1]].parallel_foreach:
                    input_paths = (
                        "$(python -m metaflow.plugins.argo.generate_input_paths %s {{workflow.creationTimestamp}} %s {{inputs.parameters.split-cardinality}})"
                        % (
                            foreach_step,
                            input_paths,
                        )
                    )
                else:
                    # Handle @parallel where output from volume mount isn't accessible
                    input_paths = (
                        "$(python -m metaflow.plugins.argo.jobset_input_paths %s %s {{inputs.parameters.task-id-entropy}} {{inputs.parameters.num-parallel}})"
                        % (
                            run_id,
                            foreach_step,
                        )
                    )
            step = [
                "step",
                node.name,
                "--run-id %s" % run_id,
                "--task-id %s" % task_id,
                "--retry-count %s" % retry_count,
                "--max-user-code-retries %d" % user_code_retries,
                "--input-paths %s" % input_paths,
            ]
            if node.parallel_step:
                step.append(
                    "--split-index ${MF_CONTROL_INDEX:-$((MF_WORKER_REPLICA_INDEX + 1))}"
                )
                # This is needed for setting the value of the UBF context in the CLI.
                step.append("--ubf-context $UBF_CONTEXT")

            elif any(self.graph[n].type == "foreach" for n in node.in_funcs):
                # Pass split-index to a foreach task
                step.append("--split-index {{inputs.parameters.split-index}}")
            if self.tags:
                step.extend("--tag %s" % tag for tag in self.tags)
            if self.namespace is not None:
                step.append("--namespace=%s" % self.namespace)

            step_cmds.extend([" ".join(entrypoint + top_level + step)])

            cmd_str = "%s; c=$?; %s; exit $c" % (
                " && ".join([init_cmds, bash_capture_logs(" && ".join(step_cmds))]),
                BASH_SAVE_LOGS,
            )
            cmds = shlex.split('bash -c "%s"' % cmd_str)

            # Resolve resource requirements.
            resources = dict(
                [deco for deco in node.decorators if deco.name == "kubernetes"][
                    0
                ].attributes
            )

            if (
                resources["namespace"]
                and resources["namespace"] != KUBERNETES_NAMESPACE
            ):
                raise ArgoWorkflowsException(
                    "Multi-namespace Kubernetes execution of flows in Argo Workflows "
                    "is not currently supported. \nStep *%s* is trying to override "
                    "the default Kubernetes namespace *%s*."
                    % (node.name, KUBERNETES_NAMESPACE)
                )

            run_time_limit = [
                deco for deco in node.decorators if deco.name == "kubernetes"
            ][0].run_time_limit

            # Resolve @environment decorator. We set three classes of environment
            # variables -
            #   (1) User-specified environment variables through @environment
            #   (2) Metaflow runtime specific environment variables
            #   (3) @kubernetes, @argo_workflows_internal bookkeeping environment
            #       variables
            env = dict(
                [deco for deco in node.decorators if deco.name == "environment"][
                    0
                ].attributes["vars"]
            )

            # Temporary passing of *some* environment variables. Do not rely on this
            # mechanism as it will be removed in the near future
            env.update(
                {
                    k: v
                    for k, v in config_values()
                    if k.startswith("METAFLOW_CONDA_")
                    or k.startswith("METAFLOW_DEBUG_")
                }
            )

            env.update(
                {
                    **{
                        # These values are needed by Metaflow to set it's internal
                        # state appropriately.
                        "METAFLOW_CODE_METADATA": self.code_package_metadata,
                        "METAFLOW_CODE_URL": self.code_package_url,
                        "METAFLOW_CODE_SHA": self.code_package_sha,
                        "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
                        "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
                        "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
                        "METAFLOW_USER": "argo-workflows",
                        "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
                        "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
                        "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
                        "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
                        "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
                        "METAFLOW_KUBERNETES_WORKLOAD": 1,
                        "METAFLOW_KUBERNETES_FETCH_EC2_METADATA": KUBERNETES_FETCH_EC2_METADATA,
                        "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
                        "METAFLOW_OWNER": self.username,
                    },
                    **{
                        # Configuration for Argo Events. Keep these in sync with the
                        # environment variables for @kubernetes decorator.
                        "METAFLOW_ARGO_EVENTS_EVENT": ARGO_EVENTS_EVENT,
                        "METAFLOW_ARGO_EVENTS_EVENT_BUS": ARGO_EVENTS_EVENT_BUS,
                        "METAFLOW_ARGO_EVENTS_EVENT_SOURCE": ARGO_EVENTS_EVENT_SOURCE,
                        "METAFLOW_ARGO_EVENTS_SERVICE_ACCOUNT": ARGO_EVENTS_SERVICE_ACCOUNT,
                        "METAFLOW_ARGO_EVENTS_WEBHOOK_URL": ARGO_EVENTS_INTERNAL_WEBHOOK_URL,
                        "METAFLOW_ARGO_EVENTS_WEBHOOK_AUTH": ARGO_EVENTS_WEBHOOK_AUTH,
                    },
                    **{
                        # Some optional values for bookkeeping
                        "METAFLOW_FLOW_FILENAME": os.path.basename(sys.argv[0]),
                        "METAFLOW_FLOW_NAME": self.flow.name,
                        "METAFLOW_STEP_NAME": node.name,
                        "METAFLOW_RUN_ID": run_id,
                        # "METAFLOW_TASK_ID": task_id,
                        "METAFLOW_RETRY_COUNT": retry_count,
                        "METAFLOW_PRODUCTION_TOKEN": self.production_token,
                        "ARGO_WORKFLOW_TEMPLATE": self.name,
                        "ARGO_WORKFLOW_NAME": "{{workflow.name}}",
                        "ARGO_WORKFLOW_NAMESPACE": KUBERNETES_NAMESPACE,
                    },
                    **self.metadata.get_runtime_environment("argo-workflows"),
                }
            )
            # add METAFLOW_S3_ENDPOINT_URL
            env["METAFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL

            # support Metaflow sandboxes
            env["METAFLOW_INIT_SCRIPT"] = KUBERNETES_SANDBOX_INIT_SCRIPT
            env["METAFLOW_KUBERNETES_SANDBOX_INIT_SCRIPT"] = (
                KUBERNETES_SANDBOX_INIT_SCRIPT
            )

            # support for @secret
            env["METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE"] = DEFAULT_SECRETS_BACKEND_TYPE
            env["METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION"] = (
                AWS_SECRETS_MANAGER_DEFAULT_REGION
            )
            env["METAFLOW_GCP_SECRET_MANAGER_PREFIX"] = GCP_SECRET_MANAGER_PREFIX
            env["METAFLOW_AZURE_KEY_VAULT_PREFIX"] = AZURE_KEY_VAULT_PREFIX

            # support for Azure
            env["METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT"] = (
                AZURE_STORAGE_BLOB_SERVICE_ENDPOINT
            )
            env["METAFLOW_DATASTORE_SYSROOT_AZURE"] = DATASTORE_SYSROOT_AZURE
            env["METAFLOW_CARD_AZUREROOT"] = CARD_AZUREROOT
            env["METAFLOW_ARGO_WORKFLOWS_KUBERNETES_SECRETS"] = (
                ARGO_WORKFLOWS_KUBERNETES_SECRETS
            )
            env["METAFLOW_ARGO_WORKFLOWS_ENV_VARS_TO_SKIP"] = (
                ARGO_WORKFLOWS_ENV_VARS_TO_SKIP
            )

            # support for GCP
            env["METAFLOW_DATASTORE_SYSROOT_GS"] = DATASTORE_SYSROOT_GS
            env["METAFLOW_CARD_GSROOT"] = CARD_GSROOT

            # Map Argo Events payload (if any) to environment variables
            if self.triggers:
                for event in self.triggers:
                    env[
                        "METAFLOW_ARGO_EVENT_PAYLOAD_%s_%s"
                        % (event["type"], event["sanitized_name"])
                    ] = ("{{workflow.parameters.%s}}" % event["sanitized_name"])

            # Map S3 upload headers to environment variables
            if S3_SERVER_SIDE_ENCRYPTION is not None:
                env["METAFLOW_S3_SERVER_SIDE_ENCRYPTION"] = S3_SERVER_SIDE_ENCRYPTION

            metaflow_version = self.environment.get_environment_info()
            metaflow_version["flow_name"] = self.graph.name
            metaflow_version["production_token"] = self.production_token
            env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

            # map config values
            cfg_env = {
                param["name"]: param["kv_name"] for param in self.config_parameters
            }
            if cfg_env:
                env["METAFLOW_FLOW_CONFIG_VALUE"] = json.dumps(cfg_env)

            # Set the template inputs and outputs for passing state. Very simply,
            # the container template takes in input-paths as input and outputs
            # the task-id (which feeds in as input-paths to the subsequent task).
            # In addition to that, if the parent of the node under consideration
            # is a for-each node, then we take the split-index as an additional
            # input. Analogously, if the node under consideration is a foreach
            # node, then we emit split cardinality as an extra output. I would like
            # to thank the designers of Argo Workflows for making this so
            # straightforward! Things become a bit more complicated to support very
            # wide foreaches where we have to resort to passing a root-input-path
            # so that we can compute the task ids for each parent task of a for-each
            # join task deterministically inside the join task without resorting to
            # passing a rather long list of (albiet compressed)
            inputs = []
            # To set the input-paths as a parameter, we need to ensure that the node
            # is not (a start node or a parallel join node). Start nodes will have no
            # input paths and parallel join will derive input paths based on a
            # formulaic approach.
            if not (
                node.name == "start"
                or (node.type == "join" and self.graph[node.in_funcs[0]].parallel_step)
            ):
                inputs.append(Parameter("input-paths"))
            if any(self.graph[n].type == "foreach" for n in node.in_funcs):
                # Fetch split-index from parent
                inputs.append(Parameter("split-index"))

            if (
                node.type == "join"
                and self.graph[node.split_parents[-1]].type == "foreach"
            ):
                # @parallel join tasks require `num-parallel` and `task-id-entropy`
                # to construct the input paths, so we pass them down as input parameters.
                if self.graph[node.split_parents[-1]].parallel_foreach:
                    inputs.extend(
                        [Parameter("num-parallel"), Parameter("task-id-entropy")]
                    )
                else:
                    # append this only for joins of foreaches, not static splits
                    inputs.append(Parameter("split-cardinality"))
            # check if the node is a @parallel node.
            elif node.parallel_step:
                inputs.extend(
                    [
                        Parameter("num-parallel"),
                        Parameter("task-id-entropy"),
                        Parameter("jobset-name"),
                        Parameter("workerCount"),
                    ]
                )
                if any(d.name == "retry" for d in node.decorators):
                    inputs.append(Parameter("retryCount"))

            if node.is_inside_foreach and self.graph[node.out_funcs[0]].type == "join":
                if any(
                    self.graph[parent].matching_join
                    == self.graph[node.out_funcs[0]].name
                    for parent in self.graph[node.out_funcs[0]].split_parents
                    if self.graph[parent].type == "foreach"
                ) and any(not self.graph[f].type == "foreach" for f in node.in_funcs):
                    # we need to propagate the split-index and root-input-path info for
                    # the last step inside a foreach for correctly joining nested
                    # foreaches
                    if not any(self.graph[n].type == "foreach" for n in node.in_funcs):
                        # Don't add duplicate split index parameters.
                        inputs.append(Parameter("split-index"))
                    inputs.append(Parameter("root-input-path"))

            outputs = []
            # @parallel steps will not have a task-id as an output parameter since task-ids
            # are derived at runtime.
            if not (node.name == "end" or node.parallel_step):
                outputs = [Parameter("task-id").valueFrom({"path": "/mnt/out/task_id"})]
            if node.type == "foreach":
                # Emit split cardinality from foreach task
                outputs.append(
                    Parameter("num-splits").valueFrom({"path": "/mnt/out/splits"})
                )
                outputs.append(
                    Parameter("split-cardinality").valueFrom(
                        {"path": "/mnt/out/split_cardinality"}
                    )
                )

            if node.parallel_foreach:
                outputs.extend(
                    [
                        Parameter("num-parallel").valueFrom(
                            {"path": "/mnt/out/num_parallel"}
                        ),
                        Parameter("task-id-entropy").valueFrom(
                            {"path": "/mnt/out/task_id_entropy"}
                        ),
                    ]
                )
            # Outputs should be defined over here and not in the _dag_template for @parallel.

            # It makes no sense to set env vars to None (shows up as "None" string)
            # Also we skip some env vars (e.g. in case we want to pull them from KUBERNETES_SECRETS)
            env = {
                k: v
                for k, v in env.items()
                if v is not None
                and k not in set(ARGO_WORKFLOWS_ENV_VARS_TO_SKIP.split(","))
            }

            # Tmpfs variables
            use_tmpfs = resources["use_tmpfs"]
            tmpfs_size = resources["tmpfs_size"]
            tmpfs_path = resources["tmpfs_path"]
            tmpfs_tempdir = resources["tmpfs_tempdir"]
            # Set shared_memory to 0 if it isn't specified. This results
            # in Kubernetes using it's default value when the pod is created.
            shared_memory = resources.get("shared_memory", 0)
            port = resources.get("port", None)
            if port:
                port = int(port)

            tmpfs_enabled = use_tmpfs or (tmpfs_size and not use_tmpfs)

            if tmpfs_enabled and tmpfs_tempdir:
                env["METAFLOW_TEMPDIR"] = tmpfs_path

            qos_requests, qos_limits = qos_requests_and_limits(
                resources["qos"],
                resources["cpu"],
                resources["memory"],
                resources["disk"],
            )

            security_context = resources.get("security_context", None)
            _security_context = {}
            if security_context is not None and len(security_context) > 0:
                _security_context = {
                    "security_context": kubernetes_sdk.V1SecurityContext(
                        **security_context
                    )
                }

            # Create a ContainerTemplate for this node. Ideally, we would have
            # liked to inline this ContainerTemplate and avoid scanning the workflow
            # twice, but due to issues with variable substitution, we will have to
            # live with this routine.
            if node.parallel_step:
                jobset_name = "{{inputs.parameters.jobset-name}}"
                jobset = KubernetesArgoJobSet(
                    kubernetes_sdk=kubernetes_sdk,
                    name=jobset_name,
                    flow_name=self.flow.name,
                    run_id=run_id,
                    step_name=self._sanitize(node.name),
                    task_id=task_id,
                    attempt=retry_count,
                    user=self.username,
                    subdomain=jobset_name,
                    command=cmds,
                    namespace=resources["namespace"],
                    image=resources["image"],
                    image_pull_policy=resources["image_pull_policy"],
                    image_pull_secrets=resources["image_pull_secrets"],
                    service_account=resources["service_account"],
                    secrets=(
                        [
                            k
                            for k in (
                                list(
                                    []
                                    if not resources.get("secrets")
                                    else (
                                        [resources.get("secrets")]
                                        if isinstance(resources.get("secrets"), str)
                                        else resources.get("secrets")
                                    )
                                )
                                + KUBERNETES_SECRETS.split(",")
                                + ARGO_WORKFLOWS_KUBERNETES_SECRETS.split(",")
                            )
                            if k
                        ]
                    ),
                    node_selector=resources.get("node_selector"),
                    cpu=str(resources["cpu"]),
                    memory=str(resources["memory"]),
                    disk=str(resources["disk"]),
                    gpu=resources["gpu"],
                    gpu_vendor=str(resources["gpu_vendor"]),
                    tolerations=resources["tolerations"],
                    use_tmpfs=use_tmpfs,
                    tmpfs_tempdir=tmpfs_tempdir,
                    tmpfs_size=tmpfs_size,
                    tmpfs_path=tmpfs_path,
                    timeout_in_seconds=run_time_limit,
                    persistent_volume_claims=resources["persistent_volume_claims"],
                    shared_memory=shared_memory,
                    port=port,
                    qos=resources["qos"],
                    security_context=security_context,
                )

                for k, v in env.items():
                    jobset.environment_variable(k, v)

                # Set labels. Do not allow user-specified task labels to override internal ones.
                #
                # Explicitly add the task-id-hint label. This is important because this label
                # is returned as an Output parameter of this step and is used subsequently as an
                # an input in the join step.
                kubernetes_labels = {
                    "task_id_entropy": "{{inputs.parameters.task-id-entropy}}",
                    "num_parallel": "{{inputs.parameters.num-parallel}}",
                    "metaflow/argo-workflows-name": "{{workflow.name}}",
                    "workflows.argoproj.io/workflow": "{{workflow.name}}",
                }
                jobset.labels(
                    {
                        **resources["labels"],
                        **self._base_labels,
                        **kubernetes_labels,
                    }
                )

                jobset.environment_variable(
                    "MF_MASTER_ADDR", jobset.jobset_control_addr
                )
                jobset.environment_variable("MF_MASTER_PORT", str(port))
                jobset.environment_variable(
                    "MF_WORLD_SIZE", "{{inputs.parameters.num-parallel}}"
                )
                # We need this task-id set so that all the nodes are aware of the control
                # task's task-id. These "MF_" variables populate the `current.parallel` namedtuple
                jobset.environment_variable(
                    "MF_PARALLEL_CONTROL_TASK_ID",
                    "control-{{inputs.parameters.task-id-entropy}}-0",
                )
                # for k, v in .items():
                jobset.environment_variables_from_selectors(
                    {
                        "MF_WORKER_REPLICA_INDEX": "metadata.annotations['jobset.sigs.k8s.io/job-index']",
                        "JOBSET_RESTART_ATTEMPT": "metadata.annotations['jobset.sigs.k8s.io/restart-attempt']",
                        "METAFLOW_KUBERNETES_JOBSET_NAME": "metadata.annotations['jobset.sigs.k8s.io/jobset-name']",
                        "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                        "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                        "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                        "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                        "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
                        "TASK_ID_SUFFIX": "metadata.annotations['jobset.sigs.k8s.io/job-index']",
                    }
                )

                # Set annotations. Do not allow user-specified task-specific annotations to override internal ones.
                annotations = {
                    # setting annotations explicitly as they wont be
                    # passed down from WorkflowTemplate level
                    "metaflow/step_name": node.name,
                    "metaflow/attempt": str(retry_count),
                    "metaflow/run_id": run_id,
                }

                jobset.annotations(
                    {
                        **resources["annotations"],
                        **self._base_annotations,
                        **annotations,
                    }
                )

                jobset.control.replicas(1)
                jobset.worker.replicas("{{=asInt(inputs.parameters.workerCount)}}")
                jobset.control.environment_variable("UBF_CONTEXT", UBF_CONTROL)
                jobset.worker.environment_variable("UBF_CONTEXT", UBF_TASK)
                jobset.control.environment_variable("MF_CONTROL_INDEX", "0")
                # `TASK_ID_PREFIX` needs to explicitly be `control` or `worker`
                # because the join task uses a formulaic approach to infer the task-ids
                jobset.control.environment_variable("TASK_ID_PREFIX", "control")
                jobset.worker.environment_variable("TASK_ID_PREFIX", "worker")

                yield (
                    Template(ArgoWorkflows._sanitize(node.name))
                    .resource(
                        "create",
                        jobset.dump(),
                        "status.terminalState == Completed",
                        "status.terminalState == Failed",
                    )
                    .inputs(Inputs().parameters(inputs))
                    .outputs(
                        Outputs().parameters(
                            [
                                Parameter("task-id-entropy").valueFrom(
                                    {"jsonPath": "{.metadata.labels.task_id_entropy}"}
                                ),
                                Parameter("num-parallel").valueFrom(
                                    {"jsonPath": "{.metadata.labels.num_parallel}"}
                                ),
                            ]
                        )
                    )
                    .retry_strategy(
                        times=total_retries,
                        minutes_between_retries=minutes_between_retries,
                    )
                )
            else:
                yield (
                    Template(self._sanitize(node.name))
                    # Set @timeout values
                    .active_deadline_seconds(run_time_limit)
                    # Set service account
                    .service_account_name(resources["service_account"])
                    # Configure template input
                    .inputs(Inputs().parameters(inputs))
                    # Configure template output
                    .outputs(Outputs().parameters(outputs))
                    # Fail fast!
                    .fail_fast()
                    # Set @retry/@catch values
                    .retry_strategy(
                        times=total_retries,
                        minutes_between_retries=minutes_between_retries,
                    )
                    .metadata(
                        ObjectMeta()
                        .annotation("metaflow/step_name", node.name)
                        # Unfortunately, we can't set the task_id since it is generated
                        # inside the pod. However, it can be inferred from the annotation
                        # set by argo-workflows - `workflows.argoproj.io/outputs` - refer
                        # the field 'task-id' in 'parameters'
                        # .annotation("metaflow/task_id", ...)
                        .annotation("metaflow/attempt", retry_count)
                        .annotations(resources["annotations"])
                        .labels(resources["labels"])
                    )
                    # Set emptyDir volume for state management
                    .empty_dir_volume("out")
                    # Set tmpfs emptyDir volume if enabled
                    .empty_dir_volume(
                        "tmpfs-ephemeral-volume",
                        medium="Memory",
                        size_limit=tmpfs_size if tmpfs_enabled else 0,
                    )
                    .empty_dir_volume("dhsm", medium="Memory", size_limit=shared_memory)
                    .pvc_volumes(resources.get("persistent_volume_claims"))
                    # Set node selectors
                    .node_selectors(resources.get("node_selector"))
                    # Set tolerations
                    .tolerations(resources.get("tolerations"))
                    # Set image pull secrets
                    .image_pull_secrets(resources.get("image_pull_secrets"))
                    # Set container
                    .container(
                        # TODO: Unify the logic with kubernetes.py
                        # Important note - Unfortunately, V1Container uses snakecase while
                        # Argo Workflows uses camel. For most of the attributes, both cases
                        # are indistinguishable, but unfortunately, not for all - (
                        # env_from, value_from, etc.) - so we need to handle the conversion
                        # ourselves using to_camelcase. We need to be vigilant about
                        # resources attributes in particular where the keys maybe user
                        # defined.
                        to_camelcase(
                            kubernetes_sdk.V1Container(
                                name=self._sanitize(node.name),
                                command=cmds,
                                termination_message_policy="FallbackToLogsOnError",
                                ports=(
                                    [
                                        kubernetes_sdk.V1ContainerPort(
                                            container_port=port
                                        )
                                    ]
                                    if port
                                    else None
                                ),
                                env=[
                                    kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                                    for k, v in env.items()
                                ]
                                # Add environment variables for book-keeping.
                                # https://argoproj.github.io/argo-workflows/fields/#fields_155
                                + [
                                    kubernetes_sdk.V1EnvVar(
                                        name=k,
                                        value_from=kubernetes_sdk.V1EnvVarSource(
                                            field_ref=kubernetes_sdk.V1ObjectFieldSelector(
                                                field_path=str(v)
                                            )
                                        ),
                                    )
                                    for k, v in {
                                        "METAFLOW_KUBERNETES_NAMESPACE": "metadata.namespace",
                                        "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                                        "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                                        "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                                        "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
                                        "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
                                    }.items()
                                ],
                                image=resources["image"],
                                image_pull_policy=resources["image_pull_policy"],
                                resources=kubernetes_sdk.V1ResourceRequirements(
                                    requests=qos_requests,
                                    limits={
                                        **qos_limits,
                                        **{
                                            "%s.com/gpu".lower()
                                            % resources["gpu_vendor"]: str(
                                                resources["gpu"]
                                            )
                                            for k in [0]
                                            if resources["gpu"] is not None
                                        },
                                    },
                                ),
                                # Configure secrets
                                env_from=[
                                    kubernetes_sdk.V1EnvFromSource(
                                        secret_ref=kubernetes_sdk.V1SecretEnvSource(
                                            name=str(k),
                                            # optional=True
                                        )
                                    )
                                    for k in list(
                                        []
                                        if not resources.get("secrets")
                                        else (
                                            [resources.get("secrets")]
                                            if isinstance(resources.get("secrets"), str)
                                            else resources.get("secrets")
                                        )
                                    )
                                    + KUBERNETES_SECRETS.split(",")
                                    + ARGO_WORKFLOWS_KUBERNETES_SECRETS.split(",")
                                    if k
                                ],
                                volume_mounts=[
                                    # Assign a volume mount to pass state to the next task.
                                    kubernetes_sdk.V1VolumeMount(
                                        name="out", mount_path="/mnt/out"
                                    )
                                ]
                                # Support tmpfs.
                                + (
                                    [
                                        kubernetes_sdk.V1VolumeMount(
                                            name="tmpfs-ephemeral-volume",
                                            mount_path=tmpfs_path,
                                        )
                                    ]
                                    if tmpfs_enabled
                                    else []
                                )
                                # Support shared_memory
                                + (
                                    [
                                        kubernetes_sdk.V1VolumeMount(
                                            name="dhsm",
                                            mount_path="/dev/shm",
                                        )
                                    ]
                                    if shared_memory
                                    else []
                                )
                                # Support persistent volume claims.
                                + (
                                    [
                                        kubernetes_sdk.V1VolumeMount(
                                            name=claim, mount_path=path
                                        )
                                        for claim, path in resources.get(
                                            "persistent_volume_claims"
                                        ).items()
                                    ]
                                    if resources.get("persistent_volume_claims")
                                    is not None
                                    else []
                                ),
                                **_security_context,
                            ).to_dict()
                        )
                    )
                )

    # Return daemon container templates for workflow execution notifications.
    def _daemon_templates(self):
        templates = []
        if self.enable_heartbeat_daemon:
            templates.append(self._heartbeat_daemon_template())
        return templates

    # Return lifecycle hooks for workflow execution notifications.
    def _lifecycle_hooks(self):
        hooks = []
        if self.notify_on_error:
            hooks.append(self._slack_error_template())
            hooks.append(self._pager_duty_alert_template())
            hooks.append(self._incident_io_alert_template())
        if self.notify_on_success:
            hooks.append(self._slack_success_template())
            hooks.append(self._pager_duty_change_template())
            hooks.append(self._incident_io_change_template())

        exit_hook_decos = self.flow._flow_decorators.get("exit_hook", [])

        for deco in exit_hook_decos:
            hooks.extend(self._lifecycle_hook_from_deco(deco))

        # Clean up None values from templates.
        hooks = list(filter(None, hooks))

        if hooks:
            hooks.append(
                ExitHookHack(
                    url=(
                        self.notify_slack_webhook_url
                        or "https://events.pagerduty.com/v2/enqueue"
                    )
                )
            )
        return hooks

    def _lifecycle_hook_from_deco(self, deco):
        from kubernetes import client as kubernetes_sdk

        start_step = [step for step in self.graph if step.name == "start"][0]
        # We want to grab the base image used by the start step, as this is known to be pullable from within the cluster,
        # and it might contain the required libraries, allowing us to start up faster.
        start_kube_deco = [
            deco for deco in start_step.decorators if deco.name == "kubernetes"
        ][0]
        resources = dict(start_kube_deco.attributes)
        kube_defaults = dict(start_kube_deco.defaults)

        run_id_template = "argo-{{workflow.name}}"
        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        metaflow_version["production_token"] = self.production_token
        env = {
            # These values are needed by Metaflow to set it's internal
            # state appropriately.
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
            "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_USER": "argo-workflows",
            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            "METAFLOW_OWNER": self.username,
        }
        # pass on the Run pathspec for script
        env["RUN_PATHSPEC"] = f"{self.graph.name}/{run_id_template}"

        # support Metaflow sandboxes
        env["METAFLOW_INIT_SCRIPT"] = KUBERNETES_SANDBOX_INIT_SCRIPT

        env["METAFLOW_WORKFLOW_NAME"] = "{{workflow.name}}"
        env["METAFLOW_WORKFLOW_NAMESPACE"] = "{{workflow.namespace}}"
        env = {
            k: v
            for k, v in env.items()
            if v is not None
            and k not in set(ARGO_WORKFLOWS_ENV_VARS_TO_SKIP.split(","))
        }

        def _cmd(fn_name):
            mflog_expr = export_mflog_env_vars(
                datastore_type=self.flow_datastore.TYPE,
                stdout_path="$PWD/.logs/mflog_stdout",
                stderr_path="$PWD/.logs/mflog_stderr",
                flow_name=self.flow.name,
                run_id=run_id_template,
                step_name=f"_hook_{fn_name}",
                task_id="1",
                retry_count="0",
            )
            cmds = " && ".join(
                [
                    # For supporting sandboxes, ensure that a custom script is executed
                    # before anything else is executed. The script is passed in as an
                    # env var.
                    '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                    "mkdir -p $PWD/.logs",
                    mflog_expr,
                ]
                + self.environment.get_package_commands(
                    self.code_package_url, self.flow_datastore.TYPE
                )[:-1]
                # Replace the line 'Task in starting'
                + [f"mflog 'Lifecycle hook {fn_name} is starting.'"]
                + [
                    f"python -m metaflow.plugins.exit_hook.exit_hook_script {metaflow_version['script']} {fn_name} $RUN_PATHSPEC"
                ]
            )

            cmds = shlex.split('bash -c "%s"' % cmds)
            return cmds

        def _container(cmds):
            return to_camelcase(
                kubernetes_sdk.V1Container(
                    name="main",
                    command=cmds,
                    image=deco.attributes["options"].get("image", None)
                    or resources["image"],
                    env=[
                        kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                        for k, v in env.items()
                    ],
                    env_from=[
                        kubernetes_sdk.V1EnvFromSource(
                            secret_ref=kubernetes_sdk.V1SecretEnvSource(
                                name=str(k),
                                # optional=True
                            )
                        )
                        for k in list(
                            []
                            if not resources.get("secrets")
                            else (
                                [resources.get("secrets")]
                                if isinstance(resources.get("secrets"), str)
                                else resources.get("secrets")
                            )
                        )
                        + KUBERNETES_SECRETS.split(",")
                        + ARGO_WORKFLOWS_KUBERNETES_SECRETS.split(",")
                        if k
                    ],
                    resources=kubernetes_sdk.V1ResourceRequirements(
                        requests={
                            "cpu": str(kube_defaults["cpu"]),
                            "memory": "%sM" % str(kube_defaults["memory"]),
                        }
                    ),
                ).to_dict()
            )

        # create lifecycle hooks from deco
        hooks = []
        for success_fn_name in deco.success_hooks:
            hook = ContainerHook(
                name=f"success-{success_fn_name.replace('_', '-')}",
                container=_container(cmds=_cmd(success_fn_name)),
                service_account_name=resources["service_account"],
                on_success=True,
            )
            hooks.append(hook)

        for error_fn_name in deco.error_hooks:
            hook = ContainerHook(
                name=f"error-{error_fn_name.replace('_', '-')}",
                service_account_name=resources["service_account"],
                container=_container(cmds=_cmd(error_fn_name)),
                on_error=True,
            )
            hooks.append(hook)

        return hooks

    def _exit_hook_templates(self):
        templates = []
        if self.enable_error_msg_capture:
            templates.extend(self._error_msg_capture_hook_templates())

        return templates

    def _error_msg_capture_hook_templates(self):
        from kubernetes import client as kubernetes_sdk

        start_step = [step for step in self.graph if step.name == "start"][0]
        # We want to grab the base image used by the start step, as this is known to be pullable from within the cluster,
        # and it might contain the required libraries, allowing us to start up faster.
        resources = dict(
            [deco for deco in start_step.decorators if deco.name == "kubernetes"][
                0
            ].attributes
        )

        run_id_template = "argo-{{workflow.name}}"
        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        metaflow_version["production_token"] = self.production_token

        mflog_expr = export_mflog_env_vars(
            datastore_type=self.flow_datastore.TYPE,
            stdout_path="$PWD/.logs/mflog_stdout",
            stderr_path="$PWD/.logs/mflog_stderr",
            flow_name=self.flow.name,
            run_id=run_id_template,
            step_name="_run_capture_error",
            task_id="1",
            retry_count="0",
        )

        cmds = " && ".join(
            [
                # For supporting sandboxes, ensure that a custom script is executed
                # before anything else is executed. The script is passed in as an
                # env var.
                '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                "mkdir -p $PWD/.logs",
                mflog_expr,
            ]
            + self.environment.get_package_commands(
                self.code_package_url,
                self.flow_datastore.TYPE,
                self.code_package_metadata,
            )[:-1]
            # Replace the line 'Task in starting'
            # FIXME: this can be brittle.
            + ["mflog 'Error capture hook is starting.'"]
            + ["argo_error=$(python -m 'metaflow.plugins.argo.capture_error')"]
            + ["export METAFLOW_ARGO_ERROR=$argo_error"]
            + [
                """python -c 'import json, os; error_obj=os.getenv(\\"METAFLOW_ARGO_ERROR\\");data=json.loads(error_obj); print(data[\\"message\\"])'"""
            ]
            + [
                'if [ -n \\"${METAFLOW_ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT}\\" ]; then eval \\"${METAFLOW_ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT}\\"; fi'
            ]
        )

        # TODO: Also capture the first failed task id
        cmds = shlex.split('bash -c "%s"' % cmds)
        env = {
            # These values are needed by Metaflow to set it's internal
            # state appropriately.
            "METAFLOW_CODE_METADATA": self.code_package_metadata,
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
            "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_USER": "argo-workflows",
            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            "METAFLOW_OWNER": self.username,
        }
        # support Metaflow sandboxes
        env["METAFLOW_INIT_SCRIPT"] = KUBERNETES_SANDBOX_INIT_SCRIPT
        env["METAFLOW_ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT"] = (
            ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT
        )

        env["METAFLOW_WORKFLOW_NAME"] = "{{workflow.name}}"
        env["METAFLOW_WORKFLOW_NAMESPACE"] = "{{workflow.namespace}}"
        env["METAFLOW_ARGO_WORKFLOW_FAILURES"] = "{{workflow.failures}}"
        env = {
            k: v
            for k, v in env.items()
            if v is not None
            and k not in set(ARGO_WORKFLOWS_ENV_VARS_TO_SKIP.split(","))
        }
        return [
            Template("error-msg-capture-hook")
            .service_account_name(resources["service_account"])
            .container(
                to_camelcase(
                    kubernetes_sdk.V1Container(
                        name="main",
                        command=cmds,
                        image=resources["image"],
                        env=[
                            kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                            for k, v in env.items()
                        ],
                        env_from=[
                            kubernetes_sdk.V1EnvFromSource(
                                secret_ref=kubernetes_sdk.V1SecretEnvSource(
                                    name=str(k),
                                    # optional=True
                                )
                            )
                            for k in list(
                                []
                                if not resources.get("secrets")
                                else (
                                    [resources.get("secrets")]
                                    if isinstance(resources.get("secrets"), str)
                                    else resources.get("secrets")
                                )
                            )
                            + KUBERNETES_SECRETS.split(",")
                            + ARGO_WORKFLOWS_KUBERNETES_SECRETS.split(",")
                            if k
                        ],
                        resources=kubernetes_sdk.V1ResourceRequirements(
                            # NOTE: base resources for this are kept to a minimum to save on running costs.
                            # This has an adverse effect on startup time for the daemon, which can be completely
                            # alleviated by using a base image that has the required dependencies pre-installed
                            requests={
                                "cpu": "200m",
                                "memory": "100Mi",
                            },
                            limits={
                                "cpu": "200m",
                                "memory": "500Mi",
                            },
                        ),
                    ).to_dict()
                )
            ),
            Template("capture-error-hook-fn-preflight").steps(
                [
                    WorkflowStep()
                    .name("capture-error-hook-fn-preflight")
                    .template("error-msg-capture-hook")
                    .when("{{workflow.status}} != Succeeded")
                ]
            ),
        ]

    def _pager_duty_alert_template(self):
        # https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTgx-send-an-alert-event
        if self.notify_pager_duty_integration_key is None:
            return None
        return HttpExitHook(
            name="notify-pager-duty-on-error",
            method="POST",
            url="https://events.pagerduty.com/v2/enqueue",
            headers={"Content-Type": "application/json"},
            body=json.dumps(
                {
                    "event_action": "trigger",
                    "routing_key": self.notify_pager_duty_integration_key,
                    # "dedup_key": self.flow.name,  # TODO: Do we need deduplication?
                    "payload": {
                        "source": "{{workflow.name}}",
                        "severity": "info",
                        "summary": "Metaflow run %s/argo-{{workflow.name}} failed!"
                        % self.flow.name,
                        "custom_details": {
                            "Flow": self.flow.name,
                            "Run ID": "argo-{{workflow.name}}",
                        },
                    },
                    "links": self._pager_duty_notification_links(),
                }
            ),
            on_error=True,
        )

    def _incident_io_alert_template(self):
        if self.notify_incident_io_api_key is None:
            return None
        if self.incident_io_alert_source_config_id is None:
            raise MetaflowException(
                "Creating alerts for errors requires a alert source config ID."
            )
        ui_links = self._incident_io_ui_urls_for_run()
        return HttpExitHook(
            name="notify-incident-io-on-error",
            method="POST",
            url=(
                "https://api.incident.io/v2/alert_events/http/%s"
                % self.incident_io_alert_source_config_id
            ),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.notify_incident_io_api_key,
            },
            body=json.dumps(
                {
                    "idempotency_key": "argo-{{workflow.name}}",  # use run id to deduplicate alerts.
                    "status": "firing",
                    "title": "Flow %s has failed." % self.flow.name,
                    "description": "Metaflow run {run_pathspec} failed!{urls}".format(
                        run_pathspec="%s/argo-{{workflow.name}}" % self.flow.name,
                        urls=(
                            "\n\nSee details for the run at:\n\n"
                            + "\n\n".join(ui_links)
                            if ui_links
                            else ""
                        ),
                    ),
                    "source_url": (
                        "%s/%s/%s"
                        % (
                            UI_URL.rstrip("/"),
                            self.flow.name,
                            "argo-{{workflow.name}}",
                        )
                        if UI_URL
                        else None
                    ),
                    "metadata": {
                        **(self.incident_io_metadata or {}),
                        **{
                            "run_status": "failed",
                            "flow_name": self.flow.name,
                            "run_id": "argo-{{workflow.name}}",
                        },
                    },
                }
            ),
            on_error=True,
        )

    def _incident_io_change_template(self):
        if self.notify_incident_io_api_key is None:
            return None
        if self.incident_io_alert_source_config_id is None:
            raise MetaflowException(
                "Creating alerts for successes requires an alert source config ID."
            )
        ui_links = self._incident_io_ui_urls_for_run()
        return HttpExitHook(
            name="notify-incident-io-on-success",
            method="POST",
            url=(
                "https://api.incident.io/v2/alert_events/http/%s"
                % self.incident_io_alert_source_config_id
            ),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.notify_incident_io_api_key,
            },
            body=json.dumps(
                {
                    "idempotency_key": "argo-{{workflow.name}}",  # use run id to deduplicate alerts.
                    "status": "firing",
                    "title": "Flow %s has succeeded." % self.flow.name,
                    "description": "Metaflow run {run_pathspec} succeeded!{urls}".format(
                        run_pathspec="%s/argo-{{workflow.name}}" % self.flow.name,
                        urls=(
                            "\n\nSee details for the run at:\n\n"
                            + "\n\n".join(ui_links)
                            if ui_links
                            else ""
                        ),
                    ),
                    "source_url": (
                        "%s/%s/%s"
                        % (
                            UI_URL.rstrip("/"),
                            self.flow.name,
                            "argo-{{workflow.name}}",
                        )
                        if UI_URL
                        else None
                    ),
                    "metadata": {
                        **(self.incident_io_metadata or {}),
                        **{
                            "run_status": "succeeded",
                            "flow_name": self.flow.name,
                            "run_id": "argo-{{workflow.name}}",
                        },
                    },
                }
            ),
            on_success=True,
        )

    def _incident_io_ui_urls_for_run(self):
        links = []
        if UI_URL:
            url = "[Metaflow UI](%s/%s/%s)" % (
                UI_URL.rstrip("/"),
                self.flow.name,
                "argo-{{workflow.name}}",
            )
            links.append(url)
        if ARGO_WORKFLOWS_UI_URL:
            url = "[Argo UI](%s/workflows/%s/%s)" % (
                ARGO_WORKFLOWS_UI_URL.rstrip("/"),
                "{{workflow.namespace}}",
                "{{workflow.name}}",
            )
            links.append(url)
        return links

    def _pager_duty_change_template(self):
        # https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTgy-send-a-change-event
        if self.notify_pager_duty_integration_key is None:
            return None
        return HttpExitHook(
            name="notify-pager-duty-on-success",
            method="POST",
            url="https://events.pagerduty.com/v2/change/enqueue",
            headers={"Content-Type": "application/json"},
            body=json.dumps(
                {
                    "routing_key": self.notify_pager_duty_integration_key,
                    "payload": {
                        "summary": "Metaflow run %s/argo-{{workflow.name}} Succeeded"
                        % self.flow.name,
                        "source": "{{workflow.name}}",
                        "custom_details": {
                            "Flow": self.flow.name,
                            "Run ID": "argo-{{workflow.name}}",
                        },
                    },
                    "links": self._pager_duty_notification_links(),
                }
            ),
            on_success=True,
        )

    def _pager_duty_notification_links(self):
        links = []
        if UI_URL:
            links.append(
                {
                    "href": "%s/%s/%s"
                    % (UI_URL.rstrip("/"), self.flow.name, "argo-{{workflow.name}}"),
                    "text": "Metaflow UI",
                }
            )
        if ARGO_WORKFLOWS_UI_URL:
            links.append(
                {
                    "href": "%s/workflows/%s/%s"
                    % (
                        ARGO_WORKFLOWS_UI_URL.rstrip("/"),
                        "{{workflow.namespace}}",
                        "{{workflow.name}}",
                    ),
                    "text": "Argo UI",
                }
            )

        return links

    def _get_slack_blocks(self, message):
        """
        Use Slack's Block Kit to add general information about the environment and
        execution metadata, including a link to the UI and an optional message.
        """
        ui_link = "%s/%s/argo-{{workflow.name}}" % (UI_URL.rstrip("/"), self.flow.name)
        # fmt: off
        if getattr(current, "project_name", None):
            # Add @project metadata when available.
            environment_details_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Environment details"
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "*Project:* %s" % current.project_name
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Project Branch:* %s" % current.branch_name
                    }
                ]
            }
        else:
            environment_details_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Environment details"
                }
            }

        blocks = [
            environment_details_block,
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": " :information_source: *<%s>*" % ui_link,
                    }
                ],
            },
            {
                "type": "divider"
            },
        ]

        if message:
            blocks += [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                }
            ]
        # fmt: on
        return blocks

    def _slack_error_template(self):
        if self.notify_slack_webhook_url is None:
            return None

        message = (
            ":rotating_light: _%s/argo-{{workflow.name}}_ failed!" % self.flow.name
        )
        payload = {"text": message}
        if UI_URL:
            blocks = self._get_slack_blocks(message)
            payload = {"text": message, "blocks": blocks}

        return HttpExitHook(
            name="notify-slack-on-error",
            method="POST",
            url=self.notify_slack_webhook_url,
            body=json.dumps(payload),
            on_error=True,
        )

    def _slack_success_template(self):
        if self.notify_slack_webhook_url is None:
            return None

        message = (
            ":white_check_mark: _%s/argo-{{workflow.name}}_ succeeded!" % self.flow.name
        )
        payload = {"text": message}
        if UI_URL:
            blocks = self._get_slack_blocks(message)
            payload = {"text": message, "blocks": blocks}

        return HttpExitHook(
            name="notify-slack-on-success",
            method="POST",
            url=self.notify_slack_webhook_url,
            body=json.dumps(payload),
            on_success=True,
        )

    def _heartbeat_daemon_template(self):
        # Use all the affordances available to _parameters task
        executable = self.environment.executable("_parameters")
        run_id = "argo-{{workflow.name}}"
        script_name = os.path.basename(sys.argv[0])
        entrypoint = [executable, script_name]
        # FlowDecorators can define their own top-level options. These might affect run level information
        # so it is important to pass these to the heartbeat process as well, as it might be the first task to register a run.
        top_opts_dict = {}
        for deco in flow_decorators(self.flow):
            top_opts_dict.update(deco.get_top_level_options())

        top_level = list(dict_to_cli_options(top_opts_dict)) + [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=%s" % self.flow_datastore.TYPE,
            "--datastore-root=%s" % self.flow_datastore.datastore_root,
            "--event-logger=%s" % self.event_logger.TYPE,
            "--monitor=%s" % self.monitor.TYPE,
            "--no-pylint",
            "--with=argo_workflows_internal:auto-emit-argo-events=%i"
            % self.auto_emit_argo_events,
        ]
        heartbeat_cmds = "{entrypoint} {top_level} argo-workflows heartbeat --run_id {run_id} {tags}".format(
            entrypoint=" ".join(entrypoint),
            top_level=" ".join(top_level) if top_level else "",
            run_id=run_id,
            tags=" ".join(["--tag %s" % t for t in self.tags]) if self.tags else "",
        )

        # TODO: we do not really need MFLOG logging for the daemon at the moment, but might be good for the future.
        # Consider if we can do without this setup.
        # Configure log capture.
        mflog_expr = export_mflog_env_vars(
            datastore_type=self.flow_datastore.TYPE,
            stdout_path="$PWD/.logs/mflog_stdout",
            stderr_path="$PWD/.logs/mflog_stderr",
            flow_name=self.flow.name,
            run_id=run_id,
            step_name="_run_heartbeat_daemon",
            task_id="1",
            retry_count="0",
        )
        # TODO: Can the init be trimmed down?
        # Can we do without get_package_commands fetching the whole code package?
        init_cmds = " && ".join(
            [
                # For supporting sandboxes, ensure that a custom script is executed
                # before anything else is executed. The script is passed in as an
                # env var.
                '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                "mkdir -p $PWD/.logs",
                mflog_expr,
            ]
            + self.environment.get_package_commands(
                self.code_package_url,
                self.flow_datastore.TYPE,
            )[:-1]
            # Replace the line 'Task in starting'
            # FIXME: this can be brittle.
            + ["mflog 'Heartbeat daemon is starting.'"]
        )

        cmd_str = " && ".join([init_cmds, heartbeat_cmds])
        cmds = shlex.split('bash -c "%s"' % cmd_str)

        # Env required for sending heartbeats to the metadata service, nothing extra.
        # prod token / runtime info is required to correctly register flow branches
        env = {
            # These values are needed by Metaflow to set it's internal
            # state appropriately.
            "METAFLOW_CODE_METADATA": self.code_package_metadata,
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
            "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_USER": "argo-workflows",
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
            "METAFLOW_KUBERNETES_WORKLOAD": 1,
            "METAFLOW_KUBERNETES_FETCH_EC2_METADATA": KUBERNETES_FETCH_EC2_METADATA,
            "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
            "METAFLOW_OWNER": self.username,
            "METAFLOW_PRODUCTION_TOKEN": self.production_token,  # Used in identity resolving. This affects system tags.
        }
        # support Metaflow sandboxes
        env["METAFLOW_INIT_SCRIPT"] = KUBERNETES_SANDBOX_INIT_SCRIPT

        # cleanup env values
        env = {
            k: v
            for k, v in env.items()
            if v is not None
            and k not in set(ARGO_WORKFLOWS_ENV_VARS_TO_SKIP.split(","))
        }

        # We want to grab the base image used by the start step, as this is known to be pullable from within the cluster,
        # and it might contain the required libraries, allowing us to start up faster.
        start_step = next(step for step in self.flow if step.name == "start")
        resources = dict(
            [deco for deco in start_step.decorators if deco.name == "kubernetes"][
                0
            ].attributes
        )
        from kubernetes import client as kubernetes_sdk

        return (
            DaemonTemplate("heartbeat-daemon")
            # NOTE: Even though a retry strategy does not work for Argo daemon containers,
            # this has the side-effect of protecting the exit hooks of the workflow from failing in case the daemon container errors out.
            .retry_strategy(10, 1)
            .service_account_name(resources["service_account"])
            .container(
                to_camelcase(
                    kubernetes_sdk.V1Container(
                        name="main",
                        # TODO: Make the image configurable
                        image=resources["image"],
                        command=cmds,
                        env=[
                            kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                            for k, v in env.items()
                        ],
                        env_from=[
                            kubernetes_sdk.V1EnvFromSource(
                                secret_ref=kubernetes_sdk.V1SecretEnvSource(
                                    name=str(k),
                                    # optional=True
                                )
                            )
                            for k in list(
                                []
                                if not resources.get("secrets")
                                else (
                                    [resources.get("secrets")]
                                    if isinstance(resources.get("secrets"), str)
                                    else resources.get("secrets")
                                )
                            )
                            + KUBERNETES_SECRETS.split(",")
                            + ARGO_WORKFLOWS_KUBERNETES_SECRETS.split(",")
                            if k
                        ],
                        resources=kubernetes_sdk.V1ResourceRequirements(
                            # NOTE: base resources for this are kept to a minimum to save on running costs.
                            # This has an adverse effect on startup time for the daemon, which can be completely
                            # alleviated by using a base image that has the required dependencies pre-installed
                            requests={
                                "cpu": "200m",
                                "memory": "100Mi",
                            },
                            limits={
                                "cpu": "200m",
                                "memory": "100Mi",
                            },
                        ),
                    )
                ).to_dict()
            )
        )

    def _compile_sensor(self):
        # This method compiles a Metaflow @trigger decorator into Argo Events Sensor.
        #
        # Event payload is assumed as -
        # ----------------------------------------------------------------------
        # | name                   | name of the event                         |
        # | payload                |                                           |
        # |     parameter name...  |  parameter value                          |
        # |     parameter name...  |  parameter value                          |
        # |     parameter name...  |  parameter value                          |
        # |     parameter name...  |  parameter value                          |
        # ----------------------------------------------------------------------
        #
        #
        #
        # At the moment, every event-triggered workflow template has a dedicated
        # sensor (which can potentially be a bit wasteful in scenarios with high
        # volume of workflows and low volume of events) - introducing a many-to-one
        # sensor-to-workflow-template solution is completely in the realm of
        # possibilities (modulo consistency and transactional guarantees).
        #
        # This implementation side-steps the more prominent/popular usage of event
        # sensors where the sensor is responsible for submitting the workflow object
        # directly. Instead we construct the equivalent behavior of `argo submit
        # --from` to reference an already submitted workflow template. This ensures
        # that Metaflow generated Kubernetes objects can be easily reasoned about.
        #
        # At the moment, Metaflow configures for webhook and NATS event sources. If you
        # are interested in the HA story for either - please follow this link
        # https://argoproj.github.io/argo-events/eventsources/ha/.
        #
        # There is some potential for confusion between Metaflow concepts and Argo
        # Events concepts, particularly for event names. Argo Events EventSource
        # define an event name which is different than the Metaflow event name - think
        # of Argo Events name as a type of event (conceptually like topics in Kafka)
        # while Metaflow event names are a field within the Argo Event.
        #
        #
        # At the moment, there is parity between the labels and annotations for
        # workflow templates and sensors - that may or may not be the case in the
        # future.
        #
        # Unfortunately, there doesn't seem to be a way to create a sensor filter
        # where one (or more) fields across multiple events have the same value.
        # Imagine a scenario where we want to trigger a flow iff both the dependent
        # events agree on the same date field. Unfortunately, there isn't any way in
        # Argo Events (as of apr'23) to ensure that.

        # Nothing to do here - let's short circuit and exit.
        if not self.triggers:
            return {}

        # Ensure proper configuration is available for Argo Events
        if ARGO_EVENTS_EVENT is None:
            raise ArgoWorkflowsException(
                "An Argo Event name hasn't been configured for your deployment yet. "
                "Please see this article for more details on event names - "
                "https://argoproj.github.io/argo-events/eventsources/naming/. "
                "It is very likely that all events for your deployment share the "
                "same name. You can configure it by executing "
                "`metaflow configure kubernetes` or setting METAFLOW_ARGO_EVENTS_EVENT "
                "in your configuration. If in doubt, reach out for support at "
                "http://chat.metaflow.org"
            )
        # Unfortunately argo events requires knowledge of event source today.
        # Hopefully, some day this requirement can be removed and events can be truly
        # impervious to their source and destination.
        if ARGO_EVENTS_EVENT_SOURCE is None:
            raise ArgoWorkflowsException(
                "An Argo Event Source name hasn't been configured for your deployment "
                "yet. Please see this article for more details on event names - "
                "https://argoproj.github.io/argo-events/eventsources/naming/. "
                "You can configure it by executing `metaflow configure kubernetes` or "
                "setting METAFLOW_ARGO_EVENTS_EVENT_SOURCE in your configuration. If "
                "in doubt, reach out for support at http://chat.metaflow.org"
            )
        # Service accounts are a hard requirement since we utilize the
        # argoWorkflow trigger for resource sensors today.
        if ARGO_EVENTS_SERVICE_ACCOUNT is None:
            raise ArgoWorkflowsException(
                "An Argo Event service account hasn't been configured for your "
                "deployment yet. Please see this article for more details on event "
                "names - https://argoproj.github.io/argo-events/service-accounts/. "
                "You can configure it by executing `metaflow configure kubernetes` or "
                "setting METAFLOW_ARGO_EVENTS_SERVICE_ACCOUNT in your configuration. "
                "If in doubt, reach out for support at http://chat.metaflow.org"
            )

        try:
            # Kubernetes is a soft dependency for generating Argo objects.
            # We can very well remove this dependency for Argo with the downside of
            # adding a bunch more json bloat classes (looking at you... V1Container)
            from kubernetes import client as kubernetes_sdk
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import Python package 'kubernetes'. Install kubernetes "
                "sdk (https://pypi.org/project/kubernetes/) first."
            )

        return (
            Sensor()
            .metadata(
                # Sensor metadata.
                ObjectMeta()
                .name(ArgoWorkflows._sensor_name(self.name))
                .namespace(KUBERNETES_NAMESPACE)
                .labels(self._base_labels)
                .label("app.kubernetes.io/name", "metaflow-sensor")
                .annotations(self._base_annotations)
            )
            .spec(
                SensorSpec().template(
                    # Sensor template.
                    SensorTemplate()
                    .metadata(
                        ObjectMeta()
                        .label("app.kubernetes.io/name", "metaflow-sensor")
                        .label("app.kubernetes.io/part-of", "metaflow")
                        .annotations(self._base_annotations)
                    )
                    .container(
                        # Run sensor in guaranteed QoS. The sensor isn't doing a lot
                        # of work so we roll with minimal resource allocation. It is
                        # likely that in subsequent releases we will agressively lower
                        # sensor resources to pack more of them on a single node.
                        to_camelcase(
                            kubernetes_sdk.V1Container(
                                name="main",
                                resources=kubernetes_sdk.V1ResourceRequirements(
                                    requests={
                                        "cpu": "100m",
                                        "memory": "250Mi",
                                    },
                                    limits={
                                        "cpu": "100m",
                                        "memory": "250Mi",
                                    },
                                ),
                            ).to_dict()
                        )
                    )
                    .service_account_name(ARGO_EVENTS_SERVICE_ACCOUNT)
                    # TODO (savin): Handle bypassing docker image rate limit errors.
                )
                # Set sensor replica to 1 for now.
                # TODO (savin): Allow for multiple replicas for HA.
                .replicas(1)
                # TODO: Support revision history limit to manage old deployments
                # .revision_history_limit(...)
                .event_bus_name(ARGO_EVENTS_EVENT_BUS)
                # Workflow trigger.
                .trigger(
                    Trigger().template(
                        TriggerTemplate(self.name)
                        # Trigger a deployed workflow template
                        .argo_workflow_trigger(
                            ArgoWorkflowTrigger()
                            .source(
                                {
                                    "resource": {
                                        "apiVersion": "argoproj.io/v1alpha1",
                                        "kind": "Workflow",
                                        "metadata": {
                                            "generateName": "%s-" % self.name,
                                            "namespace": KUBERNETES_NAMESPACE,
                                            # Useful to paint the UI
                                            "annotations": {
                                                "metaflow/triggered_by": json.dumps(
                                                    [
                                                        {
                                                            key: trigger.get(key)
                                                            for key in ["name", "type"]
                                                        }
                                                        for trigger in self.triggers
                                                    ]
                                                )
                                            },
                                        },
                                        "spec": {
                                            "arguments": {
                                                "parameters": [
                                                    Parameter(parameter["name"])
                                                    .value(parameter["value"])
                                                    .to_json()
                                                    for parameter in self.parameters.values()
                                                ]
                                                # Also consume event data
                                                + [
                                                    Parameter(event["sanitized_name"])
                                                    .value(json.dumps(None))
                                                    .to_json()
                                                    for event in self.triggers
                                                ]
                                            },
                                            "workflowTemplateRef": {
                                                "name": self.name,
                                            },
                                        },
                                    }
                                }
                            )
                            .parameters(
                                [
                                    y
                                    for x in list(
                                        list(
                                            TriggerParameter()
                                            .src(
                                                dependency_name=event["sanitized_name"],
                                                # Technically, we don't need to create
                                                # a payload carry-on and can stuff
                                                # everything within the body.
                                                # NOTE: We need the conditional logic in order to successfully fall back to the default value
                                                # when the event payload does not contain a key for a parameter.
                                                # NOTE: Keys might contain dashes, so use the safer 'get' for fetching the value
                                                data_template='{{ if (hasKey $.Input.body.payload "%s") }}{{- (get $.Input.body.payload "%s" %s) -}}{{- else -}}{{ (fail "use-default-instead") }}{{- end -}}'
                                                % (
                                                    v,
                                                    v,
                                                    (
                                                        "| toRawJson | squote"
                                                        if self.parameters[
                                                            parameter_name
                                                        ]["type"]
                                                        == "JSON"
                                                        else "| toRawJson"
                                                    ),
                                                ),
                                                # Unfortunately the sensor needs to
                                                # record the default values for
                                                # the parameters - there doesn't seem
                                                # to be any way for us to skip
                                                value=(
                                                    json.dumps(
                                                        self.parameters[parameter_name][
                                                            "value"
                                                        ]
                                                    )
                                                    if self.parameters[parameter_name][
                                                        "type"
                                                    ]
                                                    == "JSON"
                                                    else self.parameters[
                                                        parameter_name
                                                    ]["value"]
                                                ),
                                            )
                                            .dest(
                                                # this undocumented (mis?)feature in
                                                # argo-events allows us to reference
                                                # parameters by name rather than index
                                                "spec.arguments.parameters.#(name=%s).value"
                                                % parameter_name
                                            )
                                            for parameter_name, v in event.get(
                                                "parameters", {}
                                            ).items()
                                        )
                                        for event in self.triggers
                                    )
                                    for y in x
                                ]
                                + [
                                    # Map event payload to parameters for current
                                    TriggerParameter()
                                    .src(
                                        dependency_name=event["sanitized_name"],
                                        data_key="body.payload",
                                        value=json.dumps(None),
                                    )
                                    .dest(
                                        "spec.arguments.parameters.#(name=%s).value"
                                        % event["sanitized_name"]
                                    )
                                    for event in self.triggers
                                ]
                            )
                            # Reset trigger conditions ever so often by wiping
                            # away event tracking history on a schedule.
                            # @trigger(options={"reset_at": {"cron": , "timezone": }})
                            # timezone is IANA standard, e.g. America/Los_Angeles
                            # TODO: Introduce "end_of_day", "end_of_hour" ..
                        ).conditions_reset(
                            cron=self.trigger_options.get("reset_at", {}).get("cron"),
                            timezone=self.trigger_options.get("reset_at", {}).get(
                                "timezone"
                            ),
                        )
                    )
                )
                # Event dependencies. As of Mar' 23, Argo Events docs suggest using
                # Jetstream event bus rather than NATS streaming bus since the later
                # doesn't support multiple combos of the same event name and event
                # source name.
                .dependencies(
                    # Event dependencies don't entertain dots
                    EventDependency(event["sanitized_name"]).event_name(
                        ARGO_EVENTS_EVENT
                    )
                    # TODO: Alternatively fetch this from @trigger config options
                    .event_source_name(ARGO_EVENTS_EVENT_SOURCE).filters(
                        # Ensure that event name matches and all required parameter
                        # fields are present in the payload. There is a possibility of
                        # dependency on an event where none of the fields are required.
                        # At the moment, this event is required but the restriction
                        # can be removed if needed.
                        EventDependencyFilter().exprs(
                            [
                                {
                                    "expr": "name == '%s'" % event["name"],
                                    "fields": [
                                        {"name": "name", "path": "body.payload.name"}
                                    ],
                                }
                            ]
                            + [
                                {
                                    "expr": "true == true",  # field name is present
                                    "fields": [
                                        {
                                            "name": "field",
                                            "path": "body.payload.%s" % v,
                                        }
                                    ],
                                }
                                for parameter_name, v in event.get(
                                    "parameters", {}
                                ).items()
                                # only for required parameters
                                if self.parameters[parameter_name]["is_required"]
                            ]
                            + [
                                {
                                    "expr": "field == '%s'" % v,  # trigger_on_finish
                                    "fields": [
                                        {
                                            "name": "field",
                                            "path": "body.payload.%s" % filter_key,
                                        }
                                    ],
                                }
                                for filter_key, v in event.get("filters", {}).items()
                                if v
                            ]
                        )
                    )
                    for event in self.triggers
                )
            )
        )

    def list_to_prose(self, items, singular):
        items = ["*%s*" % item for item in items]
        item_count = len(items)
        plural = singular + "s"
        item_type = singular
        if item_count == 1:
            result = items[0]
        elif item_count == 2:
            result = "%s and %s" % (items[0], items[1])
            item_type = plural
        elif item_count > 2:
            result = "%s and %s" % (
                ", ".join(items[0 : item_count - 1]),
                items[item_count - 1],
            )
            item_type = plural
        else:
            result = ""
        if result:
            result = "%s %s" % (result, item_type)
        return result


# Helper classes to assist with JSON-foo. This can very well replaced with an explicit
# dependency on argo-workflows Python SDK if this method turns out to be painful.
# TODO: Autogenerate them, maybe?


class WorkflowTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["apiVersion"] = "argoproj.io/v1alpha1"
        self.payload["kind"] = "WorkflowTemplate"

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def spec(self, workflow_spec):
        self.payload["spec"] = workflow_spec.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ObjectMeta(object):
    # https://argoproj.github.io/argo-workflows/fields/#objectmeta

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)
        return self

    def generate_name(self, generate_name):
        self.payload["generateName"] = generate_name
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels or {})
        return self

    def name(self, name):
        self.payload["name"] = name
        return self

    def namespace(self, namespace):
        self.payload["namespace"] = namespace
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class WorkflowStep(object):
    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def name(self, name):
        self.payload["name"] = str(name)
        return self

    def template(self, template):
        self.payload["template"] = str(template)
        return self

    def when(self, condition):
        self.payload["when"] = str(condition)
        return self

    def step(self, expression):
        self.payload["expression"] = str(expression)
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class WorkflowSpec(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowspec
    # This object sets all Workflow level properties.

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a workflow in seconds
        if active_deadline_seconds is not None:
            self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def automount_service_account_token(self, mount=True):
        self.payload["automountServiceAccountToken"] = mount
        return self

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def archive_logs(self, archive_logs=True):
        self.payload["archiveLogs"] = archive_logs
        return self

    def entrypoint(self, entrypoint):
        self.payload["entrypoint"] = entrypoint
        return self

    def onExit(self, on_exit_template):
        if on_exit_template:
            self.payload["onExit"] = on_exit_template
        return self

    def parallelism(self, parallelism):
        # Set parallelism at Workflow level
        self.payload["parallelism"] = int(parallelism)
        return self

    def pod_metadata(self, metadata):
        self.payload["podMetadata"] = metadata.to_json()
        return self

    def priority(self, priority):
        if priority is not None:
            self.payload["priority"] = int(priority)
        return self

    def workflow_metadata(self, workflow_metadata):
        self.payload["workflowMetadata"] = workflow_metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        # https://argoproj.github.io/argo-workflows/workflow-rbac/
        self.payload["serviceAccountName"] = service_account_name
        return self

    def templates(self, templates):
        if "templates" not in self.payload:
            self.payload["templates"] = []
        for template in templates:
            self.payload["templates"].append(template.to_json())
        return self

    def hooks(self, hooks):
        # https://argoproj.github.io/argo-workflows/fields/#lifecyclehook
        if "hooks" not in self.payload:
            self.payload["hooks"] = {}
        for k, v in hooks.items():
            self.payload["hooks"].update({k: v.to_json()})
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Metadata(object):
    # https://argoproj.github.io/argo-workflows/fields/#metadata

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels or {})
        return self

    def labels_from(self, labels_from):
        # Only available in workflow_metadata
        # https://github.com/argoproj/argo-workflows/blob/master/examples/label-value-from-workflow.yaml
        if "labelsFrom" not in self.payload:
            self.payload["labelsFrom"] = {}
        for k, v in labels_from.items():
            self.payload["labelsFrom"].update({k: {"expression": v}})
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class DaemonTemplate(object):
    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.name = name
        self.payload = tree()
        self.payload["daemon"] = True
        self.payload["name"] = name

    def container(self, container):
        self.payload["container"] = container
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def retry_strategy(self, times, minutes_between_retries):
        if times > 0:
            self.payload["retryStrategy"] = {
                "retryPolicy": "Always",
                "limit": times,
                "backoff": {"duration": "%sm" % minutes_between_retries},
            }
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Template(object):
    # https://argoproj.github.io/argo-workflows/fields/#template

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a pod in seconds, only obeyed for container templates
        # Used for implementing @timeout.
        self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def dag(self, dag_template):
        self.payload["dag"] = dag_template.to_json()
        return self

    def steps(self, steps):
        if "steps" not in self.payload:
            self.payload["steps"] = []
        # steps is a list of lists.
        # hence we go over every item in the incoming list
        # serialize it and then append the list to the payload
        step_list = []
        for step in steps:
            step_list.append(step.to_json())
        self.payload["steps"].append(step_list)
        return self

    def container(self, container):
        # Luckily this can simply be V1Container and we are spared from writing more
        # boilerplate - https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md.
        self.payload["container"] = container
        return self

    def http(self, http):
        self.payload["http"] = http.to_json()
        return self

    def inputs(self, inputs):
        self.payload["inputs"] = inputs.to_json()
        return self

    def outputs(self, outputs):
        self.payload["outputs"] = outputs.to_json()
        return self

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def metadata(self, metadata):
        self.payload["metadata"] = metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def retry_strategy(self, times, minutes_between_retries):
        if times > 0:
            self.payload["retryStrategy"] = {
                "retryPolicy": "Always",
                "limit": times,
                "backoff": {"duration": "%sm" % minutes_between_retries},
            }
        return self

    def empty_dir_volume(self, name, medium=None, size_limit=None):
        """
        Create and attach an emptyDir volume for Kubernetes.

        Parameters:
        -----------
        name: str
            name for the volume
        size_limit: int (optional)
            sizeLimit (in MiB) for the volume
        medium: str (optional)
            storage medium of the emptyDir
        """
        # Do not add volume if size is zero. Enables conditional chaining.
        if size_limit == 0:
            return self
        # Attach an emptyDir volume
        # https://argoproj.github.io/argo-workflows/empty-dir/
        if "volumes" not in self.payload:
            self.payload["volumes"] = []
        self.payload["volumes"].append(
            {
                "name": name,
                "emptyDir": {
                    # Add default unit as ours differs from Kubernetes default.
                    **({"sizeLimit": "{}Mi".format(size_limit)} if size_limit else {}),
                    **({"medium": medium} if medium else {}),
                },
            }
        )
        return self

    def pvc_volumes(self, pvcs=None):
        """
        Create and attach Persistent Volume Claims as volumes.

        Parameters:
        -----------
        pvcs: Optional[Dict]
            a dictionary of pvc's and the paths they should be mounted to. e.g.
            {"pv-claim-1": "/mnt/path1", "pv-claim-2": "/mnt/path2"}
        """
        if pvcs is None:
            return self
        if "volumes" not in self.payload:
            self.payload["volumes"] = []
        for claim in pvcs.keys():
            self.payload["volumes"].append(
                {"name": claim, "persistentVolumeClaim": {"claimName": claim}}
            )
        return self

    def node_selectors(self, node_selectors):
        if "nodeSelector" not in self.payload:
            self.payload["nodeSelector"] = {}
        if node_selectors:
            self.payload["nodeSelector"].update(node_selectors)
        return self

    def tolerations(self, tolerations):
        self.payload["tolerations"] = tolerations
        return self

    def image_pull_secrets(self, image_pull_secrets):
        self.payload["image_pull_secrets"] = image_pull_secrets
        return self

    def to_json(self):
        return self.payload

    def resource(self, action, manifest, success_criteria, failure_criteria):
        self.payload["resource"] = {}
        self.payload["resource"]["action"] = action
        self.payload["resource"]["setOwnerReference"] = True
        self.payload["resource"]["successCondition"] = success_criteria
        self.payload["resource"]["failureCondition"] = failure_criteria
        self.payload["resource"]["manifest"] = manifest
        return self

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Inputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#inputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Outputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#outputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Parameter(object):
    # https://argoproj.github.io/argo-workflows/fields/#parameter

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def value(self, value):
        self.payload["value"] = value
        return self

    def default(self, value):
        self.payload["default"] = value
        return self

    def valueFrom(self, value_from):
        self.payload["valueFrom"] = value_from
        return self

    def description(self, description):
        self.payload["description"] = description
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def tasks(self, tasks):
        if "tasks" not in self.payload:
            self.payload["tasks"] = []
        for task in tasks:
            self.payload["tasks"].append(task.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTask(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtask

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def dependencies(self, dependencies):
        self.payload["dependencies"] = dependencies
        return self

    def template(self, template):
        # Template reference
        self.payload["template"] = template
        return self

    def inline(self, template):
        # We could have inlined the template here but
        # https://github.com/argoproj/argo-workflows/issues/7432 prevents us for now.
        self.payload["inline"] = template.to_json()
        return self

    def with_param(self, with_param):
        self.payload["withParam"] = with_param
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Arguments(object):
    # https://argoproj.github.io/argo-workflows/fields/#arguments

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Sensor(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.Sensor

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["apiVersion"] = "argoproj.io/v1alpha1"
        self.payload["kind"] = "Sensor"

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def spec(self, sensor_spec):
        self.payload["spec"] = sensor_spec.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class SensorSpec(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.SensorSpec

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def replicas(self, replicas=1):
        # TODO: Make number of deployment replicas configurable.
        self.payload["replicas"] = int(replicas)
        return self

    def template(self, sensor_template):
        self.payload["template"] = sensor_template.to_json()
        return self

    def trigger(self, trigger):
        if "triggers" not in self.payload:
            self.payload["triggers"] = []
        self.payload["triggers"].append(trigger.to_json())
        return self

    def dependencies(self, dependencies):
        if "dependencies" not in self.payload:
            self.payload["dependencies"] = []
        for dependency in dependencies:
            self.payload["dependencies"].append(dependency.to_json())
        return self

    def event_bus_name(self, event_bus_name):
        self.payload["eventBusName"] = event_bus_name
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class SensorTemplate(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.Template

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def container(self, container):
        # Luckily this can simply be V1Container and we are spared from writing more
        # boilerplate - https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md.
        self.payload["container"] = container
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class EventDependency(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.EventDependency

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def event_source_name(self, event_source_name):
        self.payload["eventSourceName"] = event_source_name
        return self

    def event_name(self, event_name):
        self.payload["eventName"] = event_name
        return self

    def filters(self, event_dependency_filter):
        self.payload["filters"] = event_dependency_filter.to_json()
        return self

    def transform(self, event_dependency_transformer=None):
        if event_dependency_transformer:
            self.payload["transform"] = event_dependency_transformer
        return self

    def filters_logical_operator(self, logical_operator):
        self.payload["filtersLogicalOperator"] = logical_operator.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class EventDependencyFilter(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.EventDependencyFilter

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def exprs(self, exprs):
        self.payload["exprs"] = exprs
        return self

    def context(self, event_context):
        self.payload["context"] = event_context
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Trigger(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.Trigger

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def template(self, trigger_template):
        self.payload["template"] = trigger_template.to_json()
        return self

    def parameters(self, trigger_parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for trigger_parameter in trigger_parameters:
            self.payload["parameters"].append(trigger_parameter.to_json())
        return self

    def policy(self, trigger_policy):
        self.payload["policy"] = trigger_policy.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class TriggerTemplate(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.TriggerTemplate

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def argo_workflow_trigger(self, argo_workflow_trigger):
        self.payload["argoWorkflow"] = argo_workflow_trigger.to_json()
        return self

    def conditions_reset(self, cron, timezone):
        if cron:
            self.payload["conditionsReset"] = [
                {"byTime": {"cron": cron, "timezone": timezone}}
            ]
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ArgoWorkflowTrigger(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.ArgoWorkflowTrigger

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["operation"] = "submit"
        self.payload["group"] = "argoproj.io"
        self.payload["version"] = "v1alpha1"
        self.payload["resource"] = "workflows"

    def source(self, source):
        self.payload["source"] = source
        return self

    def parameters(self, trigger_parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for trigger_parameter in trigger_parameters:
            self.payload["parameters"].append(trigger_parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class TriggerParameter(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.TriggerParameter

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def src(self, dependency_name, value, data_key=None, data_template=None):
        self.payload["src"] = {
            "dependencyName": dependency_name,
            "dataKey": data_key,
            "dataTemplate": data_template,
            "value": value,
            # explicitly set it to false to ensure proper deserialization
            "useRawData": False,
        }
        return self

    def dest(self, dest):
        self.payload["dest"] = dest
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)
