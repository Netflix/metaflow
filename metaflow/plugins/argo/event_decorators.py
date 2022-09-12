from metaflow import current
from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.exception import MetaflowException
from .util import are_events_configured, valid_statuses

ERR_MSG = """Make sure configuration entries METAFLOW_EVENT_SOURCE_NAME, METAFLOW_EVENT_SOURCE_URL, and 
METAFLOW_EVENT_SERVICE_ACCOUNT are correct.

Authentication (Optional)
=========================
If authentication is required and credentials are stored in a Kubernetes secret, check
METAFLOW_EVENT_AUTH_SECRET and METAFLOW_EVENT_AUTH_KEY.

If an authentication token is used, verify METAFLOW_EVENT_AUTH_TOKEN has the correct value.
"""


class TriggerOnDecorator(FlowDecorator):

    name = "trigger_on"
    defaults = {"flow": None, "status": "succeeded"}
    options = {
        "flow": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when this flow completes.",
        ),
        "status": dict(
            is_flag=False,
            show_default=True,
            help="Status of triggering flow. Valid values are 'succeeded', 'failed'",
        ),
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if are_events_configured():
            self._option_values = options
            flow_name = self.attributes.get("flow")
            if flow_name is None:
                raise MetaflowException(msg="@trigger_on needs a flow.")
            status = self.attributes.get("status").lower()
            if status not in valid_statuses():
                raise MetaflowException(
                    msg=(
                        "@trigger_on requires status to be one of: %s."
                        % (", ".join(valid_statuses()))
                    )
                )
            # @project use is detected and flow name altered to fit
            # when Argo Workflow templates are generated
            current._update_env({"trigger_on": {"flow": flow_name, "status": status}})
        else:
            # Defer raising an error in case user has specified --ignore-triggers
            current._update_env(
                {
                    "trigger_on_error": {
                        "message": ERR_MSG,
                        "headline": "@trigger_on requires eventing support",
                    }
                }
            )
