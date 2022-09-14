import re

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

BAD_EVENT_ERR_MSG = """"The event field is empty. Verify the event name begins with an alphanumeric
character and doesn't begin with the reserved prefix 'metaflow_'.
"""


class BadEventNameException(MetaflowException):
    headline = "Bad or missing event name detected"


class BadEventMappingsException(MetaflowException):
    headline = "Bad event field mappings detected"


def sanitize_user_event(name):
    # NATS uses dots as a subtopic delimiter
    name = name.replace(".", "-")
    name = name.lower()
    # METAFLOW_ and metaflow_ prefixes are reserved
    return re.sub("^metaflow_", "", name)


def validate_mappings(mappings):
    if mappings is None:
        return None
    if type(mappings) != dict:
        raise BadEventMappingsException(
            msg="Event field mappings must be a dict of string keys and "
            + "values mapping event fields to flow parameters."
        )
    for k in mappings.keys():
        v = mappings[k]
        if type(k) != str or type(v) != str:
            raise BadEventMappingsException(
                msg="Event field mappings must be a dict of string keys and "
                + "values mapping event fields to flow parameters."
            )
    return mappings


def validate_data(deco_name, data):
    for key in data.keys():
        if type(key) != str:
            raise MetaflowException(
                msg="@%s requires data to be a dict with string keys: %s"
                % (deco_name, str(key))
            )
    return data


class TriggerOnDecorator(FlowDecorator):

    name = "trigger_on"
    defaults = {
        "flow": None,
        "status": "succeeded",
        "event": None,
        "data": None,
        "mappings": None,
    }
    options = {
        "flow": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when this flow completes.",
        ),
        "event": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the flow when this event is received",
        ),
        "status": dict(
            is_flag=False,
            show_default=True,
            help="Status of triggering flow. Valid values are 'succeeded', 'failed'",
        ),
        "mappings": dict(
            is_flag=False,
            show_default=True,
            help="Mapping of event fields to flow parameters",
        ),
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if are_events_configured():
            self._option_values = options
            flow_name = self.attributes.get("flow")
            event_name = self.attributes.get("event")
            if flow_name in [None, ""] and event_name in [None, ""]:
                raise MetaflowException(
                    msg=("@%s needs a flow or event name." % self.name)
                )
            trigger_on = {
                "flow": flow_name,
                "status": None,
                "event": event_name,
                "mappings": None,
            }
            if flow_name is not None:
                status = self.attributes.get("status").lower()
                if status not in valid_statuses():
                    raise MetaflowException(
                        msg=(
                            "@%s requires status to be one of: %s."
                            % (self.name, ", ".join(valid_statuses()))
                        )
                    )
                trigger_on["status"] = status
            elif event_name is not None:
                sanitized = sanitize_user_event(event_name)
                if sanitized == "":
                    raise BadEventNameException(BAD_EVENT_ERR_MSG)
                trigger_on["event"] = sanitized
            mappings = self.attributes.get("mappings")
            trigger_on["mappings"] = validate_mappings(mappings)
            current._update_env({"trigger_on": trigger_on})

        else:
            # Defer raising an error in case user has specified --ignore-triggers
            current._update_env(
                {
                    "event_decorator_error": {
                        "message": ERR_MSG,
                        "headline": ("@%s requires eventing support" % self.name),
                    }
                }
            )


class EmitEventDecorator(StepDecorator):

    name = "emit_event"
    defaults = {"event": None, "task_status": "succeeded", "data": {}}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.attributes.get("task_status") not in valid_statuses():
            raise MetaflowException(
                msg=(
                    "@%s requires task_status to be one of: %s."
                    % (self.name, ", ".join(valid_statuses()))
                )
            )
        event_name = self.attributes.get("event")
        if event_name in [None, ""]:
            raise BadEventNameException("@%s needs an event" % self.name)
        event_name = sanitize_user_event(event_name)
        if event_name == "":
            raise BadEventNameException(BAD_EVENT_ERR_MSG)
        self.attributes["event"] = event_name
        data = self.attributes.get("data")
        if data is not None:
            validated = validate_data(self.name, data)
            self.attributes["data"] = validated

    def formatted_status(self):
        status = self.attributes.get("task_status")
        if status is None:
            return None
        return status.capitalize()

    @property
    def event(self):
        return self.attributes["event"]

    @property
    def data(self):
        return self.attributes.get("data")


class AnnotateLifecycleDecorator(FlowDecorator):

    name = "annotate_lifecycle"
    defaults = {"status": "all", "data": {}}

    options = {
        "status": dict(
            is_flag=False,
            show_default=True,
            help="Name of lifecycle event to annotate. Valid values are 'all', 'succeeded', 'failed'",
        ),
        "data": dict(
            is_flag=False,
            show_default=True,
            help="Extra data used to annotate the lifecycle event",
        ),
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        if are_events_configured():
            self._option_values = options
            status = self.attributes.get("status")
            data = self.attributes.get("data")
            if status is None:
                raise MetaflowException("@%s needs a status" % self.name)
            if data is None:
                raise MetaflowException("@%s needs annotation data" % self.name)
            if status not in valid_statuses() and self.status != "all":
                raise MetaflowException(
                    msg=(
                        "@%s requires status to be one of: %s."
                        % (self.name, ", ".join(valid_statuses().append("all")))
                    )
                )
            if type(data) != dict:
                raise MetaflowException(
                    msg=("@%s requires annotation data to be a map" % self.name)
                )
            validated = validate_data(self.name, data)
            if status == "all":
                statuses = ["succeeded", "failed"]
            else:
                statuses = [status]
            current._update_env(
                {"lifecycle_annotation": {"statuses": statuses, "data": validated}}
            )
        else:
            # Defer raising an error in case user has specified --ignore-triggers
            current._update_env(
                {
                    "event_decorator_error": {
                        "message": ERR_MSG,
                        "headline": ("@%s requires eventing support" % self.name),
                    }
                }
            )
