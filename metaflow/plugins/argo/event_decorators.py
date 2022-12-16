import json
import re


from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from .eventing import TriggerSet, TriggerInfo

ERR_MSG = """Make sure configuration entries METAFLOW_EVENT_SOURCE_NAME, METAFLOW_EVENT_SOURCE_URL, and 
METAFLOW_EVENT_SERVICE_ACCOUNT are correct.

Authentication (Optional)
=========================
If authentication is required and credentials are stored in a Kubernetes secret, check
METAFLOW_EVENT_AUTH_SECRET and METAFLOW_EVENT_AUTH_KEY.

If an authentication token is used, verify METAFLOW_EVENT_AUTH_TOKEN has the correct value.
"""

BAD_AGGREGATE_MAPPING_MSG = """Event field mappings for multiple events or flows must be a dict of
string keys corresponding to an event or flow name and values of another
dict of string keys and values mapping flow parameters to event fields.

Examples
--------
@trigger_on(flows=["FirstFlow", "SecondFlow"], mappings={"FirstFlow": {"alpha": "alpha_value"}, 
                                                         "SecondFlow": {"delta": "delta_value"}})

@trigger_on(events=["first", "second"], mappings={"first": {"alpha": "alpha_value"}, 
                                                  "second": {"delta": "delta_value"}})                                                         
"""

BAD_MAPPING_MSG = """Event field mappings for a single event or flow must be a dict of
string keys and values mapping flow parameters to event fields.

Examples
--------
@trigger_on(flow="FirstFlow", mappings={"alpha": "alpha_value", "delta": "delta_value"})

@trigger_on(event="first", mappings={"alpha": "alpha_value", "delta": "delta_value"})
"""


class BadEventNameException(MetaflowException):
    headline = "Bad or missing event name detected"


class BadEventMappingsException(MetaflowException):
    headline = "Bad event field mappings detected"


BAD_AGGREGATE_MAPPING_ERROR = BadEventMappingsException(msg=BAD_AGGREGATE_MAPPING_MSG)

BAD_MAPPING_ERROR = BadEventMappingsException(msg=BAD_MAPPING_MSG)


def validate_mappings(mappings, aggregate):
    if mappings is None:
        return None
    if type(mappings) != dict:
        if aggregate:
            raise BAD_AGGREGATE_MAPPING_ERROR
        else:
            raise BAD_MAPPING_ERROR
    for k in mappings.keys():
        if type(k) != str:
            if aggregate:
                raise BAD_AGGREGATE_MAPPING_ERROR
            else:
                raise BAD_MAPPING_ERROR
        v = mappings[k]
        if aggregate:
            if type(v) != dict:
                raise BAD_AGGREGATE_MAPPING_ERROR
            for k1 in v.keys():
                v1 = v[k1]
                if type(k1) != str or type(v1) != str:
                    raise BAD_AGGREGATE_MAPPING_ERROR
        else:
            if type(v) != str:
                raise BAD_MAPPING_ERROR
    return mappings


class TriggerOnDecorator(FlowDecorator):

    name = "trigger_on"
    defaults = {
        "flow": None,
        "flows": [],
        "event": None,
        "events": [],
        "data": None,
        "mappings": {},
        "reset": "",
    }
    options = {
        "flow": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when the named flow completes.",
        ),
        "flows": dict(
            is_flag=False,
            show_default=False,
            help="Trigger the current flow when all named flows complete.",
        ),
        "event": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when the named user event is received.",
        ),
        "events": dict(
            is_flag=False,
            show_default=False,
            help="Trigger the current flow when all user events are received.",
        ),
        "mappings": dict(
            is_flag=False,
            show_default=True,
            help="Mapping of flow parameters to event fields.",
        ),
        "reset": dict(
            is_flag=False,
            show_default=True,
            help="Reset event trigger state after an elapsed interval or at a specific time (depending on the orchestrator)",
        ),
    }

    def flow_init(
        self,
        flow,
        graph,
        environment,
        flow_datastore,
        metadata,
        logger,
        echo,
        options,
        **kwarg
    ):
        self.attributes["trigger_set"] = None
        self.attributes["error"] = None
        self._option_values = options
        (flows, events) = self._read_inputs()
        project_name = None
        branch_name = None
        # @project has already been evaluated
        if "project_name" in current:
            project_name = current.get("project_name")
            branch_name = current.get("branch_name")
        else:
            if flow._flow_decorators.get("project"):
                raise MetaflowException(
                    "Move @project below @{}. ".format(self.name)
                    + "Project namespacing must be applied before triggers are built."
                )
        self.attributes["trigger_set"] = TriggerSet(
            project_name, branch_name, self.attributes.get("reset", "")
        )
        mappings = self.attributes.get("mappings")
        is_aggregate = (len(flows) > 1) or (
            events is not None and len(flows) + len(events) > 1
        )
        validated = validate_mappings(mappings, is_aggregate)
        for flow in flows:
            info = TriggerInfo(TriggerInfo.LIFECYCLE_EVENT)
            info.name = flow
            info.status = "succeeded"
            if not is_aggregate:
                info.mappings = {flow: validated}
            else:
                info.mappings = mappings
            self.attributes["trigger_set"].append(info)
        if events is not None:
            for event in events:
                info = TriggerInfo(TriggerInfo.USER_EVENT)
                info.name = event
                if not is_aggregate:
                    info.mappings = {event: validated}
                else:
                    info.mappings = validated
                self.attributes["trigger_set"].append(info)

    def _read_inputs(self):
        self._fix_plurals()
        flow_name = self.attributes.get("flow")
        if flow_name == "":
            flow_name = None
        flow_names = self.attributes.get("flows")
        event_name = self.attributes.get("event")
        if event_name == "":
            event_name = None
        event_names = self.attributes.get("events")
        if (
            flow_name is None
            and len(flow_names) == 0
            and event_name is None
            and len(event_names) == 0
        ):
            raise MetaflowException(
                msg=("@%s needs at least one flow or event name." % self.name)
            )

        if flow_name is not None:
            flow_names.append(flow_name)
        if event_name is not None:
            event_names.append(event_name)
        return (flow_names, event_names)

    def _fix_plurals(self):
        flow_name = self.attributes.get("flow")
        if flow_name == "":
            flow_name = None
        flow_names = self.attributes.get("flows")
        if type(flow_names) == str and flow_name is None:
            self.attributes["flow"] = flow_names
            self.attributes["flows"] = []

        event_name = self.attributes.get("event")
        if event_name == "":
            event_name = None
        event_names = self.attributes.get("events")
        if type(event_names) == str and event_name is None:
            self.attributes["event"] = event_names
            self.attributes["events"] = []


class TriggerOnFinishDecorator(TriggerOnDecorator):

    name = "trigger_on_finish"
    defaults = {"flow": None, "flows": [], "reset": ""}
    options = {
        "flow": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when the named flow completes.",
        ),
        "flows": dict(
            is_flag=False,
            show_default=False,
            help="Trigger the current flow when all named flows complete.",
        ),
        "reset": dict(
            is_flag=False,
            show_default=True,
            help="Reset event trigger state after an elapsed interval or at a specific time (depending on the orchestrator)",
        ),
    }
