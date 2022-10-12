import json
import re

from metaflow import current
from metaflow.decorators import FlowDecorator, StepDecorator, flow_decorators
from metaflow.exception import MetaflowException
from .util import are_events_configured

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
    }
    options = {
        "flow": dict(
            is_flag=False,
            show_default=True,
            help="Trigger the current flow when this flow completes.",
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
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.attributes["trigger_set"] = None
        self.attributes["error"] = None
        if are_events_configured():
            self._option_values = options
            (flows, events) = self._read_inputs()
            if "project_name" in current:
                self.attributes["trigger_set"] = TriggerSet(
                    current.project_name, current.branch_name
                )
            else:
                self.attributes["trigger_set"] = TriggerSet(None, None)
            mappings = self.attributes.get("mappings")
            is_aggregate = (len(flows) + len(events)) > 1
            validated = validate_mappings(mappings, is_aggregate)
            for flow in flows:
                info = TriggerInfo(TriggerInfo.LIFECYCLE_EVENT)
                info.name = flow
                info.status = "succeeded"
                info.mappings = validated
                self.attributes["trigger_set"].append(info)
            for event in events:
                info = TriggerInfo(TriggerInfo.USER_EVENT)
                info.name = event
                info.mappings = validated
                self.attributes["trigger_set"].append(info)

        else:
            # Defer raising an error in case user has specified --ignore-triggers
            self.error = {
                "event_decorator_error": {
                    "message": ERR_MSG,
                    "headline": ("@%s requires eventing support" % self.name),
                }
            }

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


class TriggerSet:
    def __init__(self, project, branch):
        project_decorator = None
        self._project = project
        self._branch = branch
        self.triggers = []

    def append(self, trigger_info):
        trigger_info.add_namespacing(self._project, self._branch)
        self.triggers.append(trigger_info)

    def add_namespacing(self, project, branch):
        self._project = project
        self._branch = branch
        for t in self.triggers:
            t.add_namespacing(self._project, self._branch)

    def is_empty(self):
        return len(self.triggers) == 0

    def __len__(self):
        return len(self.triggers)


class TriggerInfo:

    LIFECYCLE_EVENT = 0
    USER_EVENT = 1

    def __init__(self, type):
        self.type = type
        self._project = None
        self._branch = None
        self._name = None
        self.status = None
        self.mappings = {}

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name

    @property
    def formatted_name(self):
        if self._project is not None and self._branch is not None:
            formatted = "-".join(
                [self._project.replace("_", ""), self._branch, self._name]
            ).lower()
        else:
            formatted = self._name.lower()
        return formatted.replace(".", "-")

    def add_namespacing(self, project, branch):
        self._project = project
        self._branch = branch

    def has_mappings(self):
        return self._name in self.mappings

    def __str__(self):
        data = {
            "type": self.type,
            "name": self._formatted_name,
            "original_name": self._name,
            "status": self.status,
            "mappings": len(self.mappings),
        }
        return json.dumps(data, indent=4)
