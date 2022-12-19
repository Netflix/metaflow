import json
import re


from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from .eventing import TriggerSet, TriggerInfo

BAD_INPUTS_MSG = """The {0} attribute must be a list of {1} names.

Examples
--------
@{2}({0}=["MyOtherFlow"])
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


class BadTriggerInputsException(MetaflowException):
    headline = "Bad workflow trigger inputs detected"


BAD_AGGREGATE_MAPPING_ERROR = BadEventMappingsException(msg=BAD_AGGREGATE_MAPPING_MSG)

BAD_MAPPING_ERROR = BadEventMappingsException(msg=BAD_MAPPING_MSG)


def bad_inputs_error(field_name, field_type, deco_name):
    return BadTriggerInputsException(
        msg=BAD_INPUTS_MSG.format(field_name, field_type, deco_name, field_name)
    )


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
        "flows": [],
        "events": [],
        "mappings": {},
        "reset": "",
    }
    options = {
        "flows": dict(
            is_flag=False,
            show_default=False,
            help="Trigger the current flow when all named flows complete.",
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
        (flows, events) = self._validate_inputs()
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
            if not is_aggregate and validated is not None:
                info.mappings = {flow: validated}
            else:
                info.mappings = mappings
            self.attributes["trigger_set"].append(info)
        if events is not None:
            for event in events:
                info = TriggerInfo(TriggerInfo.USER_EVENT)
                info.name = event
                if not is_aggregate and validated is not None:
                    info.mappings = {event: validated}
                else:
                    info.mappings = validated
                self.attributes["trigger_set"].append(info)

    def _validate_inputs(self):
        flows = self.attributes.get("flows")
        events = self.attributes.get("events")
        if flows is not None and type(flows) != list:
            raise bad_inputs_error("flows", "flow", self.name)
        if events is not None and type(events) != list:
            raise bad_inputs_error("events", "event", self.name)

        if flows is None or len(flows) == 0:
            if events is None or len(events) == 0:
                self._report_empty_inputs()

        return (flows, events)

    def _report_empty_inputs(self):
        raise MetaflowException(
            msg=("@%s needs at least one flow or event name." % self.name)
        )


class TriggerOnFinishDecorator(TriggerOnDecorator):

    name = "trigger_on_finish"
    defaults = {"flows": [], "reset": ""}
    options = {
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

    def _report_empty_inputs(self):
        raise MetaflowException(msg=("@%s needs at least one flow name." % self.name))
