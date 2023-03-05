import json
import re

from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from metaflow.util import get_username, is_stringish

# TODO: Support dynamic parameter mapping through a context object that exposes
#       flow name and user name similar to parameter context


# TODO: At some point, lift this decorator interface to be a top-level decorator since
#       the interface stays consistent for a similar implementation for AWS Step
#       Functions and Airflow.
class ArgoEventsDecorator(FlowDecorator):
    name = "trigger"
    defaults = {
        "event": None,
        "events": [],
        "options": {},  # TODO: introduce support for options
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        # TODO: Fix up all error messages so that they pretty print nicely.
        # TODO: Check if parameters indeed exist for the mapping.
        self.triggers = []
        if sum(map(bool, (self.attributes["event"], self.attributes["events"]))) > 1:
            raise MetaflowException(
                "Specify only one of *event* or *events* "
                "attributes in *@trigger* decorator."
            )
        elif self.attributes["event"]:
            # event attribute supports the following formats -
            #     1. event='table.prod_db.members'
            #     2. event={'name': 'table.prod_db.members',
            #               'parameters': {'alpha': 'member_weight'}}
            if is_stringish(self.attributes["event"]):
                self.triggers.append(
                    {"name": str(self.attributes["event"]), "type": "event"}
                )
            elif isinstance(self.attributes["event"], dict):
                if "name" not in dict(self.attributes["event"]):
                    raise MetaflowException(
                        "The *event* attribute for *@trigger* is missing the *name* key."
                    )
                self.triggers.append(
                    dict(**self.attributes["event"], **{"type": "event"})
                )
            else:
                raise MetaflowException(
                    "Incorrect format for *event* attribute in *@trigger* decorator. "
                    "Supported formats are string and dictionary - \n"
                    "@trigger(event='foo') or @trigger(event={'name': 'foo', "
                    "'parameters': {'alpha': 'beta'}})"
                )
        elif self.attributes["events"]:
            # events attribute supports the following formats -
            #     1. events='table.prod_db.members AND table.prod_db.metadata'
            #     2. events=[{'name': 'table.prod_db.members',
            #               'parameters': {'alpha': 'member_weight'}},
            #               'AND', -- optional - if omitted, default is AND
            #                {'name': 'table.prod_db.metadata',
            #               'parameters': {'beta': 'grade'}}]
            if is_stringish(self.attributes["events"]):
                for event in str(self.attributes["events"]).split(" AND "):
                    self.triggers.append({"name": event, "type": "event"})
            elif isinstance(self.attributes["events"], list):
                for event in self.attributes["events"]:
                    if is_stringish(event) and str(event).upper() != "AND":
                        self.triggers.append({"name": str(event), "type": "event"})
                    elif isinstance(event, dict):
                        if "name" not in dict(event):
                            raise MetaflowException(
                                "One or more events in *events* attribute for "
                                "*@trigger* are missing the *name* key."
                            )
                        self.triggers.append(dict(event, **{"type": "event"}))
                    else:
                        raise MetaflowException(
                            "One or more events in *events* attribute in *@trigger* "
                            "decorator have an incorrect format. Supported formats "
                            "are string and dictionary - \n"
                            "@trigger(events='foo AND bar') or "
                            "@trigger(events=[{'name': 'foo', 'parameters': {'alpha': "
                            "'beta'}},  'AND', {'name': 'bar', 'parameters': "
                            "{'gamma': 'kappa'}}])"
                        )
                # TODO: Check that parameters don't conflict and are present in flow.
            else:
                raise MetaflowException(
                    "Incorrect format for *events* attribute in *@trigger* decorator. "
                    "Supported format is list or string - \n"
                    "@trigger(events='foo AND bar') or "
                    "@trigger(events=[{'name': 'foo', 'parameters': {'alpha': "
                    "'beta'}},  'AND', {'name': 'bar', 'parameters': "
                    "{'gamma': 'kappa'}}])"
                )
        if not self.triggers:
            raise MetaflowException("No event(s) specified in *@trigger* decorator.")


class TriggerOnFinishDecorator(FlowDecorator):
    name = "trigger_on_finish"
    defaults = {"flow": None, "branch": None, "project": None, "flows": []}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.triggers = []
        if sum(map(bool, (self.attributes["flow"], self.attributes["flows"]))) > 1:
            raise MetaflowException(
                "Specify only one of *flow* or *flows* "
                "attributes in *@trigger_on_finish* decorator."
            )
        elif self.attributes["flow"]:
            list(
                filter(
                    lambda item: item is not None,
                    [
                        "metaflow",
                        self.attributes.get("project", current.get("project_name")),
                        self.attributes.get("branch", current.get("branch_name")),
                        self.attributes["flow"],
                    ],
                )
            )
            self.triggers.append(
                # Trigger on metaflow.project.branch.flow.end event
                {
                    "name": ".".join(
                        list(
                            filter(
                                lambda item: item is not None,
                                [
                                    "metaflow",
                                    self.attributes.get(
                                        "project", current.get("project_name")
                                    ),
                                    self.attributes.get(
                                        "branch", current.get("branch_name")
                                    ),
                                    self.attributes["flow"],
                                    "end",
                                ],
                            )
                        )
                    ),
                    "filters": {
                        "auto-generated-by-metaflow": True,
                        # TODO: Add a time based filter to guard against cached events
                    },
                    "type": "run",
                }
            )
