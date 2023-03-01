import json
import re

from metaflow.exception import MetaflowException
from metaflow.decorators import FlowDecorator
from metaflow import current
from metaflow.util import get_username
from metaflow.decorators import FlowDecorator
from metaflow.util import is_stringish

# TODO (savin) : Ensure that this list is addressed in it's entirety before merging PR
#                1. Support events UX
#                2. Support flows UX
#                3. Clean out error messages


# TODO: At some point, lift this decorator interface to be a top-level decorator since
#       the interface stays consistent for a similar implementation for AWS Step
#       Functions and Airflow.
class ArgoEventsDecorator(FlowDecorator):
    name = "trigger"
    defaults = {
        "event": None,
        "events": [],
        "options": {} # TODO: introduce support for options
    }

    # TODO: Ensure that step-functions and airflow throw a nice unsupported error

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        # TODO: Fix up all error messages so that they pretty print nicely.
        self.events = []
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
                self.events.append({"name": str(self.attributes["event"])})
            elif isinstance(self.attributes["event"], dict):
                if "name" not in dict(self.attributes["event"]):
                    raise MetaflowException(
                        "The *event* attribute for *@trigger* is missing the *name* key."
                    )
                self.events.append(dict(self.attributes["event"]))
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
                    self.events.append({"name": event})
            elif isinstance(self.attributes["events"], list):
                for event in self.attributes["events"]:
                    if is_stringish(event) and str(event).upper() != "AND":
                        self.events.append({"name": str(event)})
                    elif isinstance(event, dict):
                        if "name" not in dict(event):
                            raise MetaflowException(
                                "One or more events in *events* attribute for "
                                "*@trigger* are missing the *name* key."
                            )
                        self.events.append(dict(event))
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
                # TODO: Check that parameters don't conflict
            else:
                raise MetaflowException(
                    "Incorrect format for *events* attribute in *@trigger* decorator. "
                    "Supported format is list or string - \n"
                    "@trigger(events='foo AND bar') or "
                    "@trigger(events=[{'name': 'foo', 'parameters': {'alpha': "
                    "'beta'}},  'AND', {'name': 'bar', 'parameters': "
                    "{'gamma': 'kappa'}}])"
                )
        if not self.events:
            raise MetaflowException("No event(s) specified in *@trigger* decorator.")


class TriggerOnFinishDecorator(FlowDecorator):
    name = "trigger_on_finish"
    defaults = {"flow": None, "branch": None, "project": None, "flows": []}

    # def flow_init(
    #     self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    # ):
    #     self.events = []
    #     if sum(map(bool, (self.attributes["flow"], self.attributes["flows"]))) > 1:
    #         raise MetaflowException("Specify only one of *flow* or *flows* "
    #             "attributes in *@trigger_on_finish* decorator.")
    #     elif self.attributes["flow"]:
