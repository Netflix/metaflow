import json
import re

from metaflow.exception import MetaflowException
from metaflow.decorators import FlowDecorator
from metaflow import current
from metaflow.util import get_username
from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.util import is_stringish


# TODO: At some point, lift this decorator interface to be a top-level decorator since
#       the interface stays consistent for a similar implementation for AWS Step
#       Functions and Airflow.
class ArgoEventsDecorator(FlowDecorator):
    name = "trigger"
    defaults = {
        # TODO (savin): Introduce support for flow-dependencies.
        "event": None,
        "events": [],
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.events = []
        if self.attributes["event"]:
            # event attribute supports the following formats -
            #     1. event='table.prod_db.members'
            #     2. event={'name': 'table.prod_db.members',
            #               'parameters': {'alpha': 'member_weight'}}
            if is_stringish(self.attributes["event"]):
                self.events.append({"name": self.attributes["event"]})
            elif isinstance(self.attributes["event"], dict):
                if "name" not in dict(self.attributes["event"]):
                    raise MetaflowException(
                        "The event attribute for @trigger is missing the name key."
                    )
                self.events.append(dict(self.attributes["event"]))
            else:
                raise MetaflowException(
                    "Incorrect format for event attribute in *@trigger* decorator. "
                    "Supported formats are string and dictionary - \n@trigger("
                    "event='table.prod_db.members') or "
                    "@trigger(event={'name': 'table.prod_db.members', 'parameters': {'alpha': 'member_weight'}})"
                )
