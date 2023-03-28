from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from metaflow.util import is_stringish
from metaflow import current

# TODO: Support dynamic parameter mapping through a context object that exposes
#       flow name and user name similar to parameter context


# TODO: This decorator interface can be a top-level decorator to support similar
#       implementations for Netflix Maestro, AWS Step Functions, Apache Airflow...
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
                self.triggers.append({"name": str(self.attributes["event"])})
            elif isinstance(self.attributes["event"], dict):
                if "name" not in dict(self.attributes["event"]):
                    raise MetaflowException(
                        "The *event* attribute for *@trigger* is missing the "
                        "*name* key."
                    )
                self.triggers.append(self.attributes["event"])
            else:
                raise MetaflowException(
                    "Incorrect format for *event* attribute in *@trigger* decorator. "
                    "Supported formats are string and dictionary - \n"
                    "@trigger(event='foo') or @trigger(event={'name': 'foo', "
                    "'parameters': {'alpha': 'beta'}})"
                )
        elif self.attributes["events"]:
            # events attribute supports the following formats -
            #     1. events=[{'name': 'table.prod_db.members',
            #               'parameters': {'alpha': 'member_weight'}},
            #                {'name': 'table.prod_db.metadata',
            #               'parameters': {'beta': 'grade'}}]
            if isinstance(self.attributes["events"], list):
                for event in self.attributes["events"]:
                    if is_stringish(event):
                        self.triggers.append({"name": str(event)})
                    elif isinstance(event, dict):
                        if "name" not in dict(event):
                            raise MetaflowException(
                                "One or more events in *events* attribute for "
                                "*@trigger* are missing the *name* key."
                            )
                        self.triggers.append(event)
                    else:
                        raise MetaflowException(
                            "One or more events in *events* attribute in *@trigger* "
                            "decorator have an incorrect format. Supported format "
                            "is dictionary - \n"
                            "@trigger(events=[{'name': 'foo', 'parameters': {'alpha': "
                            "'beta'}}, {'name': 'bar', 'parameters': "
                            "{'gamma': 'kappa'}}])"
                        )
            else:
                raise MetaflowException(
                    "Incorrect format for *events* attribute in *@trigger* decorator. "
                    "Supported format is list - \n"
                    "@trigger(events=[{'name': 'foo', 'parameters': {'alpha': "
                    "'beta'}}, {'name': 'bar', 'parameters': "
                    "{'gamma': 'kappa'}}])"
                )

        if not self.triggers:
            raise MetaflowException("No event(s) specified in *@trigger* decorator.")

        # same event shouldn't occur more than once
        names = [x["name"] for x in self.triggers]
        if len(names) != len(set(names)):
            raise MetaflowException(
                "Duplicate event names defined in *@trigger* decorator."
            )


class TriggerOnFinishDecorator(FlowDecorator):
    name = "trigger_on_finish"
    defaults = {
        "flow": None,
        "branch": None,
        "project": None,
        "flows": [],
        "options": {},
    }
    options = {
        "trigger": dict(
            multiple=True,
            default=None,
            help="Specify run pathspec for testing @trigger_on_finish locally.",
        ),
    }

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
            # flow supports the format @trigger_on_finish(flow='FooFlow')
            if is_stringish(self.attributes["flow"]):
                self.triggers.append(self.attributes)
            else:
                raise MetaflowException(
                    "Incorrect format for *flow* attribute in *@trigger_on_finish* "
                    " decorator. Supported format is string - \n"
                    "@trigger_on_finish(flow='FooFlow')"
                )
        elif self.attributes["flows"]:
            # flows attribute supports the following formats -
            #     1. flows=['FooFlow', 'BarFlow']
            if isinstance(self.attributes["flows"], list):
                for flow in self.attributes["flows"]:
                    if is_stringish(flow):
                        self.triggers.append(
                            {
                                "flow": flow,
                                "project": self.attributes["project"],
                                "branch": self.attributes["branch"],
                            }
                        )
                    else:
                        raise MetaflowException(
                            "One or more flows in *flows* attribute in "
                            "*@trigger_on_finish* decorator have an incorrect format. "
                            "Supported format is string - \n"
                            "@trigger_on_finish(flows=['FooFlow', 'BarFlow']"
                        )
            else:
                raise MetaflowException(
                    "Incorrect format for *flows* attribute in *@trigger_on_finish* "
                    "decorator. Supported format is list - \n"
                    "@trigger_on_finish(flows=['FooFlow', 'BarFlow']"
                )

        if not self.triggers:
            raise MetaflowException(
                "No flow(s) specified in *@trigger_on_finish* decorator."
            )

        self._option_values = options
        if options["trigger"]:
            print(options)
            # from metaflow import Run
            # triggers = {}
            # for trigger in options["trigger"]:
            #     run_obj = Run(trigger)
            #     triggers.append({
            #         "name": run_obj.flow_name
            #         })

            # current._update_env({"trigger": MetaflowTrigger(triggers)})

    def get_top_level_options(self):
        return list(self._option_values.items())