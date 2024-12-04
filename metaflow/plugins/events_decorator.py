import re
import json

from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from metaflow.util import is_stringish
from metaflow.parameters import DeployTimeField, deploy_time_eval

# TODO: Support dynamic parameter mapping through a context object that exposes
#       flow name and user name similar to parameter context


class TriggerDecorator(FlowDecorator):
    """
    Specifies the event(s) that this flow depends on.

    ```
    @trigger(event='foo')
    ```
    or
    ```
    @trigger(events=['foo', 'bar'])
    ```

    Additionally, you can specify the parameter mappings
    to map event payload to Metaflow parameters for the flow.
    ```
    @trigger(event={'name':'foo', 'parameters':{'flow_param': 'event_field'}})
    ```
    or
    ```
    @trigger(events=[{'name':'foo', 'parameters':{'flow_param_1': 'event_field_1'},
                     {'name':'bar', 'parameters':{'flow_param_2': 'event_field_2'}])
    ```

    'parameters' can also be a list of strings and tuples like so:
    ```
    @trigger(event={'name':'foo', 'parameters':['common_name', ('flow_param', 'event_field')]})
    ```
    This is equivalent to:
    ```
    @trigger(event={'name':'foo', 'parameters':{'common_name': 'common_name', 'flow_param': 'event_field'}})
    ```

    Parameters
    ----------
    event : Union[str, Dict[str, Any]], optional, default None
        Event dependency for this flow.
    events : List[Union[str, Dict[str, Any]]], default []
        Events dependency for this flow.
    options : Dict[str, Any], default {}
        Backend-specific configuration for tuning eventing behavior.

    MF Add To Current
    -----------------
    trigger -> metaflow.events.Trigger
        Returns `Trigger` if the current run is triggered by an event

        @@ Returns
        -------
        Trigger
            `Trigger` if triggered by an event
    """

    name = "trigger"
    defaults = {
        "event": None,
        "events": [],
        "options": {},
    }

    def process_event_name(self, event):
        if is_stringish(event):
            return {"name": str(event)}
        elif isinstance(event, dict):
            if "name" not in event:
                raise MetaflowException(
                    "The *event* attribute for *@trigger* is missing the *name* key."
                )
            if callable(event["name"]) and not isinstance(
                event["name"], DeployTimeField
            ):
                event["name"] = DeployTimeField(
                    "event_name", str, None, event["name"], False
                )
            event["parameters"] = self.process_parameters(event.get("parameters", {}))
            return event
        elif callable(event) and not isinstance(event, DeployTimeField):
            return DeployTimeField("event", [str, dict], None, event, False)
        else:
            raise MetaflowException(
                "Incorrect format for *event* attribute in *@trigger* decorator. "
                "Supported formats are string and dictionary - \n"
                "@trigger(event='foo') or @trigger(event={'name': 'foo', "
                "'parameters': {'alpha': 'beta'}})"
            )

    def process_parameters(self, parameters):
        new_param_values = {}
        if isinstance(parameters, (list, tuple)):
            for mapping in parameters:
                if is_stringish(mapping):
                    new_param_values[mapping] = mapping
                elif callable(mapping) and not isinstance(mapping, DeployTimeField):
                    mapping = DeployTimeField(
                        "parameter_val", str, None, mapping, False
                    )
                    new_param_values[mapping] = mapping
                elif isinstance(mapping, (list, tuple)) and len(mapping) == 2:
                    if callable(mapping[0]) and not isinstance(
                        mapping[0], DeployTimeField
                    ):
                        mapping[0] = DeployTimeField(
                            "parameter_val", str, None, mapping[0], False
                        )
                    if callable(mapping[1]) and not isinstance(
                        mapping[1], DeployTimeField
                    ):
                        mapping[1] = DeployTimeField(
                            "parameter_val", str, None, mapping[1], False
                        )
                    new_param_values[mapping[0]] = mapping[1]
                else:
                    raise MetaflowException(
                        "The *parameters* attribute for event is invalid. "
                        "It should be a list/tuple of strings and lists/tuples of size 2"
                    )
        elif callable(parameters) and not isinstance(parameters, DeployTimeField):
            return DeployTimeField(
                "parameters", [list, dict, tuple], None, parameters, False
            )
        elif isinstance(parameters, dict):
            for key, value in parameters.items():
                if callable(key) and not isinstance(key, DeployTimeField):
                    key = DeployTimeField("flow_parameter", str, None, key, False)
                if callable(value) and not isinstance(value, DeployTimeField):
                    value = DeployTimeField("signal_parameter", str, None, value, False)
                new_param_values[key] = value
        return new_param_values

    def flow_init(
        self,
        flow_name,
        graph,
        environment,
        flow_datastore,
        metadata,
        logger,
        echo,
        options,
    ):
        self.triggers = []
        if sum(map(bool, (self.attributes["event"], self.attributes["events"]))) > 1:
            raise MetaflowException(
                "Specify only one of *event* or *events* "
                "attributes in *@trigger* decorator."
            )
        elif self.attributes["event"]:
            event = self.attributes["event"]
            processed_event = self.process_event_name(event)
            self.triggers.append(processed_event)
        elif self.attributes["events"]:
            # events attribute supports the following formats -
            #     1. events=[{'name': 'table.prod_db.members',
            #               'parameters': {'alpha': 'member_weight'}},
            #                {'name': 'table.prod_db.metadata',
            #               'parameters': {'beta': 'grade'}}]
            if isinstance(self.attributes["events"], list):
                # process every event in events
                for event in self.attributes["events"]:
                    processed_event = self.process_event_name(event)
                    self.triggers.append("processed event", processed_event)
            elif callable(self.attributes["events"]) and not isinstance(
                self.attributes["events"], DeployTimeField
            ):
                trig = DeployTimeField(
                    "events", list, None, self.attributes["events"], False
                )
                self.triggers.append(trig)
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
        names = [
            x["name"]
            for x in self.triggers
            if not isinstance(x, DeployTimeField)
            and not isinstance(x["name"], DeployTimeField)
        ]
        if len(names) != len(set(names)):
            raise MetaflowException(
                "Duplicate event names defined in *@trigger* decorator."
            )

        self.options = self.attributes["options"]

        # TODO: Handle scenario for local testing using --trigger.

    def format_deploytime_value(self):
        new_triggers = []
        for trigger in self.triggers:
            # Case where trigger is a function that returns a list of events
            # Need to do this bc we need to iterate over list later
            if isinstance(trigger, DeployTimeField):
                evaluated_trigger = deploy_time_eval(trigger)
                if isinstance(evaluated_trigger, dict):
                    trigger = evaluated_trigger
                elif isinstance(evaluated_trigger, str):
                    trigger = {"name": evaluated_trigger}
                if isinstance(evaluated_trigger, list):
                    for trig in evaluated_trigger:
                        if is_stringish(trig):
                            new_triggers.append({"name": trig})
                        else:  # dict or another deploytimefield
                            new_triggers.append(trig)
                else:
                    new_triggers.append(trigger)
            else:
                new_triggers.append(trigger)

        self.triggers = new_triggers
        for trigger in self.triggers:
            old_trigger = trigger
            trigger_params = trigger.get("parameters", {})
            # Case where param is a function (can return list or dict)
            if isinstance(trigger_params, DeployTimeField):
                trigger_params = deploy_time_eval(trigger_params)
            # If params is a list of strings, convert to dict with same key and value
            if isinstance(trigger_params, (list, tuple)):
                new_trigger_params = {}
                for mapping in trigger_params:
                    if is_stringish(mapping) or callable(mapping):
                        new_trigger_params[mapping] = mapping
                    elif callable(mapping) and not isinstance(mapping, DeployTimeField):
                        mapping = DeployTimeField(
                            "parameter_val", str, None, mapping, False
                        )
                        new_trigger_params[mapping] = mapping
                    elif isinstance(mapping, (list, tuple)) and len(mapping) == 2:
                        if callable(mapping[0]) and not isinstance(
                            mapping[0], DeployTimeField
                        ):
                            mapping[0] = DeployTimeField(
                                "parameter_val",
                                str,
                                None,
                                mapping[1],
                                False,
                            )
                        if callable(mapping[1]) and not isinstance(
                            mapping[1], DeployTimeField
                        ):
                            mapping[1] = DeployTimeField(
                                "parameter_val",
                                str,
                                None,
                                mapping[1],
                                False,
                            )

                        new_trigger_params[mapping[0]] = mapping[1]
                    else:
                        raise MetaflowException(
                            "The *parameters* attribute for event '%s' is invalid. "
                            "It should be a list/tuple of strings and lists/tuples "
                            "of size 2" % self.attributes["event"]["name"]
                        )
                trigger_params = new_trigger_params
            trigger["parameters"] = trigger_params

            trigger_name = trigger.get("name")
            # Case where just the name is a function (always a str)
            if isinstance(trigger_name, DeployTimeField):
                trigger_name = deploy_time_eval(trigger_name)
                trigger["name"] = trigger_name

            # Third layer
            # {name:, parameters:[func, ..., ...]}
            # {name:, parameters:{func : func2}}
            for trigger in self.triggers:
                old_trigger = trigger
                trigger_params = trigger.get("parameters", {})
                new_trigger_params = {}
                for key, value in trigger_params.items():
                    if isinstance(value, DeployTimeField) and key is value:
                        evaluated_param = deploy_time_eval(value)
                        new_trigger_params[evaluated_param] = evaluated_param
                    elif isinstance(value, DeployTimeField):
                        new_trigger_params[key] = deploy_time_eval(value)
                    elif isinstance(key, DeployTimeField):
                        new_trigger_params[deploy_time_eval(key)] = value
                    else:
                        new_trigger_params[key] = value
                trigger["parameters"] = new_trigger_params
            self.triggers[self.triggers.index(old_trigger)] = trigger


class TriggerOnFinishDecorator(FlowDecorator):
    """
    Specifies the flow(s) that this flow depends on.

    ```
    @trigger_on_finish(flow='FooFlow')
    ```
    or
    ```
    @trigger_on_finish(flows=['FooFlow', 'BarFlow'])
    ```
    This decorator respects the @project decorator and triggers the flow
    when upstream runs within the same namespace complete successfully

    Additionally, you can specify project aware upstream flow dependencies
    by specifying the fully qualified project_flow_name.
    ```
    @trigger_on_finish(flow='my_project.branch.my_branch.FooFlow')
    ```
    or
    ```
    @trigger_on_finish(flows=['my_project.branch.my_branch.FooFlow', 'BarFlow'])
    ```

    You can also specify just the project or project branch (other values will be
    inferred from the current project or project branch):
    ```
    @trigger_on_finish(flow={"name": "FooFlow", "project": "my_project", "project_branch": "branch"})
    ```

    Note that `branch` is typically one of:
      - `prod`
      - `user.bob`
      - `test.my_experiment`
      - `prod.staging`

    Parameters
    ----------
    flow : Union[str, Dict[str, str]], optional, default None
        Upstream flow dependency for this flow.
    flows : List[Union[str, Dict[str, str]]], default []
        Upstream flow dependencies for this flow.
    options : Dict[str, Any], default {}
        Backend-specific configuration for tuning eventing behavior.

    MF Add To Current
    -----------------
    trigger -> metaflow.events.Trigger
        Returns `Trigger` if the current run is triggered by an event

        @@ Returns
        -------
        Trigger
            `Trigger` if triggered by an event
    """

    name = "trigger_on_finish"
    defaults = {
        "flow": None,  # flow_name or project_flow_name
        "flows": [],  # flow_names or project_flow_names
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
        self,
        flow_name,
        graph,
        environment,
        flow_datastore,
        metadata,
        logger,
        echo,
        options,
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
                self.triggers.append(
                    {
                        "fq_name": self.attributes["flow"],
                    }
                )
            elif isinstance(self.attributes["flow"], dict):
                if "name" not in self.attributes["flow"]:
                    raise MetaflowException(
                        "The *flow* attribute for *@trigger_on_finish* is missing the "
                        "*name* key."
                    )
                flow_name = self.attributes["flow"]["name"]

                if not is_stringish(flow_name) or "." in flow_name:
                    raise MetaflowException(
                        "The *name* attribute of the *flow* is not a valid string"
                    )
                result = {"fq_name": flow_name}
                if "project" in self.attributes["flow"]:
                    if is_stringish(self.attributes["flow"]["project"]):
                        result["project"] = self.attributes["flow"]["project"]
                    else:
                        raise MetaflowException(
                            "The *project* attribute of the *flow* is not a string"
                        )
                if "project_branch" in self.attributes["flow"]:
                    if is_stringish(self.attributes["flow"]["project_branch"]):
                        result["branch"] = self.attributes["flow"]["project_branch"]
                    else:
                        raise MetaflowException(
                            "The *project_branch* attribute of the *flow* is not a string"
                        )
                self.triggers.append(result)
            elif callable(self.attributes["flow"]) and not isinstance(
                self.attributes["flow"], DeployTimeField
            ):
                trig = DeployTimeField(
                    "fq_name", [str, dict], None, self.attributes["flow"], False
                )
                self.triggers.append(trig)
            else:
                raise MetaflowException(
                    "Incorrect type for *flow* attribute in *@trigger_on_finish* "
                    " decorator. Supported type is string or Dict[str, str] - \n"
                    "@trigger_on_finish(flow='FooFlow') or "
                    "@trigger_on_finish(flow={'name':'FooFlow', 'project_branch': 'branch'})"
                )
        elif self.attributes["flows"]:
            # flows attribute supports the following formats -
            #     1. flows=['FooFlow', 'BarFlow']
            if isinstance(self.attributes["flows"], list):
                for flow in self.attributes["flows"]:
                    if is_stringish(flow):
                        self.triggers.append(
                            {
                                "fq_name": flow,
                            }
                        )
                    elif isinstance(flow, dict):
                        if "name" not in flow:
                            raise MetaflowException(
                                "One or more flows in the *flows* attribute for "
                                "*@trigger_on_finish* is missing the "
                                "*name* key."
                            )
                        flow_name = flow["name"]

                        if not is_stringish(flow_name) or "." in flow_name:
                            raise MetaflowException(
                                "The *name* attribute '%s' is not a valid string"
                                % str(flow_name)
                            )
                        result = {"fq_name": flow_name}
                        if "project" in flow:
                            if is_stringish(flow["project"]):
                                result["project"] = flow["project"]
                            else:
                                raise MetaflowException(
                                    "The *project* attribute of the *flow* '%s' is not "
                                    "a string" % flow_name
                                )
                        if "project_branch" in flow:
                            if is_stringish(flow["project_branch"]):
                                result["branch"] = flow["project_branch"]
                            else:
                                raise MetaflowException(
                                    "The *project_branch* attribute of the *flow* %s "
                                    "is not a string" % flow_name
                                )
                        self.triggers.append(result)
                    else:
                        raise MetaflowException(
                            "One or more flows in *flows* attribute in "
                            "*@trigger_on_finish* decorator have an incorrect type. "
                            "Supported type is string or Dict[str, str]- \n"
                            "@trigger_on_finish(flows=['FooFlow', 'BarFlow']"
                        )
            elif callable(self.attributes["flows"]) and not isinstance(
                self.attributes["flows"], DeployTimeField
            ):
                trig = DeployTimeField(
                    "flows", list, None, self.attributes["flows"], False
                )
                self.triggers.append(trig)
            else:
                raise MetaflowException(
                    "Incorrect type for *flows* attribute in *@trigger_on_finish* "
                    "decorator. Supported type is list - \n"
                    "@trigger_on_finish(flows=['FooFlow', 'BarFlow']"
                )

        if not self.triggers:
            raise MetaflowException(
                "No flow(s) specified in *@trigger_on_finish* decorator."
            )

        # Make triggers @project aware
        for trigger in self.triggers:
            if isinstance(trigger, DeployTimeField):
                continue
            if trigger["fq_name"].count(".") == 0:
                # fully qualified name is just the flow name
                trigger["flow"] = trigger["fq_name"]
            elif trigger["fq_name"].count(".") >= 2:
                # fully qualified name is of the format - project.branch.flow_name
                trigger["project"], tail = trigger["fq_name"].split(".", maxsplit=1)
                trigger["branch"], trigger["flow"] = tail.rsplit(".", maxsplit=1)
            else:
                raise MetaflowException(
                    "Incorrect format for *flow* in *@trigger_on_finish* "
                    "decorator. Specify either just the *flow_name* or a fully "
                    "qualified name like *project_name.branch_name.flow_name*."
                )
            # TODO: Also sanity check project and branch names
            if not re.match(r"^[A-Za-z0-9_]+$", trigger["flow"]):
                raise MetaflowException(
                    "Invalid flow name *%s* in *@trigger_on_finish* "
                    "decorator. Only alphanumeric characters and "
                    "underscores(_) are allowed." % trigger["flow"]
                )

        self.options = self.attributes["options"]

        # Handle scenario for local testing using --trigger.
        self._option_values = options
        if options["trigger"]:
            from metaflow import Run
            from metaflow.events import Trigger

            run_objs = []
            for run_pathspec in options["trigger"]:
                if len(run_pathspec.split("/")) != 2:
                    raise MetaflowException(
                        "Incorrect format for run pathspec for *--trigger*. "
                        "Supported format is flow_name/run_id."
                    )
                run_obj = Run(run_pathspec, _namespace_check=False)
                if not run_obj.successful:
                    raise MetaflowException(
                        "*--trigger* does not support runs that are not successful yet."
                    )
                run_objs.append(run_obj)
            current._update_env({"trigger": Trigger.from_runs(run_objs)})

    def _parse_fq_name(self, trigger):
        if isinstance(trigger, DeployTimeField):
            trigger["fq_name"] = deploy_time_eval(trigger["fq_name"])
        if trigger["fq_name"].count(".") == 0:
            # fully qualified name is just the flow name
            trigger["flow"] = trigger["fq_name"]
        elif trigger["fq_name"].count(".") >= 2:
            # fully qualified name is of the format - project.branch.flow_name
            trigger["project"], tail = trigger["fq_name"].split(".", maxsplit=1)
            trigger["branch"], trigger["flow"] = tail.rsplit(".", maxsplit=1)
        else:
            raise MetaflowException(
                "Incorrect format for *flow* in *@trigger_on_finish* "
                "decorator. Specify either just the *flow_name* or a fully "
                "qualified name like *project_name.branch_name.flow_name*."
            )
        if not re.match(r"^[A-Za-z0-9_]+$", trigger["flow"]):
            raise MetaflowException(
                "Invalid flow name *%s* in *@trigger_on_finish* "
                "decorator. Only alphanumeric characters and "
                "underscores(_) are allowed." % trigger["flow"]
            )
        return trigger

    def format_deploytime_value(self):
        for trigger in self.triggers:
            # Case were trigger is a function that returns a list
            # Need to do this bc we need to iterate over list and process
            if isinstance(trigger, DeployTimeField):
                deploy_value = deploy_time_eval(trigger)
                if isinstance(deploy_value, list):
                    self.triggers = deploy_value
            else:
                break
        for trigger in self.triggers:
            # Entire trigger is a function (returns either string or dict)
            old_trig = trigger
            if isinstance(trigger, DeployTimeField):
                trigger = deploy_time_eval(trigger)
            if isinstance(trigger, dict):
                trigger["fq_name"] = trigger.get("name")
                trigger["project"] = trigger.get("project")
                trigger["branch"] = trigger.get("project_branch")
            # We also added this bc it won't be formatted yet
            if isinstance(trigger, str):
                trigger = {"fq_name": trigger}
                trigger = self._parse_fq_name(trigger)
            self.triggers[self.triggers.index(old_trig)] = trigger

    def get_top_level_options(self):
        return list(self._option_values.items())
