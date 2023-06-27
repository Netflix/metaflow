import json
import time
import re

from metaflow import current
from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException
from metaflow.util import is_stringish

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
    event : Union[str, dict], optional
        Event dependency for this flow.
    events : List[Union[str, dict]], optional
        Events dependency for this flow.
    options : dict, optional
        Backend-specific configuration for tuning eventing behavior.
    """

    name = "trigger"
    defaults = {
        "event": None,
        "events": [],
        "options": {},
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
                if "name" not in self.attributes["event"]:
                    raise MetaflowException(
                        "The *event* attribute for *@trigger* is missing the "
                        "*name* key."
                    )
                param_value = self.attributes["event"].get("parameters", {})
                if isinstance(param_value, (list, tuple)):
                    new_param_value = {}
                    for mapping in param_value:
                        if is_stringish(mapping):
                            new_param_value[mapping] = mapping
                        elif isinstance(mapping, (list, tuple)) and len(mapping) == 2:
                            new_param_value[mapping[0]] = mapping[1]
                        else:
                            raise MetaflowException(
                                "The *parameters* attribute for event '%s' is invalid. "
                                "It should be a list/tuple of strings and lists/tuples "
                                "of size 2" % self.attributes["event"]["name"]
                            )
                    self.attributes["event"]["parameters"] = new_param_value
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
                        if "name" not in event:
                            raise MetaflowException(
                                "One or more events in *events* attribute for "
                                "*@trigger* are missing the *name* key."
                            )
                        param_value = event.get("parameters", {})
                        if isinstance(param_value, (list, tuple)):
                            new_param_value = {}
                            for mapping in param_value:
                                if is_stringish(mapping):
                                    new_param_value[mapping] = mapping
                                elif (
                                    isinstance(mapping, (list, tuple))
                                    and len(mapping) == 2
                                ):
                                    new_param_value[mapping[0]] = mapping[1]
                                else:
                                    raise MetaflowException(
                                        "The *parameters* attribute for event '%s' is "
                                        "invalid. It should be a list/tuple of strings "
                                        "and lists/tuples of size 2" % event["name"]
                                    )
                            event["parameters"] = new_param_value
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

        self.options = self.attributes["options"]

        # TODO: Handle scenario for local testing using --trigger.


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
    flow : Union[str, Dict[str, str]], optional
        Upstream flow dependency for this flow.
    flows : List[Union[str, Dict[str, str]], optional
        Upstream flow dependencies for this flow.
    options : dict, optional
        Backend-specific configuration for tuning eventing behavior.
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

    def get_top_level_options(self):
        return list(self._option_values.items())
