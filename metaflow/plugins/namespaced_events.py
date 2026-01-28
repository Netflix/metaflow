from collections import namedtuple
import functools
import re

from metaflow.metaflow_config import NAMESPACED_EVENTS_PREFIX
from metaflow.util import is_stringish

ParsedEvent = namedtuple(
    "ParsedEvent",
    ["full_name", "project", "branch", "logical_name", "is_namespaced"],
)


def namespaced_event_name(event_name):
    """
    Creates a project-namespaced event name based on @project settings.

    Use this to automatically prefix event names with your @project
    namespace, ensuring events are isolated per project and branch.

    The resolved name follows the format:
        mfns.<project>.<branch>.<event_name>

    If no @project decorator is present, resolves to:
        mfns.<event_name>

    Parameters
    ----------
    event_name : str
        The logical event name (e.g., 'data_ready')

    Returns
    -------
    Callable[..., str]
        A callable that returns the fully namespaced event name (str) when invoked

    Examples
    --------
    With @trigger decorator:

        ```
        from metaflow import namespaced_event_name, project

        @project(name="foo")
        @trigger(event=namespaced_event_name('data_ready'))
        class MyFlow(FlowSpec):
            ...

        @project(name="foo")
        @trigger(event={'name': namespaced_event_name('data_ready'), 'parameters': {'x': 'y'}})
        class MyFlow(FlowSpec):
        ```

    With ArgoEvent:

        ```
        from metaflow import namespaced_event_name
        from metaflow.integrations import ArgoEvent

        ArgoEvent(namespaced_event_name('data_ready')).publish()
        ```
    """
    if event_name is None or not is_stringish(event_name):
        raise ValueError(
            "event_name should be a string not %s" % (str(type(event_name)))
        )

    def _delayed_name_generator(context=None, event_name=None):
        from metaflow import current

        project_name = current.get("project_name", None)
        branch_name = current.get("branch_name", None)
        return _make_namespaced_event_name(event_name, project_name, branch_name)

    return functools.partial(_delayed_name_generator, event_name=event_name)


def _make_namespaced_event_name(logical_name, project=None, branch=None):
    """
    Construct a namespaced event name from components.

    Parameters
    ----------
    logical_name : str
        The base event name
    project : str, optional
        Project name from @project decorator
    branch : str, optional
        Branch name from @project decorator

    Returns
    -------
    str
        Namespaced event name in format mfns.<project>.<branch>.<logical_name>
        or mfns.<logical_name> if no project/branch
    """
    if project and branch:
        # When it comes to project and branches we want to make
        # sure that we are not having any non-acceptable characters.
        # allow dot (.) in project and branch names
        branch = re.sub(r"[^a-zA-Z0-9_\-\.]", "", branch)
        project = re.sub(r"[^a-zA-Z0-9_\-\.]", "", project)
        return f"{NAMESPACED_EVENTS_PREFIX}.{project}.{branch}.{logical_name}"
    else:
        return f"{NAMESPACED_EVENTS_PREFIX}.{logical_name}"
