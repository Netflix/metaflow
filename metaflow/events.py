from collections import OrderedDict, namedtuple
from datetime import datetime

MetaflowEvent = namedtuple("MetaflowEvent", ["name", "id", "timestamp", "type"])
MetaflowEvent.__doc__ = """
    Container of metadata that identifies the event that triggered
    the `Run` under consideration.

    Attributes
    ----------
    name : str
        name of the event.
    id : str
        unique identifier for the event.
    timestamp : datetime
        timestamp recording creation time for the event.
    type : str
        type for the event - one of `event` or `run`
    """


class Trigger(object):
    """
    Defines a container of event triggers' metadata.

    """

    def __init__(self, _meta=None):
        if _meta is None:
            _meta = []

        _meta.sort(key=lambda x: x.get("timestamp") or float("-inf"), reverse=True)

        self._runs = None
        self._events = [
            MetaflowEvent(
                **{
                    **obj,
                    # Add timestamp as datetime. Guaranteed to exist for Metaflow
                    # events - best effort for everything else.
                    **(
                        {"timestamp": datetime.fromtimestamp(obj["timestamp"])}
                        if obj.get("timestamp")
                        and isinstance(obj.get("timestamp"), int)
                        else {}
                    ),
                }
            )
            for obj in _meta
        ]

    @classmethod
    def from_runs(cls, run_objs):
        run_objs.sort(key=lambda x: x.finished_at, reverse=True)
        trigger = Trigger(
            [
                {
                    "type": "run",
                    "timestamp": run_obj.finished_at,
                    "name": "metaflow.%s.%s" % (run_obj.parent.id, run_obj["end"].id),
                    "id": run_obj.end_task.pathspec,
                }
                for run_obj in run_objs
            ]
        )
        trigger._runs = run_objs
        return trigger

    @property
    def event(self):
        """
        The `MetaflowEvent` object corresponding to the triggering event.

        If multiple events triggered the run, this property is the latest event.

        Returns
        -------
        MetaflowEvent, optional
            The latest event that triggered the run, if applicable.
        """
        return next(iter(self._events), None)

    @property
    def events(self):
        """
        The list of `MetaflowEvent` objects correspondings to all the triggering events.

        Returns
        -------
        List[MetaflowEvent], optional
            List of all events that triggered the run
        """
        return list(self._events) or None

    @property
    def run(self):
        """
        The corresponding `Run` object if the triggering event is a Metaflow run.

        In case multiple runs triggered the run, this property is the latest run.
        Returns `None` if none of the triggering events are a `Run`.

        Returns
        -------
        Run, optional
            Latest Run that triggered this run, if applicable.
        """
        if self._runs is None:
            self.runs
        return next(iter(self._runs), None)

    @property
    def runs(self):
        """
        The list of `Run` objects in the triggering events.
        Returns `None` if none of the triggering events are `Run` objects.

        Returns
        -------
        List[Run], optional
            List of runs that triggered this run, if applicable.
        """
        if self._runs is None:
            # to avoid circular import
            from metaflow import Run

            self._runs = [
                Run(
                    # object id is the task pathspec for events that map to run
                    obj.id[: obj.id.index("/", obj.id.index("/") + 1)],
                    _namespace_check=False,
                )
                for obj in self._events
                if obj.type == "run"
            ]

        return list(self._runs) or None

    def __getitem__(self, key):
        """
        If triggering events are runs, `key` corresponds to the flow name of the triggering run.
        Otherwise, `key` corresponds to the event name and a `MetaflowEvent` object is returned.

        Returns
        -------
        Run or MetaflowEvent
            `Run` object if triggered by a run. Otherwise returns a `MetaflowEvent`.
        """
        if self.runs:
            for run in self.runs:
                if run.path_components[0] == key:
                    return run
        elif self.events:
            for event in self.events:
                if event.name == key:
                    return event
        raise KeyError(key)

    def __iter__(self):
        if self.events:
            return iter(self.events)
        return iter([])

    def __contains__(self, id):
        try:
            return bool(self.__getitem__(id))
        except KeyError:
            return False
