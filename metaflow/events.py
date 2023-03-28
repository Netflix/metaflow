from collections import namedtuple, OrderedDict
from datetime import datetime


MetaflowEvent = namedtuple("MetaflowEvent", ["name", "id", "timestamp", "type"])


class MetaflowTrigger(object):
    def __init__(self, _meta=[]):
        _meta.sort(key=lambda x: x.get("timestamp") or float("-inf"), reverse=True)
        self._events = OrderedDict(
            {
                obj["name"]: MetaflowEvent(
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
            }
        )

    @property
    def event(self):
        """
        Returns a `MetaflowEvent` object corresponding to the triggering event. If multiple events triggered the run, returns the latest event.
        """
        return next(iter(self.events), None)

    @property
    def events(self):
        """
        Returns a list of `MetaflowEvent` objects correspondings to all the triggering events.
        """
        return list(self._events.values()) or None

    @property
    def run(self):
        """
        If the triggering event is a Metaflow run, returns the corresponding `Run` object. If multiple runs triggered the run, returns the latest run. `None` if the event is not a `Run`.
        """
        return next(iter(self.runs), None)

    @property
    def runs(self):
        """
        If the triggering events correspond to Metaflow runs, returns a list of `Run` objects. Otherwise returns `None`.
        """
        # to avoid circular import
        from metaflow import Run

        return [
            Run(
                obj.id[: obj.id.index("/", obj.id.index("/") + 1)],
                _namespace_check=False,
            )
            for obj in filter(
                lambda x: x.type in ["run"],
                self._events.values(),
            )
        ] or None

    def __getitem__(self, key):
        """
        If triggering events are runs, `key` corresponds to the flow name of the triggering run. Returns a triggering `Run` object corresponding to the key. If triggering events are not runs, `key` corresponds to the event name and a `MetaflowEvent` object is returned.
        """
        if self.runs:
            for run in self.runs:
                if run.path_components[0] == key:
                    return run
        elif self.events:
            for event in self.events:
                if event.name == key:
                    return event
        else:
            return None
