from collections import namedtuple
from collections.abc import Mapping
from datetime import datetime
from itertools import groupby

MetaflowEvent = namedtuple("MetaflowEvent", ["name", "id", "timestamp", "type"])


class MetaflowTrigger(object):
    def __init__(self, _meta=[]):
        self._meta = _meta

    @property
    def event(self):
        """
        Returns a `MetaflowEvent` object corresponding to the triggering event. If multiple events triggered the run, returns None - use `events` instead.
        """
        return self.events[0] if self.events and len(self.events) == 1 else None

    @property
    def events(self):
        """
        Returns a list of `MetaflowEvent` objects correspondings to all the triggering events.
        """
        return [
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
            for obj in self._meta
        ] or None

    @property
    def run(self):
        """
        If the triggering event is a Metaflow run, returns the corresponding `Run` object. `None` if the event is not a `Run` or multiple events are present.
        """
        return self.runs[0] if self.runs and len(self.runs) == 1 else None

    @property
    def runs(self):
        """
        If the triggering events correspond to Metaflow runs, returns a list of `Run` objects. Otherwise returns `None`.
        """
        # to avoid circular import
        from metaflow import Run

        return [
            Run(
                obj["id"][: obj["id"].index("/", obj["id"].index("/") + 1)],
                _namespace_check=False,
            )
            for obj in filter(
                lambda x: x.get("type") in ["run"],
                self._meta,
            )
        ] or None

    @property
    def data(self):
        """
        A shorthand for `trigger.run.data`, except lazy-loads only the artifacts accessed instead of loading all of them as `run.data` does.
        """
        run = self.run
        if run:
            # end task has to necessarily exist
            return DataArtifactProxy(run.end_task)

    @property
    def type(self):
        """
        Return trigger type: `RUN`, `MANY-RUNS` (and), `EVENT`, `MANY-EVENTS` (and).
        """
        _counts = {
            k: len(list(v))
            for k, v in groupby(
                sorted(
                    list(
                        filter(lambda x: x.get("type") in ["event", "run"], self._meta)
                    ),
                    key=lambda x: x["type"],
                ),
                key=lambda x: x["type"],
            )
        }
        # Mixing runs and events is disallowed.
        if _counts.get("run"):
            if _counts.get("run") > 1:
                return "MANY-RUNS"
            return "RUN"
        if _counts.get("event"):
            if _counts.get("event") > 1:
                return "MANY-EVENTS"
            return "EVENT"

    def __getitem__(self, key):
        """
        If triggering events are runs, `key` corresponds to the flow name of the triggering run. Returns a triggering `Run` object corresponding to the key. If triggering events are not runs, `key` corresponds to the event name and a `MetaflowEvent` object is returned.
        """
        if self.type in ("RUN", "MANY-RUNS", "ONE-OF-RUNS"):
            for run in self.runs:
                if run.path_components[0] == key:
                    return run
        elif self.type in ("EVENT", "MANY-EVENTS", "ONE-OF-EVENTS"):
            for event in self.events:
                if event.name == key:
                    return event
        else:
            return None


# TODO: can we do away with this?
class DataArtifactProxy(Mapping):
    __slots__ = ("task",)

    def __init__(self, task):
        self.task = task

    def __getitem__(self, key):
        return self.task[key].data

    def __getattr__(self, key):
        return self.task[key].data

    def __iter__(self):
        return iter(self.task)

    def __len__(self):
        return len(self.task)
