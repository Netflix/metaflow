from datetime import datetime, timezone
import json
import os


from metaflow.exception import MetaflowException, MetaflowExceptionWrapper
from metaflow.events import (
    MetaflowTrigger,
    MetaflowEvent,
    MetaflowEventTypes,
)


class ArgoSensorTriggerInfo(MetaflowTrigger):
    def __init__(self):
        self._events = {}
        self._runs = {}
        self._all_names = []
        self._load_events()

    def _load_events(self):
        i = 0
        keep_loading = True
        while keep_loading:
            base_name = "MF_EVENT_%s" % i
            if os.getenv(base_name) is None:
                keep_loading = False
                continue
            event = ArgoEventInfo(base_name)
            if event.type == MetaflowEventTypes.RUN:
                self._runs[event.name] = event
            else:
                self._events[event.name] = event
            i += 1
        # If we have events and runs then combine them together as events
        if len(self._events) > 0:
            if len(self._runs) > 0:
                self._events = self._events.update(self._runs)
                self._runs.clear()
            self._all_names = list(self._events.keys())
        else:
            self._all_names = list(self._runs.keys()) + list(self._events.keys())

    @property
    def event(self):
        ev = self.events
        if len(ev) == 1:
            return ev[0]
        else:
            return None

    @property
    def events(self):
        return list(self._events.values())

    @property
    def data(self):
        # TODO Lazy load triggering run data (KAS Dec 21 2022)
        r = self.run
        if r is not None:
            return r.data
        return None

    @property
    def run(self):
        if len(self._runs) == 1:
            rs = [r.run for r in self._runs.values()]
            return rs[0]
        else:
            return None

    @property
    def runs(self):
        return [r.run for r in self._runs.values()]

    def names(self):
        return self._all_names

    def __getitem__(self, name):
        if name in self._events:
            return self._events[name]
        elif name in self._runs:
            run_event = self._runs[name]
            return run_event.run
        else:
            return None

    def __len__(self):
        return len(self._runs) + len(self._events)


class ArgoEventInfo(MetaflowEvent):
    def __init__(self, base_name):
        self._base_name = base_name
        self._name = None
        self._type = None
        self._timestamp = None
        self._pathspec = None
        self._run = None
        self._load()

    def _load(self):
        env_value = os.getenv(self._base_name)
        if env_value is None:
            raise MetaflowException(
                "Event data for event %s not found" % self._base_name
            )
        value = None
        try:
            value = json.loads(env_value)
        except Exception as e:
            raise MetaflowExceptionWrapper(e)

        # Validate expected fields are present
        if "timestamp" not in value:
            raise MetaflowException(
                "Event timestamp for event %s not found" % self._base_name
            )
        if "event_type" not in value:
            raise MetaflowException(
                "Event type for event %s not found" % self._base_name
            )
        if "event_name" not in value:
            raise MetaflowException(
                "Event name for event %s not found" % self._base_name
            )
        if "pathspec" not in value:
            raise MetaflowException(
                "Event pathspec for event %s not found" % self._base_name
            )
        self._pathspec = value["pathspec"]
        if value["event_type"] == "metaflow_system":
            self._type = MetaflowEventTypes.RUN
            self._name = self._pathspec.split("/", maxsplit=1)[0]
        else:
            self._type = MetaflowEventTypes.EVENT
            self._name = value["event_name"]

        # Create datetime from timestamp
        try:
            ts = int(value["timestamp"])
            ts = int(ts / 1000)
            self._timestamp = datetime.fromtimestamp(ts)
            self._timestamp.replace(tzinfo=timezone.utc)
        except ValueError:
            raise MetaflowException(
                "Event timestamp for event %s is not an integer" % self._base_name
            )

    def __str__(self):
        return "<%s name=%s, type=%s, timestamp=%s>" % (
            self.__class__.__name__,
            self._name,
            self._type,
            self._timestamp,
        )

    def __repr__(self):
        return str(self)

    @property
    def run(self):
        from metaflow.client import Run

        if self.type != MetaflowEventTypes.RUN:
            raise ValueError("Wrong event type")
        if self._run is None:
            self._run = Run(
                pathspec=self._pathspec,
                _namespace_check=False,
                _propagate_namespace_check=True,
            )
        return self._run

    @property
    def name(self):
        return self._name

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def type(self):
        return self._type
