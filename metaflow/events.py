from enum import Enum, auto


class MetaflowTrigger:
    """
    Interface for Metaflow run triggers. Concrete implementations provided by each eventing backend.
    """

    @property
    def event(self):
        """
        Returns a `MetaflowEvent` object corresponding to the triggering event. If multiple events
        triggered the run, returns None - use `events` instead.
        """
        raise NotImplementedError()

    @property
    def events(self):
        """
        Returns a list of `MetaflowEvent` objects correspondings to all the triggering events.
        """
        raise NotImplementedError()

    @property
    def data(self):
        """
        A shorthand for `trigger.run.data`, except lazy-loads only the artifacts accessed instead of
        loading all of them as `run.data` does.
        """
        raise NotImplementedError()

    @property
    def run(self):
        """
        If the triggering event is a Metaflow run, returns the corresponding `Run` object. `None` if
        the event is not a `Run` or multiple events are present.
        """
        raise NotImplementedError()

    @property
    def runs(self):
        """
        If the triggering events correspond to Metaflow runs, returns a list of `Run` objects.
        Otherwise returns `None`.
        """
        raise NotImplementedError()

    def names(self):
        """
        Returns a list of all events which caused the trigger to fire.
        """
        raise NotImplementedError()

    def __getitem__(self, name):
        """
        If triggering events are runs, `name` corresponds to the flow name of the triggering run.
        Returns a triggering `Run` object corresponding to the key. If triggering events are not
        runs, `name` corresponds to the event name and a `MetaflowEvent` object is returned.
        """
        raise NotImplementedError()

    def __len__(self):
        """
        Returns total count of triggering events.
        """
        raise NotImplementedError()


class MetaflowEventTypes(Enum):
    EVENT = auto()
    RUN = auto()

    @classmethod
    def from_str(cls, text):
        for member in cls:
            if member.name == text:
                return member
        return None

    def __str__(self):
        return self.name


class MetaflowEvent:
    """
    Interface for Metaflow events. Concrete implementations provided by each eventing backend.
    """

    @property
    def name(self):
        """event name"""
        raise NotImplementedError()

    @property
    def timestamp(self):
        """event creation timestamp as a datetime"""
        raise NotImplementedError()

    @property
    def type(self):
        """
        Return event type: `RUN` or `EVENT`
        """
        raise NotImplementedError()
