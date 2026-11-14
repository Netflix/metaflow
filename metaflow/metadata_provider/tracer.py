from collections import Counter
from contextvars import ContextVar
from typing import List, NamedTuple, Optional


class MetadataTraceRecord(NamedTuple):
    obj_type: str
    sub_type: str
    depth: int
    attempt: Optional[int]
    path: str

_active_metadata_tracer = ContextVar("active_metadata_tracer", default=None)


def get_active_metadata_tracer():
    return _active_metadata_tracer.get()


class MetadataTracer(object):
    """
    Opt-in tracer for logical metadata client requests.

    Use this as a context manager around client API calls that eventually route
    through MetadataProvider.get_object().
    """

    def __init__(self):
        self.records: List[MetadataTraceRecord] = []
        self._token = None

    def __enter__(self):
        self._token = _active_metadata_tracer.set(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._token is not None:
            _active_metadata_tracer.reset(self._token)
        return False

    @property
    def request_count(self):
        return len(self.records)

    def record(self, obj_type, sub_type, depth, attempt, path):
        self.records.append(
            MetadataTraceRecord(
                obj_type=obj_type,
                sub_type=sub_type,
                depth=depth,
                attempt=attempt,
                path=path,
            )
        )

    def summary(self):
        return {
            "request_count": self.request_count,
            "by_obj_type": dict(Counter(record.obj_type for record in self.records)),
        }
