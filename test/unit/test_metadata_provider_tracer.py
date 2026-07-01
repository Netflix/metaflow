import asyncio

import pytest

from metaflow.exception import MetaflowInternalError
from metaflow.metadata_provider import MetadataTraceRecord, MetadataTracer
from metaflow.metadata_provider.metadata import MetadataProvider


class _TracingProvider(MetadataProvider):
    TYPE = "tracing-test"

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        if sub_type == "metadata":
            return [
                {
                    "field_name": "attempt",
                    "value": "0",
                    "ts_epoch": 1000,
                    "tags": [],
                },
                {
                    "field_name": "attempt",
                    "value": "1",
                    "ts_epoch": 2000,
                    "tags": [],
                },
                {
                    "field_name": "note",
                    "value": "attempt-1-only",
                    "ts_epoch": 2500,
                    "tags": ["attempt_id:1"],
                },
            ]
        return {"obj_type": obj_type, "sub_type": sub_type, "attempt": attempt}


def test_valid_get_object_records_trace():
    with MetadataTracer() as tracer:
        result = _TracingProvider.get_object(
            "run", "step", None, None, "MyFlow", "1"
        )

    assert result == {"obj_type": "run", "sub_type": "step", "attempt": None}
    assert tracer.request_count == 1
    assert tracer.records == [
        MetadataTraceRecord(
            obj_type="run",
            sub_type="step",
            depth=2,
            attempt=None,
            path="MyFlow/1",
        )
    ]


def test_invalid_calls_raise_and_record_nothing():
    with MetadataTracer() as tracer:
        with pytest.raises(MetaflowInternalError):
            _TracingProvider.get_object("not-a-type", "self", None, None, "MyFlow")
        with pytest.raises(MetaflowInternalError):
            _TracingProvider.get_object("run", "run", None, None, "MyFlow", "1")
        with pytest.raises(ValueError):
            _TracingProvider.get_object("task", "metadata", None, "bad", "MyFlow")

    assert tracer.request_count == 0
    assert tracer.records == []


def test_attempt_is_normalized_before_recording():
    with MetadataTracer() as tracer:
        _TracingProvider.get_object(
            "task", "artifact", None, "2", "MyFlow", "1", "step", "task"
        )

    assert tracer.records == [
        MetadataTraceRecord(
            obj_type="task",
            sub_type="artifact",
            depth=4,
            attempt=2,
            path="MyFlow/1/step/task",
        )
    ]


def test_metadata_attempt_reconstruction_does_not_add_extra_record():
    with MetadataTracer() as tracer:
        result = _TracingProvider.get_object(
            "task", "metadata", None, "1", "MyFlow", "1", "step", "task"
        )

    assert tracer.request_count == 1
    assert tracer.records[0] == MetadataTraceRecord(
        obj_type="task",
        sub_type="metadata",
        depth=4,
        attempt=1,
        path="MyFlow/1/step/task",
    )
    assert result == [
        {
            "field_name": "attempt",
            "value": "1",
            "ts_epoch": 2000,
            "tags": [],
        },
        {
            "field_name": "note",
            "value": "attempt-1-only",
            "ts_epoch": 2500,
            "tags": ["attempt_id:1"],
        }
    ]


def test_nested_contexts_restore_outer_tracer():
    with MetadataTracer() as outer:
        _TracingProvider.get_object("run", "step", None, None, "OuterFlow", "1")
        with MetadataTracer() as inner:
            _TracingProvider.get_object("step", "task", None, None, "InnerFlow", "2", "start")
        _TracingProvider.get_object("run", "self", None, None, "OuterFlow", "1")

    assert outer.records == [
        MetadataTraceRecord(
            obj_type="run",
            sub_type="step",
            depth=2,
            attempt=None,
            path="OuterFlow/1",
        ),
        MetadataTraceRecord(
            obj_type="run",
            sub_type="self",
            depth=2,
            attempt=None,
            path="OuterFlow/1",
        ),
    ]
    assert inner.records == [
        MetadataTraceRecord(
            obj_type="step",
            sub_type="task",
            depth=3,
            attempt=None,
            path="InnerFlow/2/start",
        )
    ]


@pytest.mark.asyncio
async def test_concurrent_tasks_do_not_leak_traces():
    async def run_trace(flow_id, step_name):
        with MetadataTracer() as tracer:
            await asyncio.sleep(0)
            _TracingProvider.get_object("step", "task", None, None, flow_id, "1", step_name)
            await asyncio.sleep(0)
            return tracer.records

    first, second = await asyncio.gather(
        run_trace("FlowA", "start"),
        run_trace("FlowB", "join"),
    )

    assert first == [
        MetadataTraceRecord(
            obj_type="step",
            sub_type="task",
            depth=3,
            attempt=None,
            path="FlowA/1/start",
        )
    ]
    assert second == [
        MetadataTraceRecord(
            obj_type="step",
            sub_type="task",
            depth=3,
            attempt=None,
            path="FlowB/1/join",
        )
    ]


def test_public_exports_work():
    with MetadataTracer() as tracer:
        _TracingProvider.get_object("run", "self", None, None, "MyFlow", "1")

    assert isinstance(tracer.records[0], MetadataTraceRecord)
    assert tracer.summary() == {
        "request_count": 1,
        "by_obj_type": {"run": 1},
    }
