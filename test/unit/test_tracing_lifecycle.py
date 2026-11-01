import os
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
import opentelemetry.trace as trace_api
import opentelemetry.propagate as propagate

import metaflow.tracing as tracing
from metaflow.tracing import tracing_modules
from metaflow import FlowSpec, step
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.datastore import FlowDataStore
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from metaflow.plugins import MONITOR_SIDECARS
from metaflow.task import MetaflowTask


@pytest.fixture(autouse=True)
def clean_tracing_state(monkeypatch):
    monkeypatch.setenv("METAFLOW_CONSOLE_TRACE_ENABLED", "true")
    trace_api._TRACER_PROVIDER = None
    trace_api._TRACER_PROVIDER_SET_ONCE._done = False
    propagate._HTTP_TEXT_FORMAT = None
    tracing_modules.tracer_provider = None
    yield
    trace_api._TRACER_PROVIDER = None
    trace_api._TRACER_PROVIDER_SET_ONCE._done = False
    propagate._HTTP_TEXT_FORMAT = None
    tracing_modules.tracer_provider = None


def test_tracing_disabled():
    import metaflow.tracing as disabled_tracing
    # Test no-op behaviors when functions are called directly from disabled module
    with disabled_tracing.traced("noop_span", {"key": "val"}) as span:
        assert span is None
    assert disabled_tracing.get_trace_id() == ""
    assert disabled_tracing.get_current_span() is None
    # Ensure these do not raise
    disabled_tracing.add_span_event("noop_event", {"a": 1})
    disabled_tracing.set_span_attributes({"b": 2})
    disabled_tracing.flush_tracing()
    env = {"EXISTING": "1"}
    assert disabled_tracing.inject_tracing_vars(env) == {"EXISTING": "1"}


def test_tracing_modules_in_memory_lifecycle():
    exporter = InMemorySpanExporter()
    tracing.init_tracing(exporter=exporter)

    with tracing_modules.traced("parent_span", {"flow": "test_flow"}) as parent:
        assert parent is not None
        tracing_modules.add_span_event("custom_event", {"detail": "started"})
        tracing_modules.set_span_attributes({"extra_attr": 99})

        with tracing_modules.traced("child_subphase", {"phase": "work"}):
            pass

    with pytest.raises(ValueError):
        with tracing_modules.traced("error_span"):
            raise ValueError("boom")

    tracing_modules.flush_tracing()
    spans = exporter.get_finished_spans()
    span_names = [s.name for s in spans]
    assert "parent_span" in span_names
    assert "child_subphase" in span_names
    assert "error_span" in span_names

    parent_span = next(s for s in spans if s.name == "parent_span")
    child_span = next(s for s in spans if s.name == "child_subphase")
    error_span = next(s for s in spans if s.name == "error_span")

    # Hierarchy verification
    assert child_span.parent.span_id == parent_span.context.span_id

    # Attributes & event verification
    assert parent_span.attributes["flow"] == "test_flow"
    assert parent_span.attributes["extra_attr"] == 99
    event_names = [e.name for e in parent_span.events]
    assert "custom_event" in event_names

    # Error status verification
    assert error_span.status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in error_span.events)


def test_context_propagation_vars():
    exporter = InMemorySpanExporter()
    tracing.init_tracing(exporter=exporter)

    with tracing_modules.traced("trace_parent_span") as span:
        env = {}
        tracing_modules.inject_tracing_vars(env)
        env_lower = {k.lower(): v for k, v in env.items()}
        assert "traceparent" in env_lower
        parts = env_lower["traceparent"].split("-")
        assert len(parts) == 4
        assert parts[0] == "00"
        assert parts[1] == format(span.context.trace_id, "032x")
        assert parts[2] == format(span.context.span_id, "016x")


class LifecycleFlow(FlowSpec):
    @step
    def start(self):
        self.message = "hello tracing"
        self.next(self.end)

    @step
    def end(self):
        self.final_msg = self.message.upper()


def test_task_run_step_lifecycle_spans(tmp_path, monkeypatch):
    sysroot = str(tmp_path / "sysroot")
    os.makedirs(sysroot, exist_ok=True)
    monkeypatch.setattr(LocalStorage, "SYSROOT_VAR", sysroot)
    monkeypatch.setattr(LocalStorage, "datastore_root", sysroot)
    exporter = InMemorySpanExporter()
    tracing.init_tracing(exporter=exporter)

    storage_root = str(tmp_path / "datastore")
    os.makedirs(storage_root, exist_ok=True)

    flow = LifecycleFlow(use_cli=False)
    flow._set_constants(flow._graph, {}, [])
    env = MetaflowEnvironment(flow)
    datastore = FlowDataStore(
        flow_name=flow.name,
        environment=env,
        metadata=None,
        event_logger=None,
        monitor=None,
        storage_impl=LocalStorage,
        ds_root=storage_root,
    )
    metadata = LocalMetadataProvider(
        environment=env,
        flow=flow,
        event_logger=None,
        monitor=None,
    )
    monitor = MONITOR_SIDECARS["nullSidecarMonitor"](flow)
    task = MetaflowTask(
        flow,
        datastore,
        metadata,
        environment=env,
        console_logger=lambda *args, **kwargs: None,
        event_logger=None,
        monitor=monitor,
        ubf_context=None,
    )

    # Initialize parameters datastore
    p_ds = datastore.get_task_datastore("1001", "_parameters", "0", attempt=0, mode="w")
    p_ds.init_task()
    p_ds.save_artifacts([
        ("_foreach_stack", []),
        ("_foreach_var", None),
        ("_graph_info", flow._graph_info),
    ])
    p_ds.done()

    # 1. Run start step with retry_count=0
    task.run_step(
        step_name="start",
        run_id="1001",
        task_id="1",
        origin_run_id=None,
        input_paths=["1001/_parameters/0"],
        split_index=None,
        retry_count=0,
        max_user_code_retries=0,
    )

    spans = exporter.get_finished_spans()
    names = [s.name for s in spans]

    assert "task/start" in names
    assert "task/step_func/start" in names
    assert "task/persist_output" in names

    task_span = next(s for s in spans if s.name == "task/start")
    step_func_span = next(s for s in spans if s.name == "task/step_func/start")
    persist_span = next(s for s in spans if s.name == "task/persist_output")

    # Hierarchy verification
    assert step_func_span.parent.span_id == task_span.context.span_id
    assert persist_span.parent.span_id == task_span.context.span_id

    # Attribute verification
    assert task_span.attributes["metaflow.flow_name"] == "LifecycleFlow"
    assert task_span.attributes["metaflow.run_id"] == "1001"
    assert task_span.attributes["metaflow.step_name"] == "start"
    assert task_span.attributes["metaflow.task_id"] == "1"
    assert task_span.attributes["metaflow.retry_count"] == 0
    assert task_span.attributes["metaflow.attempt"] == 0

    # 2. Run end step with input_paths and retry_count=1
    exporter.clear()
    flow_end = LifecycleFlow(use_cli=False)
    env_end = MetaflowEnvironment(flow_end)
    task_end = MetaflowTask(
        flow_end,
        datastore,
        metadata,
        environment=env_end,
        console_logger=lambda *args, **kwargs: None,
        event_logger=None,
        monitor=monitor,
        ubf_context=None,
    )

    task_end.run_step(
        step_name="end",
        run_id="1001",
        task_id="2",
        origin_run_id=None,
        input_paths=["1001/start/1"],
        split_index=0,
        retry_count=1,
        max_user_code_retries=1,
    )

    end_spans = exporter.get_finished_spans()
    end_names = [s.name for s in end_spans]

    assert "task/end" in end_names
    assert "task/load_inputs" in end_names
    assert "task/step_func/end" in end_names
    assert "task/persist_output" in end_names

    task_end_span = next(s for s in end_spans if s.name == "task/end")
    load_inputs_span = next(s for s in end_spans if s.name == "task/load_inputs")

    assert load_inputs_span.parent.span_id == task_end_span.context.span_id
    assert task_end_span.attributes["metaflow.retry_count"] == 1
    assert task_end_span.attributes["metaflow.is_retry"] is True
    assert task_end_span.attributes["metaflow.split_index"] == 0
    event_names = [e.name for e in task_end_span.events]
    assert "task_retry" in event_names


def test_runtime_flow_span_and_retry_event():
    exporter = InMemorySpanExporter()
    tracing.init_tracing(exporter=exporter)

    # Verify flow span around runtime execute
    flow = LifecycleFlow(use_cli=False)
    with tracing_modules.traced(f"flow/{flow.name}", attrs={"metaflow.flow_name": flow.name, "metaflow.run_id": "r100"}):
        tracing_modules.add_span_event("task_retry", {"metaflow.step_name": "start", "metaflow.retry_count": 1})

    tracing_modules.flush_tracing()
    spans = exporter.get_finished_spans()
    flow_span = next(s for s in spans if s.name == "flow/LifecycleFlow")
    assert flow_span.attributes["metaflow.flow_name"] == "LifecycleFlow"
    assert flow_span.attributes["metaflow.run_id"] == "r100"
    event_names = [e.name for e in flow_span.events]
    assert "task_retry" in event_names
    retry_event = next(e for e in flow_span.events if e.name == "task_retry")
    assert retry_event.attributes["metaflow.step_name"] == "start"
    assert retry_event.attributes["metaflow.retry_count"] == 1


def test_remote_plugins_trace_propagation():
    exporter = InMemorySpanExporter()
    tracing.init_tracing(exporter=exporter)

    class DummyJob(object):
        def __init__(self):
            self.env = {}

        def environment_variable(self, name, value):
            self.env[name] = value
            return self

    with tracing.traced("remote_parent_flow") as span:
        job = DummyJob()
        for k, v in tracing.inject_tracing_vars({}).items():
            job.environment_variable(k, v)

        assert "traceparent" in {k.lower(): v for k, v in job.env.items()}
        tp_val = next(v for k, v in job.env.items() if k.lower() == "traceparent")
        trace_id_hex = format(span.context.trace_id, "032x")
        assert trace_id_hex in tp_val

