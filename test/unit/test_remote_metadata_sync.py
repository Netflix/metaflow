from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from metaflow.datastore.exceptions import DataException
from metaflow.plugins.aws.batch import batch_cli
from metaflow.plugins.aws.batch.batch import BatchException


def test_best_effort_sync_skips_non_local_metadata():
    flow_datastore = Mock()
    metadata = SimpleNamespace(TYPE="service")

    batch_cli._sync_local_metadata_from_datastore_best_effort(
        flow_datastore=flow_datastore,
        metadata=metadata,
        run_id="1",
        step_name="step",
        task_id="task",
        attempt=2,
    )

    flow_datastore.get_task_datastore.assert_not_called()


def test_best_effort_sync_uses_allow_not_done_and_attempt():
    flow_datastore = Mock()
    task_ds = Mock()
    task_ds.has_metadata.return_value = False
    flow_datastore.get_task_datastore.return_value = task_ds
    metadata = SimpleNamespace(TYPE="local")

    batch_cli._sync_local_metadata_from_datastore_best_effort(
        flow_datastore=flow_datastore,
        metadata=metadata,
        run_id="1",
        step_name="step",
        task_id="task",
        attempt=3,
    )

    flow_datastore.get_task_datastore.assert_called_once_with(
        "1",
        "step",
        "task",
        attempt=3,
        allow_not_done=True,
    )
    task_ds.has_metadata.assert_called_once_with("local_metadata")


def test_best_effort_sync_loads_local_metadata_when_present(monkeypatch):
    flow_datastore = Mock()
    task_ds = Mock()
    task_ds.has_metadata.return_value = True
    flow_datastore.get_task_datastore.return_value = task_ds
    metadata = SimpleNamespace(TYPE="local")

    sync_mock = Mock()
    monkeypatch.setattr(batch_cli, "sync_local_metadata_from_datastore", sync_mock)

    batch_cli._sync_local_metadata_from_datastore_best_effort(
        flow_datastore=flow_datastore,
        metadata=metadata,
        run_id="1",
        step_name="step",
        task_id="task",
        attempt=0,
    )

    sync_mock.assert_called_once()


def test_best_effort_sync_never_raises():
    flow_datastore = Mock()
    flow_datastore.get_task_datastore.side_effect = RuntimeError("boom")
    metadata = SimpleNamespace(TYPE="local")

    batch_cli._sync_local_metadata_from_datastore_best_effort(
        flow_datastore=flow_datastore,
        metadata=metadata,
        run_id="1",
        step_name="step",
        task_id="task",
        attempt=0,
    )


def test_best_effort_sync_warns_on_failure():
    flow_datastore = Mock()
    flow_datastore.get_task_datastore.side_effect = RuntimeError("boom")
    metadata = SimpleNamespace(TYPE="local")
    warn_fn = Mock()

    batch_cli._sync_local_metadata_from_datastore_best_effort(
        flow_datastore=flow_datastore,
        metadata=metadata,
        run_id="1",
        step_name="step",
        task_id="task",
        attempt=0,
        warn_fn=warn_fn,
    )

    warn_fn.assert_called_once()
    assert "Failed to sync local metadata" in warn_fn.call_args[0][0]


def _make_batch_step_context(flow_datastore, metadata_type="local"):
    echo_always = Mock()
    obj = SimpleNamespace(
        echo_always=echo_always,
        environment=SimpleNamespace(
            executable=lambda step_name, executable: "python",
            get_environment_info=lambda: {"metaflow_version": "test"},
        ),
        graph={"train": SimpleNamespace(decorators=[])},
        monitor=SimpleNamespace(measure=lambda *args, **kwargs: nullcontext()),
        flow=SimpleNamespace(name="Flow"),
        flow_datastore=flow_datastore,
        metadata=SimpleNamespace(TYPE=metadata_type),
    )
    return SimpleNamespace(
        obj=obj,
        parent=SimpleNamespace(parent=SimpleNamespace(params={})),
    )


def test_batch_step_surfaces_batch_error_when_metadata_sync_fails(monkeypatch):
    """Regression test for #2557: metadata sync failures must not mask Batch errors."""
    # Avoid the R code path in this unit test.
    monkeypatch.setattr(batch_cli.R, "use_r", lambda: False)

    flow_datastore = Mock()
    log_ds = Mock()
    log_ds.get_log_location.side_effect = ["stdout-url", "stderr-url"]

    ds_calls = []

    def _get_task_datastore(*args, **kwargs):
        ds_calls.append((args, kwargs))
        if kwargs.get("mode") == "w":
            return log_ds
        # Simulate the historical failure from metadata sync.
        raise DataException("No completed attempts")

    flow_datastore.get_task_datastore.side_effect = _get_task_datastore

    class FakeBatch(object):
        def __init__(self, metadata, environment):
            pass

        def launch_job(self, *args, **kwargs):
            pass

        def wait(self, *args, **kwargs):
            raise BatchException("real batch failure")

    monkeypatch.setattr(batch_cli, "Batch", FakeBatch)
    ctx = _make_batch_step_context(flow_datastore, metadata_type="local")

    with pytest.raises(BatchException, match="real batch failure"):
        batch_cli.step.callback.__wrapped__(  # type: ignore[attr-defined]
            ctx,
            "train",
            "{}",
            "sha",
            "s3://bucket/code",
            aws_batch_tags=(),
            run_id="1",
            task_id="2",
            retry_count=0,
        )

    # Ensure metadata sync was attempted after batch.wait failure:
    # 1) write-mode task datastore for log locations
    # 2) read-mode task datastore for local metadata sync, which fails
    assert len(ds_calls) == 2
    _, first_call_kwargs = ds_calls[0]
    _, second_call_kwargs = ds_calls[1]
    assert first_call_kwargs["mode"] == "w"
    assert second_call_kwargs["allow_not_done"] is True
    assert second_call_kwargs["attempt"] == 0
    ctx.obj.echo_always.assert_called_once()
    assert "Failed to sync local metadata" in ctx.obj.echo_always.call_args[0][0]
