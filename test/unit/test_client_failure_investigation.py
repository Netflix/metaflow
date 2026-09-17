import pytest

from metaflow.client.core import (
    FailureSummary,
    Flow,
    Run,
    Task,
    _normalize_exception,
)


class _ExceptionObject:
    """Stand-in for a deserialized exception artifact carrying attributes."""

    def __init__(self, **attributes):
        for key, value in attributes.items():
            setattr(self, key, value)


# ---------------------------------------------------------------------------
# _normalize_exception -- pure function, tested with real inputs
# ---------------------------------------------------------------------------


def test_normalize_exception_from_mapping():
    assert _normalize_exception(
        {"type": "ValueError", "message": "boom", "stacktrace": "line 1"}
    ) == {"type": "ValueError", "message": "boom", "stacktrace": "line 1"}


def test_normalize_exception_mapping_falls_back_to_exception_key():
    result = _normalize_exception({"exception": "kaboom"})
    assert result["message"] == "kaboom"
    assert result["type"] is None
    assert result["stacktrace"] is None


def test_normalize_exception_from_object_attributes():
    assert _normalize_exception(
        _ExceptionObject(type="RuntimeError", message="bad", stacktrace="tb")
    ) == {"type": "RuntimeError", "message": "bad", "stacktrace": "tb"}


def test_normalize_exception_object_without_type_uses_qualified_name():
    result = _normalize_exception(_ExceptionObject())
    assert result["type"].endswith("._ExceptionObject")
    assert result["message"]  # falls back to str(data)
    assert result["stacktrace"] is None


def test_normalize_exception_from_bare_string():
    result = _normalize_exception("just a string")
    assert result["message"] == "just a string"
    assert result["stacktrace"] is None


# ---------------------------------------------------------------------------
# Task.failure_summary
# ---------------------------------------------------------------------------


def test_task_failure_summary_none_when_no_exception(mocker):
    task = mocker.Mock()
    task.exception = None
    assert Task.failure_summary.fget(task) is None


def test_task_failure_summary_builds_summary_from_exception(mocker):
    task = mocker.Mock()
    task.exception = {"type": "ValueError", "message": "boom", "stacktrace": "tb"}
    task.current_attempt = 2

    summary = Task.failure_summary.fget(task)

    assert isinstance(summary, FailureSummary)
    assert summary.exception_type == "ValueError"
    assert summary.message == "boom"
    assert summary.stacktrace == "tb"
    assert summary.attempt == 2


def test_task_failure_summary_propagates_read_errors():
    class _Boom:
        @property
        def exception(self):
            raise RuntimeError("exception artifact unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        Task.failure_summary.fget(_Boom())


# ---------------------------------------------------------------------------
# Run.failed_task
# ---------------------------------------------------------------------------


def _task(mocker, *, successful, finished, exception=None):
    """A task as the client sees it through its persisted `_success`,
    `_task_ok` and `_exception` artifacts."""
    return mocker.Mock(successful=successful, finished=finished, exception=exception)


def _ok(mocker):
    return _task(mocker, successful=True, finished=True)


def _crashed(mocker):
    # The task runner sets `_task_ok` to False when the step raises, so a
    # crashed task is *not* finished as far as `Task.finished` is concerned.
    return _task(
        mocker, successful=False, finished=False, exception=RuntimeError("boom")
    )


def _handled(mocker):
    # A failure handled by a decorator such as @catch: finished, not successful,
    # and no exception recorded.
    return _task(mocker, successful=False, finished=True)


def _running(mocker):
    # No artifacts persisted yet: reads exactly like a crashed task minus the
    # exception.
    return _task(mocker, successful=False, finished=False)


def _step(mocker, tasks):
    step = mocker.MagicMock()
    step.__iter__.return_value = iter(tasks)
    return step


def _run(mocker, steps):
    run = mocker.MagicMock()
    run.__iter__.return_value = iter(steps)
    return run


def test_run_failed_task_returns_first_failed_in_iteration_order(mocker):
    ok = _ok(mocker)
    bad = _crashed(mocker)
    run = _run(mocker, [_step(mocker, [ok, bad])])

    assert Run.failed_task.fget(run) is bad


def test_run_failed_task_scans_steps_in_order(mocker):
    ok = _ok(mocker)
    bad = _crashed(mocker)
    later = _crashed(mocker)
    run = _run(mocker, [_step(mocker, [ok]), _step(mocker, [bad, later])])

    assert Run.failed_task.fget(run) is bad


def test_run_failed_task_includes_failures_handled_by_a_decorator(mocker):
    handled = _handled(mocker)
    run = _run(mocker, [_step(mocker, [_ok(mocker), handled])])

    assert Run.failed_task.fget(run) is handled


def test_run_failed_task_skips_tasks_that_have_not_finished(mocker):
    crashed = _crashed(mocker)
    run = _run(mocker, [_step(mocker, [_running(mocker)]), _step(mocker, [crashed])])

    assert Run.failed_task.fget(run) is crashed


def test_run_failed_task_none_when_nothing_has_failed(mocker):
    run = _run(mocker, [_step(mocker, [_ok(mocker), _running(mocker)])])

    assert Run.failed_task.fget(run) is None


# ---------------------------------------------------------------------------
# Flow.failed_runs
# ---------------------------------------------------------------------------


def _flow(mocker):
    # autospec so `runs` enforces the real signature: a wrong keyword raises
    # here instead of being swallowed by a permissive Mock.
    return mocker.create_autospec(Flow, instance=True)


def test_flow_failed_runs_forwards_status_filter_and_bounds(mocker):
    flow = _flow(mocker)
    flow.runs.return_value = iter(["r3", "r2"])

    result = list(Flow.failed_runs(flow, max_runs=2))

    assert result == ["r3", "r2"]
    flow.runs.assert_called_once_with(_filters={"status:eq": "failed"}, max_runs=2)


def test_flow_failed_runs_since_adds_ts_epoch_filter(mocker):
    flow = _flow(mocker)
    flow.runs.return_value = iter([])

    list(Flow.failed_runs(flow, since=1700000000000))

    flow.runs.assert_called_once_with(
        _filters={"status:eq": "failed", "ts_epoch:ge": 1700000000000},
        max_runs=None,
    )


def test_flow_failed_runs_passes_tags_through(mocker):
    flow = _flow(mocker)
    flow.runs.return_value = iter([])

    list(Flow.failed_runs(flow, "prod", "nightly", max_runs=5))

    flow.runs.assert_called_once_with(
        "prod", "nightly", _filters={"status:eq": "failed"}, max_runs=5
    )


def test_flow_failed_runs_returns_the_runs_iterator_directly(mocker):
    flow = _flow(mocker)
    sentinel = iter(["r1"])
    flow.runs.return_value = sentinel

    # Ergonomics: failed_runs hands back exactly what runs() returns, so callers
    # can do `for run in flow.failed_runs(): ...` without an extra wrapper.
    assert Flow.failed_runs(flow) is sentinel


def test_flow_failed_runs_reaches_the_provider_through_the_real_runs(mocker):
    # End to end through the real `Flow.runs`, with only the provider faked, so
    # a drift between the wrapper and the `runs` signature fails here.
    captured = {}

    def fake_iter_children(query_filters=None, page_size=None, required_tags=()):
        captured.update(query_filters=query_filters, required_tags=required_tags)
        yield from ["r3", "r2", "r1"]

    flow = mocker.Mock()
    flow._iter_children = fake_iter_children
    flow.runs = Flow.runs.__get__(flow, Flow)

    runs = list(
        Flow.failed_runs.__get__(flow, Flow)("prod", since=1700000000000, max_runs=2)
    )

    assert runs == ["r3", "r2"]
    assert captured == {
        "query_filters": {"status:eq": "failed", "ts_epoch:ge": 1700000000000},
        "required_tags": ("prod",),
    }


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


def test_failure_summary_is_exported_with_the_other_client_types():
    import metaflow
    import metaflow.client

    assert metaflow.client.FailureSummary is FailureSummary
    assert metaflow.FailureSummary is FailureSummary
