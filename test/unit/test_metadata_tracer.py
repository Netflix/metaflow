"""Unit tests for MetadataTracer (metaflow/metadata_provider/tracer.py)."""

import pytest
from metaflow.metadata_provider.tracer import MetadataTracer
from metaflow.metadata_provider.metadata import MetadataProvider


# ---------------------------------------------------------------------------
# Minimal stub that makes get_object() succeed without a real backend
# ---------------------------------------------------------------------------

class _StubProvider(MetadataProvider):
    TYPE = "local"

    @classmethod
    def _get_object_internal(cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args):
        return []


class _FailingProvider(MetadataProvider):
    """Stub that always raises from _get_object_internal."""
    TYPE = "local"

    @classmethod
    def _get_object_internal(cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args):
        raise RuntimeError("backend failure")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _call(obj_type="flow", sub_type="run", attempt=None, *path_args):
    """Invoke _StubProvider.get_object with safe defaults."""
    _StubProvider.get_object(obj_type, sub_type, {}, attempt, *path_args)


def _reset_tracer():
    """Ensure the class-level tracer is cleared between tests."""
    MetadataProvider._tracer = None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMetadataTracerBasic:
    def setup_method(self):
        _reset_tracer()

    def teardown_method(self):
        _reset_tracer()

    def test_no_recording_when_inactive(self):
        """No tracing overhead when no tracer is active."""
        assert MetadataProvider._tracer is None
        _call()
        # The class attribute stays None — no side-effect.
        assert MetadataProvider._tracer is None

    def test_records_calls_inside_context(self):
        """Calls made inside the context manager are recorded."""
        with MetadataTracer() as tracer:
            _call("flow", "run")
            _call("run", "step")

        assert len(tracer.calls) == 2

    def test_call_fields_populated(self):
        """Each recorded call contains the expected keys with correct types."""
        with MetadataTracer() as tracer:
            _call("flow", "run", None, "MyFlow")

        record = tracer.calls[0]
        assert record["obj_type"] == "flow"
        assert record["sub_type"] == "run"
        assert "depth" in record
        assert "path" in record
        assert "attempt" in record
        assert "ts" in record
        assert "elapsed_ms" in record
        assert record["elapsed_ms"] >= 0
        assert record["error"] is None

    def test_no_recording_after_context_exits(self):
        """Calls made after the context manager exits are not recorded."""
        with MetadataTracer() as tracer:
            _call()

        _call()  # outside context — must not be appended
        assert len(tracer.calls) == 1

    def test_tracer_cleared_after_exit(self):
        """MetadataProvider._tracer is None after the context manager exits."""
        with MetadataTracer():
            assert MetadataProvider._tracer is not None

        assert MetadataProvider._tracer is None


class TestMetadataTracerNested:
    def setup_method(self):
        _reset_tracer()

    def teardown_method(self):
        _reset_tracer()

    def test_inner_exit_restores_outer_tracer(self):
        """Exiting an inner tracer must restore the outer tracer, not clear it."""
        with MetadataTracer() as outer:
            _call()  # recorded by outer

            with MetadataTracer() as inner:
                _call()  # recorded by inner

            # After inner exits, outer should still be active
            assert MetadataProvider._tracer is outer, (
                "__exit__ should restore outer tracer, not set _tracer to None"
            )
            _call()  # should be recorded by outer

        assert len(outer.calls) == 2, "outer should have 2 calls (before + after inner)"
        assert len(inner.calls) == 1, "inner should have exactly 1 call"

    def test_outer_tracer_cleared_after_both_exit(self):
        """_tracer is None only after the outermost context manager exits."""
        with MetadataTracer() as outer:
            with MetadataTracer():
                pass
            assert MetadataProvider._tracer is outer

        assert MetadataProvider._tracer is None

    def test_triple_nesting(self):
        """Three levels of nesting all restore correctly."""
        with MetadataTracer() as t1:
            with MetadataTracer() as t2:
                with MetadataTracer() as t3:
                    assert MetadataProvider._tracer is t3

                assert MetadataProvider._tracer is t2

            assert MetadataProvider._tracer is t1

        assert MetadataProvider._tracer is None


class TestMetadataTracerErrors:
    def setup_method(self):
        _reset_tracer()

    def teardown_method(self):
        _reset_tracer()

    def test_failed_call_recorded_with_error_field(self):
        """When the backend raises, the call is still recorded with error set."""
        with MetadataTracer() as tracer:
            with pytest.raises(RuntimeError):
                _FailingProvider.get_object("flow", "run", {}, None)

        assert len(tracer.calls) == 1
        assert isinstance(tracer.calls[0]["error"], RuntimeError)

    def test_successful_call_has_no_error(self):
        """Successful calls have error=None."""
        with MetadataTracer() as tracer:
            _call()

        assert tracer.calls[0]["error"] is None

    def test_failed_call_has_elapsed_ms(self):
        """Even failed calls record a non-negative elapsed_ms."""
        with MetadataTracer() as tracer:
            with pytest.raises(RuntimeError):
                _FailingProvider.get_object("flow", "run", {}, None)

        assert tracer.calls[0]["elapsed_ms"] >= 0

    def test_exception_still_propagates(self):
        """The original exception must not be swallowed by the tracer."""
        with MetadataTracer():
            with pytest.raises(RuntimeError, match="backend failure"):
                _FailingProvider.get_object("flow", "run", {}, None)

    def test_mixed_calls_recorded_in_order(self):
        """A mix of successful and failed calls are recorded in call order."""
        with MetadataTracer() as tracer:
            _call()  # success
            with pytest.raises(RuntimeError):
                _FailingProvider.get_object("flow", "run", {}, None)  # failure
            _call()  # success

        assert len(tracer.calls) == 3
        assert tracer.calls[0]["error"] is None
        assert isinstance(tracer.calls[1]["error"], RuntimeError)
        assert tracer.calls[2]["error"] is None



    def setup_method(self):
        _reset_tracer()

    def teardown_method(self):
        _reset_tracer()

    def test_summary_empty(self):
        tracer = MetadataTracer()
        s = tracer.summary()
        assert s == {"total": 0, "by_type": {}}

    def test_summary_counts(self):
        with MetadataTracer() as tracer:
            _call("flow", "run")
            _call("flow", "run")
            _call("run", "step")

        s = tracer.summary()
        assert s["total"] == 3
        assert s["by_type"]["flow"] == 2
        assert s["by_type"]["run"] == 1

    def test_report_prints(self, capsys):
        with MetadataTracer() as tracer:
            _call("flow", "run")

        tracer.report()
        captured = capsys.readouterr()
        assert "MetadataTracer" in captured.out
        assert "flow" in captured.out


class TestMetadataTracerPathRecording:
    def setup_method(self):
        _reset_tracer()

    def teardown_method(self):
        _reset_tracer()

    def test_path_joined_from_args(self):
        with MetadataTracer() as tracer:
            _call("flow", "run", None, "MyFlow", "42")

        assert tracer.calls[0]["path"] == "MyFlow/42"

    def test_none_args_excluded_from_path(self):
        with MetadataTracer() as tracer:
            _call("flow", "run", None, "MyFlow", None)

        assert tracer.calls[0]["path"] == "MyFlow"

    def test_attempt_recorded(self):
        with MetadataTracer() as tracer:
            _StubProvider.get_object("task", "artifact", {}, 2, "MyFlow", "1", "start", "1")

        assert tracer.calls[0]["attempt"] == 2

    def test_attempt_none_recorded(self):
        with MetadataTracer() as tracer:
            _call("flow", "run", None)

        assert tracer.calls[0]["attempt"] is None
