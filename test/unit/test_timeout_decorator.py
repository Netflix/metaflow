"""Tests for the stack-frame filter in TimeoutDecorator._sigalrm_handler.

Regression tests for issue #3016: the timeout decorator's stack filter was
dead code due to a filename typo ("timeout_decorators.py" instead of
"timeout_decorator.py"), causing internal decorator frames to leak into the
user-facing TimeoutException message.

Note on substring matching:
    The production filter uses ``_THIS_FILE not in line`` where _THIS_FILE
    resolves to "timeout_decorator.py".  Because this test file is named
    "test_timeout_decorator.py", which *contains* "timeout_decorator.py" as
    a substring, frames originating from this test file are also filtered
    out by ``pretty_print_stack``.  This does not weaken the regression
    test — the test still fails if the typo is reintroduced (because then
    internal decorator frames would reappear).  The substring-based
    filtering is a pre-existing design choice preserved by this fix.
"""

import signal

import pytest

from metaflow.plugins.timeout_decorator import TimeoutDecorator, TimeoutException


def _make_decorator(step_name="my_step", seconds=1):
    """Create a minimal TimeoutDecorator instance for testing."""
    deco = TimeoutDecorator({"seconds": seconds, "minutes": 0, "hours": 0})
    deco.secs = seconds
    deco.step_name = step_name
    deco.logger = lambda msg: None  # no-op logger
    return deco


class TestStackFilterExcludesInternalFrames:
    """The filtered stack in the exception message must not contain
    frames from timeout_decorator.py itself."""

    def test_stack_section_excludes_timeout_decorator_frames(self):
        deco = _make_decorator()
        with pytest.raises(TimeoutException) as exc_info:
            deco._sigalrm_handler(signal.SIGALRM, None)

        message = str(exc_info.value)
        # Extract only the stack section after the header line
        marker = "Stack when the timeout was raised:\n"
        assert marker in message, "Expected stack header in exception message"
        stack_section = message.split(marker, 1)[1]

        # No line in the filtered stack should reference the decorator file
        for line in stack_section.splitlines():
            assert "timeout_decorator.py" not in line, (
                f"Internal frame leaked into user-facing stack: {line!r}"
            )


class TestTimeoutMessageContainsStepInfo:
    """The exception message must still include the step name and
    the configured timeout duration."""

    def test_message_contains_step_name(self):
        deco = _make_decorator(step_name="train_model", seconds=42)
        with pytest.raises(TimeoutException) as exc_info:
            deco._sigalrm_handler(signal.SIGALRM, None)

        message = str(exc_info.value)
        assert "train_model" in message

    def test_message_contains_timeout_values(self):
        deco = _make_decorator(seconds=10)
        with pytest.raises(TimeoutException) as exc_info:
            deco._sigalrm_handler(signal.SIGALRM, None)

        message = str(exc_info.value)
        assert "10 seconds" in message
        assert "timed out" in message
