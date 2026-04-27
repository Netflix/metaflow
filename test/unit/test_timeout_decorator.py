"""Regression tests for the stack-frame filter in
TimeoutDecorator._sigalrm_handler (issue #3016).

The filter was dead code due to a filename typo (``timeout_decorators.py``
with a trailing ``s``), which let internal decorator frames leak into the
user-facing TimeoutException message.
"""

import os
import signal

import pytest

from metaflow.plugins.timeout_decorator import TimeoutDecorator, TimeoutException


@pytest.fixture
def make_decorator():
    """Factory: build a minimally-initialized TimeoutDecorator."""

    def _build(step_name="my_step", seconds=1):
        deco = TimeoutDecorator({"seconds": seconds, "minutes": 0, "hours": 0})
        deco.secs = seconds
        deco.step_name = step_name
        deco.logger = lambda msg: None
        return deco

    return _build


def test_stack_section_excludes_decorator_frames(make_decorator):
    """Filtered stack strips production decorator frames but keeps caller frames."""
    deco = make_decorator()
    with pytest.raises(TimeoutException) as exc_info:
        deco._sigalrm_handler(signal.SIGALRM, None)

    message = str(exc_info.value)
    marker = "Stack when the timeout was raised:\n"
    assert marker in message
    stack_section = message.split(marker, 1)[1]

    # Production decorator's frames are stripped.
    decorator_path = os.path.join("metaflow", "plugins", "timeout_decorator.py")
    for line in stack_section.splitlines():
        assert (
            decorator_path not in line
        ), f"Internal decorator frame leaked into the stack: {line!r}"

    # Caller's frame (this test file) is preserved. Under the old
    # basename-based filter, "test_timeout_decorator.py" would match
    # "timeout_decorator.py" as a substring and be incorrectly stripped.
    assert (
        "test_timeout_decorator.py" in stack_section
    ), "Caller frame was incorrectly filtered out of the stack"


@pytest.mark.parametrize(
    "step_name, seconds, expected_substrings",
    [
        ("train_model", 42, ["train_model", "timed out"]),
        ("my_step", 10, ["10 seconds", "timed out"]),
    ],
    ids=["step_name", "timeout_values"],
)
def test_message_contains_step_info(
    make_decorator, step_name, seconds, expected_substrings
):
    """Exception message includes the step name and the configured timeout."""
    deco = make_decorator(step_name=step_name, seconds=seconds)
    with pytest.raises(TimeoutException) as exc_info:
        deco._sigalrm_handler(signal.SIGALRM, None)

    message = str(exc_info.value)
    for expected in expected_substrings:
        assert expected in message
