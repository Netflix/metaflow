"""
Unit tests for metaflow/plugins/timeout_decorator.py

Bug: _sigalrm_handler contained a typo in its stack-frame filter:
    "timeout_decorators.py"  (with trailing 's')
instead of
    "timeout_decorator.py"   (correct filename)

Because the string never matched the real filename, the filter was dead code:
every frame -- including the decorator's own internal frames -- was always
included in the TimeoutException message, polluting user-facing tracebacks.

Fix: correct the string literal so the filter actually matches.
"""

import sys
from unittest.mock import MagicMock

# fcntl is Linux/macOS-only.  Stub it out before any metaflow import so that
# plugin resolution (which loads Linux-targeting decorators) doesn't crash on
# Windows.  This mirrors what the CI environment achieves simply by running on
# Linux; we want the unit test to be cross-platform.
if "fcntl" not in sys.modules:
    sys.modules["fcntl"] = MagicMock()

# Stub the sidecar subprocess module that directly uses fcntl at import time.
if "metaflow.sidecar.sidecar_subprocess" not in sys.modules:
    sys.modules["metaflow.sidecar.sidecar_subprocess"] = MagicMock()

from unittest.mock import patch  # noqa: E402  (must follow the sys.modules stubs)

from metaflow.plugins.timeout_decorator import TimeoutDecorator  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_decorator():
    """Return a minimally configured TimeoutDecorator ready for handler tests."""
    deco = TimeoutDecorator({"seconds": 1, "minutes": 0, "hours": 0}, "timeout")
    deco.secs = 1
    deco.step_name = "myStep"
    logged = []
    deco.logger = lambda msg, **kwargs: logged.append(msg)
    return deco, logged


# ---------------------------------------------------------------------------
# Core regression test
# ---------------------------------------------------------------------------


def test_sigalrm_handler_filters_own_frames():
    """
    Stack frames from timeout_decorator.py must be excluded from the
    TimeoutException message; user frames must be preserved.

    This test FAILS without the fix (the filter checked 'timeout_decorators.py'
    which never matched the real filename) and PASSES with the fix.
    """
    deco, _ = _make_decorator()

    # Simulate traceback.format_stack returning one decorator frame and one user frame.
    fake_stack = [
        '  File "timeout_decorator.py", line 84, in _sigalrm_handler\n'
        "    raise TimeoutException(...)\n",
        '  File "user_flow.py", line 10, in my_step\n' "    time.sleep(9999)\n",
    ]

    exc_message = None
    with patch("traceback.format_stack", return_value=fake_stack):
        try:
            deco._sigalrm_handler(14, None)  # 14 == signal.SIGALRM on Linux
        except Exception as exc:
            exc_message = str(exc)

    assert exc_message is not None, "_sigalrm_handler must raise TimeoutException"

    # The decorator's own frame must be filtered out.
    assert "timeout_decorator.py" not in exc_message, (
        "Decorator's own stack frame leaked into the TimeoutException message. "
        "The filename filter is broken (checked 'timeout_decorators.py' instead "
        "of 'timeout_decorator.py')."
    )

    # The user's frame must still appear.
    assert (
        "user_flow.py" in exc_message
    ), "User's stack frame was incorrectly removed from the TimeoutException message."


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------


def test_sigalrm_handler_message_contains_step_name():
    """TimeoutException message must include the step name."""
    deco, _ = _make_decorator()

    with patch("traceback.format_stack", return_value=[]):
        try:
            deco._sigalrm_handler(14, None)
        except Exception as exc:
            exc_message = str(exc)

    assert "myStep" in exc_message


def test_sigalrm_handler_message_contains_duration():
    """TimeoutException message must state hours/minutes/seconds."""
    deco, _ = _make_decorator()

    with patch("traceback.format_stack", return_value=[]):
        try:
            deco._sigalrm_handler(14, None)
        except Exception as exc:
            exc_message = str(exc)

    assert "hours" in exc_message
    assert "minutes" in exc_message
    assert "seconds" in exc_message


def test_sigalrm_handler_all_decorator_frames_filtered():
    """
    When *all* stack frames come from timeout_decorator.py, the resulting
    stack section in the message must be empty (no decorator frames shown).
    """
    deco, _ = _make_decorator()

    fake_stack = [
        '  File "timeout_decorator.py", line 81, in _sigalrm_handler\n    ...\n',
        '  File "timeout_decorator.py", line 73, in task_pre_step\n    ...\n',
    ]

    with patch("traceback.format_stack", return_value=fake_stack):
        try:
            deco._sigalrm_handler(14, None)
        except Exception as exc:
            exc_message = str(exc)

    assert "timeout_decorator.py" not in exc_message


def test_sigalrm_handler_preserves_multiple_user_frames():
    """All user frames must appear; only decorator frames are stripped."""
    deco, _ = _make_decorator()

    fake_stack = [
        '  File "timeout_decorator.py", line 84, in _sigalrm_handler\n    ...\n',
        '  File "my_flow.py", line 20, in start\n    heavy_computation()\n',
        '  File "helper.py", line 5, in heavy_computation\n    time.sleep(99)\n',
    ]

    with patch("traceback.format_stack", return_value=fake_stack):
        try:
            deco._sigalrm_handler(14, None)
        except Exception as exc:
            exc_message = str(exc)

    assert "timeout_decorator.py" not in exc_message
    assert "my_flow.py" in exc_message
    assert "helper.py" in exc_message
