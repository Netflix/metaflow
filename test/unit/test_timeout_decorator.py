"""
Unit test for TimeoutDecorator Windows platform check.

Tests that @timeout raises a clear MetaflowException on platforms
where signal.SIGALRM is not available (e.g., Windows), instead of
letting a cryptic AttributeError propagate.
"""
import signal
import sys
import unittest
from unittest.mock import MagicMock, patch

from metaflow.exception import MetaflowException
from metaflow.plugins.timeout_decorator import TimeoutDecorator


class TestTimeoutDecoratorPlatformCheck(unittest.TestCase):
    """Test that @timeout raises a clear error on unsupported platforms."""

    def _make_decorator(self, seconds=10):
        """Create a TimeoutDecorator with the given timeout duration."""
        deco = TimeoutDecorator.__new__(TimeoutDecorator)
        deco.attributes = {"hours": 0, "minutes": 0, "seconds": seconds}
        deco.init()
        return deco

    def test_raises_on_platform_without_sigalrm(self):
        """When signal.SIGALRM is absent, step_init should raise MetaflowException."""
        deco = self._make_decorator(seconds=30)

        # Mock the signal module as seen by timeout_decorator — without SIGALRM.
        # spec=[] means the mock has NO attributes, so hasattr(..., 'SIGALRM') is False.
        mock_signal = MagicMock(spec=[])
        with patch("metaflow.plugins.timeout_decorator.signal", mock_signal):
            with self.assertRaises(MetaflowException) as ctx:
                deco.step_init(
                    flow=MagicMock(),
                    graph=MagicMock(),
                    step="my_step",
                    decos=[],
                    environment=MagicMock(),
                    flow_datastore=MagicMock(),
                    logger=MagicMock(),
                )
            error_msg = str(ctx.exception)
            self.assertIn("@timeout", error_msg)
            self.assertIn("not supported", error_msg)
            self.assertIn("SIGALRM", error_msg)
            self.assertIn("my_step", error_msg)

    def test_no_error_on_platform_with_sigalrm(self):
        """When signal.SIGALRM exists, step_init should succeed normally."""
        if not hasattr(signal, "SIGALRM"):
            self.skipTest("SIGALRM not available on this platform")

        deco = self._make_decorator(seconds=30)

        # No mocking needed — real signal module has SIGALRM on Linux/macOS.
        deco.step_init(
            flow=MagicMock(),
            graph=MagicMock(),
            step="my_step",
            decos=[],
            environment=MagicMock(),
            flow_datastore=MagicMock(),
            logger=MagicMock(),
        )

    def test_error_message_includes_platform(self):
        """The error message should include the platform name for debugging."""
        deco = self._make_decorator(seconds=30)

        mock_signal = MagicMock(spec=[])
        with patch("metaflow.plugins.timeout_decorator.signal", mock_signal):
            with self.assertRaises(MetaflowException) as ctx:
                deco.step_init(
                    flow=MagicMock(),
                    graph=MagicMock(),
                    step="test_step",
                    decos=[],
                    environment=MagicMock(),
                    flow_datastore=MagicMock(),
                    logger=MagicMock(),
                )
            self.assertIn(sys.platform, str(ctx.exception))

    def test_zero_seconds_raises_duration_error(self):
        """step_init should raise MetaflowException if timeout duration is 0."""
        deco = self._make_decorator(seconds=0)

        with self.assertRaises(MetaflowException) as ctx:
            deco.step_init(
                flow=MagicMock(),
                graph=MagicMock(),
                step="my_step",
                decos=[],
                environment=MagicMock(),
                flow_datastore=MagicMock(),
                logger=MagicMock(),
            )
        self.assertIn("duration", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
