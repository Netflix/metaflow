"""
Unit test for TimeoutDecorator Windows platform check.

Tests that @timeout raises a clear MetaflowException on platforms
where signal.SIGALRM is not available (e.g., Windows), instead of
letting a cryptic AttributeError propagate.

Uses importlib.util to load modules directly, bypassing the metaflow
package __init__.py which has Linux-only dependencies (fcntl).
"""
import importlib.util
import os
import signal
import sys
import types
import unittest
from unittest.mock import MagicMock

# ── Bootstrap: load the modules we need without triggering metaflow.__init__ ──

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _load_module(name, rel_path):
    """Load a single .py file as a module, registered under `name`."""
    full_path = os.path.join(_ROOT, *rel_path.split("/"))
    spec = importlib.util.spec_from_file_location(name, full_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# First, ensure 'metaflow' exists as a package in sys.modules (but NOT its __init__)
if "metaflow" not in sys.modules:
    metaflow_pkg = types.ModuleType("metaflow")
    metaflow_pkg.__path__ = [os.path.join(_ROOT, "metaflow")]
    metaflow_pkg.__package__ = "metaflow"
    sys.modules["metaflow"] = metaflow_pkg

# Load exception module directly (it has no problematic imports)
_load_module("metaflow.exception", "metaflow/exception.py")

# Stub out dependencies of timeout_decorator
for stub_name in [
    "metaflow.decorators",
    "metaflow.unbounded_foreach",
    "metaflow.metaflow_config",
    "metaflow.plugins",
]:
    if stub_name not in sys.modules:
        sys.modules[stub_name] = MagicMock()

# Set specific attributes that timeout_decorator needs
sys.modules["metaflow.metaflow_config"].DEFAULT_RUNTIME_LIMIT = 3600
sys.modules["metaflow.unbounded_foreach"].UBF_CONTROL = "control"

# Make StepDecorator a simple base class so TimeoutDecorator can inherit
sys.modules["metaflow.decorators"].StepDecorator = type("StepDecorator", (), {})

# Now load the module under test
_load_module("metaflow.plugins.timeout_decorator", "metaflow/plugins/timeout_decorator.py")

from metaflow.exception import MetaflowException
from metaflow.plugins.timeout_decorator import TimeoutDecorator


# ── Tests ──


class TestTimeoutDecoratorPlatformCheck(unittest.TestCase):
    """Test that @timeout raises a clear error on unsupported platforms."""

    def _make_decorator(self, seconds=10):
        """Create a TimeoutDecorator with given attributes."""
        deco = TimeoutDecorator.__new__(TimeoutDecorator)
        deco.attributes = {"hours": 0, "minutes": 0, "seconds": seconds}
        deco.init()
        return deco

    def test_raises_on_platform_without_sigalrm(self):
        """When signal.SIGALRM is absent, step_init should raise MetaflowException."""
        deco = self._make_decorator(seconds=30)

        original = getattr(signal, "SIGALRM", None)
        if original is not None:
            delattr(signal, "SIGALRM")
        try:
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
        finally:
            if original is not None:
                signal.SIGALRM = original

    def test_no_error_on_platform_with_sigalrm(self):
        """When signal.SIGALRM exists, step_init should succeed normally."""
        if not hasattr(signal, "SIGALRM"):
            self.skipTest("SIGALRM not available on this platform")

        deco = self._make_decorator(seconds=30)
        # Should not raise
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

        original = getattr(signal, "SIGALRM", None)
        if original is not None:
            delattr(signal, "SIGALRM")
        try:
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
        finally:
            if original is not None:
                signal.SIGALRM = original

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
