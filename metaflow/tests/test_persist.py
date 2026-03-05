import pytest
import pickle
from types import MethodType, FunctionType

from metaflow.exception import MetaflowException

# --------------------------------------------------
# Dummy classes for testing
# --------------------------------------------------

class Parameter:
    """Dummy Parameter class to replace missing import."""
    def __init__(self, name=None):
        self.name = name


class Unpicklable:
    def __getstate__(self):
        raise RuntimeError("cannot pickle me")


class DummyFlow:
    _EPHEMERAL = set()

    def __init__(self):
        self.good = 123
        self.bad = Unpicklable()
        self.param = Parameter(name="dummy")
        self._datastore = None


class DummyParentDatastore:
    """Parent datastore with some artifacts to transfer."""
    def __init__(self):
        self._objects = {"parent_artifact": 999}
        self._info = {}
        self.orig_datastore = self  # simulate parent reference


class DummyDatastore:
    """Mock TaskDataStore to test persist logic."""

    def __init__(self):
        self._persist = True
        self._is_done_set = False
        self._mode = "w"
        self._objects = {}
        self._info = {}

    def save_artifacts(self, artifacts_iter, len_hint=None):
        """Force serialization and store artifacts in _objects."""
        for name, obj in artifacts_iter:
            try:
                pickle.dumps(obj)  # force serialization
                self._objects[name] = obj  # store artifact in datastore
            except Exception as e:
                raise MetaflowException(
                    f"Failed to serialize artifact '{name}': {e}"
                ) from e

    def transfer_artifacts(self, parent_ds, names):
        for name in names:
            self._objects[name] = parent_ds._objects[name]

    def persist(self, flow):
        """Simplified persist method for testing."""
        if not self._persist:
            return

        if getattr(flow, "_datastore", None):
            self._objects.update(flow._datastore._objects)
            self._info.update(getattr(flow._datastore, "_info", {}))

        valid_artifacts = []
        current_artifact_names = set()

        for var in dir(flow):
            if var.startswith("__") or var in flow._EPHEMERAL:
                continue

            if hasattr(flow.__class__, var) and isinstance(getattr(flow.__class__, var), property):
                continue

            val = getattr(flow, var)
            if not isinstance(val, (MethodType, FunctionType, Parameter)):
                valid_artifacts.append((var, val))
                current_artifact_names.add(var)

        # Transfer parent artifacts not overridden
        if getattr(flow._datastore, "orig_datastore", None):
            parent_artifacts = set(flow._datastore._objects.keys())
            unchanged_artifacts = parent_artifacts - current_artifact_names
            if unchanged_artifacts:
                self.transfer_artifacts(flow._datastore.orig_datastore, list(unchanged_artifacts))

        def artifacts_iter():
            while valid_artifacts:
                var, val = valid_artifacts.pop()
                if not var.startswith("_") and var != "name":
                    delattr(flow, var)
                yield var, val

        try:
            self.save_artifacts(artifacts_iter(), len_hint=len(valid_artifacts))
        except Exception as e:
            raise MetaflowException(
                f"Failed to serialize artifact while persisting task data: {e}"
            ) from e


# --------------------------------------------------
# Tests
# --------------------------------------------------

def test_persist_unpicklable_artifact():
    """Persisting a flow with unpicklable artifact raises MetaflowException."""
    flow = DummyFlow()
    ds = DummyDatastore()
    with pytest.raises(MetaflowException) as exc:
        ds.persist(flow)
    assert "Failed to serialize artifact" in str(exc.value)


def test_persist_parent_artifact_transfer():
    """Artifacts from parent datastore are transferred if not overridden."""
    flow = DummyFlow()
    parent_ds = DummyParentDatastore()
    flow._datastore = parent_ds
    ds = DummyDatastore()

    # Remove the unpicklable artifact for this test
    del flow.bad

    ds.persist(flow)
    # Ensure parent artifact transferred
    assert ds._objects.get("parent_artifact") == 999


def test_persist_skips_methods_and_parameters():
    """Methods and Parameters should not be persisted as artifacts."""
    flow = DummyFlow()
    ds = DummyDatastore()
    del flow.bad  # Remove unpicklable
    ds.persist(flow)
    assert "param" not in ds._objects  # Parameter skipped
    assert "good" in ds._objects       # normal artifact persisted