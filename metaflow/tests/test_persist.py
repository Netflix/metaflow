import pytest
from types import SimpleNamespace
from types import MethodType, FunctionType
from metaflow.exception import MetaflowException

# Dummy Parameter class to mimic Metaflow
class Parameter:
    pass

# Dummy Flow class
class DummyFlow:
    _EPHEMERAL = set()
    
    def __init__(self):
        self.good_artifact = "I am serializable"
        self.bad_artifact = NonSerializable()  # Will fail serialization
        self._ignored_artifact = "ignored"
        self.name = "flow_name"
        self._datastore = None

# Artifact that will fail serialization
class NonSerializable:
    def __getstate__(self):
        raise ValueError("Cannot serialize this object!")

# Dummy Runtime with our persist method
class DummyRuntime:
    _persist = True
    _objects = {}
    _info = {}

    def save_artifacts(self, artifacts_iter, len_hint):
        # Simply iterate to simulate saving
        for name, obj in artifacts_iter:
            pass

    def transfer_artifacts(self, orig_datastore, names=None):
        pass

    def persist(self, flow):
        # Include your updated persist function here
        valid_artifacts = []
        current_artifact_names = set()
        for var in dir(flow):
            if var.startswith("__") or var in flow._EPHEMERAL:
                continue
            if hasattr(flow.__class__, var) and isinstance(getattr(flow.__class__, var), property):
                continue

            val = getattr(flow, var)
            if not (isinstance(val, MethodType) or isinstance(val, FunctionType) or isinstance(val, Parameter)):
                valid_artifacts.append((var, val))
                current_artifact_names.add(var)

        def artifacts_iter():
            while valid_artifacts:
                var, val = valid_artifacts.pop()
                if not var.startswith("_") and var != "name":
                    delattr(flow, var)
                try:
                    # Simulate serialization attempt
                    if hasattr(val, "__getstate__"):
                        val.__getstate__()
                except Exception as e:
                    raise MetaflowException(
                        f"Failed to serialize artifact '{var}' of type {type(val).__name__}: {e}"
                    ) from e
                yield var, val

        self.save_artifacts(artifacts_iter(), len_hint=len(valid_artifacts))

# =========================
# TESTS
# =========================

def test_persist_error_message():
    flow = DummyFlow()
    rt = DummyRuntime()

    # Expect an exception when persisting bad_artifact
    with pytest.raises(MetaflowException) as excinfo:
        rt.persist(flow)

    msg = str(excinfo.value)
    assert "bad_artifact" in msg
    assert "NonSerializable" in msg
    assert "Cannot serialize this object!" in msg