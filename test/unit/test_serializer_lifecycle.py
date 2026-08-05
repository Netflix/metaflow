"""Tests for the serializer lifecycle (state machine, bootstrap, diagnostics)."""

import sys
import types

import pytest

from metaflow.datastore.artifacts.diagnostic import (
    SerializerRecord,
    SerializerState,
)
from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializerStore,
    SerializationFormat,
)
from metaflow import metaflow_config


# --- Fixtures ---


@pytest.fixture
def isolated_store():
    """
    Fixture to isolate SerializerStore global state per test.
    Provides a clean slate and restores original state afterward.
    """
    # Snapshot original state
    saved_all = dict(SerializerStore._all_serializers)
    saved_active = set(SerializerStore._active_serializers)
    saved_records = dict(SerializerStore._records)
    saved_pending = dict(SerializerStore._pending_by_module)
    saved_cache = SerializerStore._ordered_cache

    # Clear state for test isolation
    SerializerStore._all_serializers.clear()
    SerializerStore._active_serializers.clear()
    SerializerStore._records.clear()
    SerializerStore._pending_by_module.clear()
    SerializerStore._ordered_cache = None

    yield

    # Restore original state
    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(saved_all)
    SerializerStore._active_serializers.clear()
    SerializerStore._active_serializers.update(saved_active)
    SerializerStore._records.clear()
    SerializerStore._records.update(saved_records)
    SerializerStore._pending_by_module.clear()
    SerializerStore._pending_by_module.update(saved_pending)
    SerializerStore._ordered_cache = saved_cache


@pytest.fixture
def make_serializer_module(monkeypatch):
    """
    Factory fixture to dynamically create a module from source code
    and register it. Automatically cleans up sys.modules via monkeypatch.
    """

    def _make(module_name, source_code):
        mod = types.ModuleType(module_name)
        exec(source_code, mod.__dict__)
        monkeypatch.setitem(sys.modules, module_name, mod)
        return mod

    return _make


# --- Tests ---


def test_serializer_record_default_fields():
    rec = SerializerRecord(name="pickle", class_path="m.pkl.Pickle")
    assert rec.name == "pickle"
    assert rec.class_path == "m.pkl.Pickle"
    assert rec.state == SerializerState.KNOWN
    assert rec.awaiting_modules == []
    assert rec.last_error is None
    assert rec.priority is None
    assert rec.type is None
    assert rec.import_trigger is None
    assert rec.dispatch_error_count == 0


def test_serializer_record_as_dict():
    rec = SerializerRecord(
        name="torch",
        class_path="ext.t.TorchSerializer",
        state=SerializerState.ACTIVE,
        awaiting_modules=[],
        last_error=None,
        priority=50,
        type="torch",
        import_trigger="eager",
        dispatch_error_count=0,
    )
    d = rec.as_dict()
    assert d["name"] == "torch"
    assert d["class_path"] == "ext.t.TorchSerializer"
    assert d["state"] == "active"
    assert d["priority"] == 50
    assert d["type"] == "torch"
    assert d["import_trigger"] == "eager"


def test_store_separates_all_vs_active(isolated_store):
    """_all_serializers is the known-classes index; _active_serializers is dispatch pool."""

    class _Known(ArtifactSerializer):
        TYPE = "test_known_not_active"

        @classmethod
        def can_serialize(cls, obj):
            return False

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    # Metaclass registers on class body execution
    assert _Known in SerializerStore._all_serializers.values()
    # But without bootstrap, it is NOT in the dispatch pool
    assert _Known not in SerializerStore._active_serializers
    # _records is an empty dict initially (for entries from DESC tuples)
    assert isinstance(SerializerStore._records, dict)


def test_bootstrap_activates_dependency_free_serializer(
    isolated_store, make_serializer_module
):
    """bootstrap_entries with an in-process serializer moves it to ACTIVE."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _BootProbe(ArtifactSerializer):
    TYPE = "test_bootstrap_probe"
    PRIORITY = 60

    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    mod = make_serializer_module("_test_bootstrap_mod", source)
    SerializerStore.bootstrap_entries(
        [("test_bootstrap_probe", "_test_bootstrap_mod._BootProbe")]
    )

    rec = SerializerStore._records["test_bootstrap_probe"]
    assert rec.state == SerializerState.ACTIVE
    assert rec.priority == 60
    assert rec.type == "test_bootstrap_probe"
    assert rec.import_trigger == "eager"
    assert mod._BootProbe in SerializerStore._active_serializers


def test_bootstrap_rejects_name_type_mismatch(isolated_store, make_serializer_module):
    """Tuple first element MUST equal class.TYPE."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Mismatch(ArtifactSerializer):
    TYPE = "actual_type"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    make_serializer_module("_test_mismatch_mod", source)
    SerializerStore.bootstrap_entries(
        [("declared_name", "_test_mismatch_mod._Mismatch")]
    )

    rec = SerializerStore._records["declared_name"]
    assert rec.state == SerializerState.BROKEN
    assert "tuple name" in rec.last_error
    assert "actual_type" in rec.last_error


def test_bootstrap_missing_module_parks_entry(isolated_store):
    """ModuleNotFoundError during import_module moves entry to PENDING_ON_IMPORTS."""
    SerializerStore.bootstrap_entries(
        [("test_absent", "_never_created_module._Absent")]
    )

    rec = SerializerStore._records["test_absent"]
    assert rec.state == SerializerState.PENDING_ON_IMPORTS
    assert "_never_created_module" in rec.awaiting_modules
    assert "test_absent" in SerializerStore._pending_by_module.get(
        "_never_created_module", []
    )


def test_bootstrap_missing_class_in_module_broken(
    isolated_store, make_serializer_module
):
    """getattr failure after successful module import moves to BROKEN."""
    # Intentionally empty module — no class inside
    make_serializer_module("_test_no_class_mod", "")
    SerializerStore.bootstrap_entries(
        [("test_no_class", "_test_no_class_mod._Missing")]
    )

    rec = SerializerStore._records["test_no_class"]
    assert rec.state == SerializerState.BROKEN
    assert "class" in rec.last_error.lower()
    assert "_Missing" in rec.last_error


def test_bootstrap_setup_imports_missing_dep_parks_entry(
    isolated_store, make_serializer_module
):
    """ImportError inside setup_imports parks on the missing module name."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _WantsMissing(ArtifactSerializer):
    TYPE = "test_setup_wants_missing"
    @classmethod
    def setup_imports(cls, context=None): cls.lazy_import("absent_at_setup_time_xyz")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    make_serializer_module("_test_setup_missing_mod", source)
    SerializerStore.bootstrap_entries(
        [("test_setup_wants_missing", "_test_setup_missing_mod._WantsMissing")]
    )

    rec = SerializerStore._records["test_setup_wants_missing"]
    assert rec.state == SerializerState.PENDING_ON_IMPORTS
    assert "absent_at_setup_time_xyz" in rec.awaiting_modules


def test_bootstrap_setup_imports_other_exception_broken(
    isolated_store, make_serializer_module
):
    """Non-ImportError from setup_imports moves entry to BROKEN."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Boom(ArtifactSerializer):
    TYPE = "test_boom"
    @classmethod
    def setup_imports(cls, context=None): raise RuntimeError("explicit boom from test")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    make_serializer_module("_test_setup_boom_mod", source)
    SerializerStore.bootstrap_entries([("test_boom", "_test_setup_boom_mod._Boom")])

    rec = SerializerStore._records["test_boom"]
    assert rec.state == SerializerState.BROKEN
    assert "RuntimeError" in rec.last_error
    assert "explicit boom" in rec.last_error


def test_bootstrap_disabled_toggle(isolated_store, make_serializer_module):
    """Entries whose name is in disabled_names land in DISABLED state."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _DisableMe(ArtifactSerializer):
    TYPE = "test_disable"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    mod = make_serializer_module("_test_disable_mod", source)
    SerializerStore.bootstrap_entries(
        [("test_disable", "_test_disable_mod._DisableMe")],
        disabled_names={"test_disable"},
    )

    rec = SerializerStore._records["test_disable"]
    assert rec.state == SerializerState.DISABLED
    assert mod._DisableMe not in SerializerStore._active_serializers


def test_retry_activates_pending_record_on_module_import(
    isolated_store, make_serializer_module, monkeypatch
):
    """When a pending record's awaited module imports, the record retries to ACTIVE."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Pending(ArtifactSerializer):
    TYPE = "test_retry_pending"
    @classmethod
    def setup_imports(cls, context=None): cls.lazy_import("retry_dep_mod_name")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    ser_mod = make_serializer_module("_test_retry_ser_mod", source)
    monkeypatch.delitem(sys.modules, "retry_dep_mod_name", raising=False)

    SerializerStore.bootstrap_entries(
        [("test_retry_pending", "_test_retry_ser_mod._Pending")]
    )

    rec = SerializerStore._records["test_retry_pending"]
    assert rec.state == SerializerState.PENDING_ON_IMPORTS
    assert "retry_dep_mod_name" in rec.awaiting_modules
    assert (
        "test_retry_pending" in SerializerStore._pending_by_module["retry_dep_mod_name"]
    )

    # Simulate the dep becoming available, then fire the retry hook.
    dep_mod = types.ModuleType("retry_dep_mod_name")
    monkeypatch.setitem(sys.modules, "retry_dep_mod_name", dep_mod)
    SerializerStore._on_module_imported("retry_dep_mod_name", dep_mod)

    assert rec.state == SerializerState.ACTIVE
    assert rec.import_trigger == "hook"
    assert ser_mod._Pending in SerializerStore._active_serializers
    assert "test_retry_pending" not in SerializerStore._pending_by_module.get(
        "retry_dep_mod_name", []
    )


def test_retry_hits_loop_guard_after_repeated_failure(
    isolated_store, make_serializer_module, monkeypatch
):
    """Calling _on_module_imported when setup_imports still fails on the same
    module name should transition to BROKEN via the loop guard."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Loopy(ArtifactSerializer):
    TYPE = "test_loopy"
    @classmethod
    def setup_imports(cls, context=None): cls.lazy_import("never_resolves_mod")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    make_serializer_module("_test_loop_ser_mod", source)
    monkeypatch.delitem(sys.modules, "never_resolves_mod", raising=False)

    SerializerStore.bootstrap_entries([("test_loopy", "_test_loop_ser_mod._Loopy")])

    rec = SerializerStore._records["test_loopy"]
    assert rec.state == SerializerState.PENDING_ON_IMPORTS

    # Fake the dep appearing but DO NOT actually put it in sys.modules,
    # so lazy_import will raise ModuleNotFoundError again.
    dep_mod = types.ModuleType("never_resolves_mod")
    SerializerStore._on_module_imported("never_resolves_mod", dep_mod)

    assert rec.state == SerializerState.BROKEN
    assert "repeated" in rec.last_error.lower()


def test_retry_fires_via_real_import_hook(
    tmp_path, monkeypatch, isolated_store, make_serializer_module
):
    """End-to-end: park a serializer on a missing module, install the hook,
    actually import the module, verify the serializer activates."""
    # Setup test package directory to act as an importable module
    pkg_dir = tmp_path / "fixture_retry_dep"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("VALUE = 42\n")

    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _E2E(ArtifactSerializer):
    TYPE = "test_e2e_retry"
    @classmethod
    def setup_imports(cls, context=None): cls.lazy_import("fixture_retry_dep")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    ser_mod = make_serializer_module("_test_e2e_ser_mod", source)
    monkeypatch.delitem(sys.modules, "fixture_retry_dep", raising=False)

    SerializerStore.bootstrap_entries([("test_e2e_retry", "_test_e2e_ser_mod._E2E")])

    rec = SerializerStore._records["test_e2e_retry"]
    assert rec.state == SerializerState.PENDING_ON_IMPORTS

    # Make the dep discoverable on sys.path and actually import it
    monkeypatch.syspath_prepend(str(tmp_path))
    import fixture_retry_dep  # noqa: F401

    assert rec.state == SerializerState.ACTIVE
    assert rec.import_trigger == "hook"
    assert ser_mod._E2E in SerializerStore._active_serializers

    # Cleanup interceptor state to prevent leaking to other tests
    from metaflow.datastore.artifacts.lazy_registry import _interceptor

    _interceptor._watched.discard("fixture_retry_dep")
    _interceptor._processed.discard("fixture_retry_dep")


def test_bootstrap_with_no_extensions_still_runs_core(isolated_store):
    """bootstrap() reads core ARTIFACT_SERIALIZERS_DESC from metaflow.plugins
    and activates PickleSerializer."""
    SerializerStore.bootstrap()

    # PickleSerializer should be in active pool (core entry)
    pickle_active = any(
        r.type == "pickle" and r.state == SerializerState.ACTIVE
        for r in SerializerStore._records.values()
    )
    assert pickle_active


def test_bootstrap_stamps_core_source_on_record(isolated_store):
    """Core serializers bootstrap with source='metaflow' on their records."""
    SerializerStore.bootstrap()

    pickle_rec = next(
        (r for r in SerializerStore._records.values() if r.type == "pickle"),
        None,
    )
    assert pickle_rec is not None
    assert pickle_rec.source == "metaflow"


def test_bootstrap_entries_accepts_source_override(
    isolated_store, make_serializer_module
):
    """bootstrap_entries accepts ``source`` and attaches it to each record."""
    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _SourceProbe(ArtifactSerializer):
    TYPE = "test_source_probe"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    ser_mod = make_serializer_module("_test_source_ser_mod", source)

    SerializerStore.bootstrap_entries(
        [("test_source_probe", "_test_source_ser_mod._SourceProbe")],
        source="fake-extension",
    )

    rec = SerializerStore._records["test_source_probe"]
    assert rec.source == "fake-extension"
    assert SerializerStore.get_source_for(ser_mod._SourceProbe) == "fake-extension"


def test_bootstrap_applies_disabled_toggle(isolated_store, monkeypatch):
    """bootstrap() respects -name toggles in ENABLED_ARTIFACT_SERIALIZER config."""
    monkeypatch.setattr(
        metaflow_config, "ENABLED_ARTIFACT_SERIALIZER", ["-pickle"], raising=False
    )
    SerializerStore.bootstrap()

    pickle_rec = next(
        (r for r in SerializerStore._records.values() if r.name == "pickle"),
        None,
    )
    assert pickle_rec is not None
    assert pickle_rec.state == SerializerState.DISABLED


def test_list_serializer_status_returns_dicts(isolated_store):
    """list_serializer_status returns one dict per _records entry, with the documented shape."""
    from metaflow.datastore.artifacts import list_serializer_status

    # Seed a fake record
    rec = SerializerRecord(
        name="fake_test_serializer",
        class_path="inline.FakeSer",
        state=SerializerState.ACTIVE,
        priority=42,
        type="fake_test_serializer",
        import_trigger="eager",
    )
    SerializerStore._records["fake_test_serializer"] = rec

    status = list_serializer_status()
    assert isinstance(status, list)

    match = next((s for s in status if s["name"] == "fake_test_serializer"), None)
    assert match is not None
    assert match["state"] == "active"
    assert match["priority"] == 42
    assert match["type"] == "fake_test_serializer"
    assert match["import_trigger"] == "eager"
    assert match["class_path"] == "inline.FakeSer"

    expected_keys = [
        "name",
        "class_path",
        "state",
        "awaiting_modules",
        "last_error",
        "priority",
        "type",
        "import_trigger",
        "dispatch_error_count",
    ]
    for key in expected_keys:
        assert key in match, f"missing key '{key}' in status dict"


def test_reset_for_tests_clears_registry_state(isolated_store, make_serializer_module):
    """SerializerStore._reset_for_tests clears _records, _active_serializers,
    _pending_by_module, _ordered_cache, and per-class _lazy_imported_names."""
    from metaflow.datastore.artifacts.lazy_registry import _interceptor

    source = """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _ResetProbe(ArtifactSerializer):
    TYPE = "test_reset_probe"
    @classmethod
    def setup_imports(cls, context=None): cls.lazy_import("json")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE): raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE): raise NotImplementedError
"""
    ser_mod = make_serializer_module("_test_reset_ser_mod", source)

    # Seed state by bootstrapping inline serializers
    SerializerStore.bootstrap_entries(
        [("test_reset_probe", "_test_reset_ser_mod._ResetProbe")]
    )
    SerializerStore.bootstrap_entries(
        [("test_reset_pending", "_never_exists_mod._Absent")]
    )

    # Snapshot pre-reset state assertion
    assert "test_reset_probe" in SerializerStore._records
    assert ser_mod._ResetProbe in SerializerStore._active_serializers
    assert "_never_exists_mod" in SerializerStore._pending_by_module
    assert "json" in getattr(ser_mod._ResetProbe, "_lazy_imported_names", set())
    assert "_never_exists_mod" in _interceptor._watched

    # Act
    SerializerStore._reset_for_tests()

    # Assert post-reset state is empty
    assert SerializerStore._records == {}
    assert len(SerializerStore._active_serializers) == 0
    assert SerializerStore._pending_by_module == {}
    assert SerializerStore._ordered_cache is None

    # The probe class should no longer have stashed attrs
    assert "json" not in ser_mod._ResetProbe.__dict__
    assert getattr(ser_mod._ResetProbe, "_lazy_imported_names", set()) == set()
    assert "_never_exists_mod" not in _interceptor._watched
