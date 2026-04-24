"""Tests for the serializer lifecycle (state machine, bootstrap, diagnostics)."""

import sys
import types

import pytest

from metaflow.datastore.artifacts.diagnostic import (
    SerializerRecord,
    SerializerState,
)


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


from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializerStore,
    SerializationFormat,
)


def test_store_separates_all_vs_active():
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

    try:
        # Metaclass registers on class body execution
        assert _Known in SerializerStore._all_serializers.values()
        # But without bootstrap, it is NOT in the dispatch pool
        assert _Known not in SerializerStore._active_serializers
        # _records is an empty dict initially (for entries from DESC tuples)
        assert isinstance(SerializerStore._records, dict)
    finally:
        SerializerStore._all_serializers.pop("test_known_not_active", None)
        SerializerStore._active_serializers.discard(_Known)
        SerializerStore._ordered_cache = None


def test_bootstrap_activates_dependency_free_serializer():
    """bootstrap_entries with an in-process serializer moves it to ACTIVE."""

    mod = types.ModuleType("_test_bootstrap_mod")
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
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
"""
    exec(source, mod.__dict__)
    sys.modules["_test_bootstrap_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_bootstrap_probe", "_test_bootstrap_mod._BootProbe"),
            ]
        )
        rec = SerializerStore._records["test_bootstrap_probe"]
        assert rec.state == SerializerState.ACTIVE
        assert rec.priority == 60
        assert rec.type == "test_bootstrap_probe"
        assert rec.import_trigger == "eager"
        assert mod._BootProbe in SerializerStore._active_serializers
    finally:
        SerializerStore._all_serializers.pop("test_bootstrap_probe", None)
        SerializerStore._active_serializers.discard(mod._BootProbe)
        SerializerStore._records.pop("test_bootstrap_probe", None)
        SerializerStore._ordered_cache = None
        del sys.modules["_test_bootstrap_mod"]


def test_bootstrap_rejects_name_type_mismatch():
    """Tuple first element MUST equal class.TYPE."""

    mod = types.ModuleType("_test_mismatch_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Mismatch(ArtifactSerializer):
    TYPE = "actual_type"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        mod.__dict__,
    )
    sys.modules["_test_mismatch_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [
                ("declared_name", "_test_mismatch_mod._Mismatch"),
            ]
        )
        rec = SerializerStore._records["declared_name"]
        assert rec.state == SerializerState.BROKEN
        assert "tuple name" in rec.last_error
        assert "actual_type" in rec.last_error
    finally:
        SerializerStore._all_serializers.pop("actual_type", None)
        SerializerStore._records.pop("declared_name", None)
        del sys.modules["_test_mismatch_mod"]


def test_bootstrap_missing_module_parks_entry():
    """ModuleNotFoundError during import_module moves entry to PENDING_ON_IMPORTS."""

    SerializerStore.bootstrap_entries(
        [
            ("test_absent", "_never_created_module._Absent"),
        ]
    )
    try:
        rec = SerializerStore._records["test_absent"]
        assert rec.state == SerializerState.PENDING_ON_IMPORTS
        assert "_never_created_module" in rec.awaiting_modules
        # _pending_by_module should track it
        assert "test_absent" in SerializerStore._pending_by_module.get(
            "_never_created_module", []
        )
    finally:
        SerializerStore._records.pop("test_absent", None)
        SerializerStore._pending_by_module.pop("_never_created_module", None)


def test_bootstrap_missing_class_in_module_broken():
    """getattr failure after successful module import moves to BROKEN."""
    mod = types.ModuleType("_test_no_class_mod")
    # Intentionally empty — no class inside
    sys.modules["_test_no_class_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_no_class", "_test_no_class_mod._Missing"),
            ]
        )
        rec = SerializerStore._records["test_no_class"]
        assert rec.state == SerializerState.BROKEN
        assert "class" in rec.last_error.lower()
        assert "_Missing" in rec.last_error
    finally:
        SerializerStore._records.pop("test_no_class", None)
        del sys.modules["_test_no_class_mod"]


def test_bootstrap_setup_imports_missing_dep_parks_entry():
    """ImportError inside setup_imports parks on the missing module name."""
    mod = types.ModuleType("_test_setup_missing_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _WantsMissing(ArtifactSerializer):
    TYPE = "test_setup_wants_missing"
    @classmethod
    def setup_imports(cls, context=None):
        cls.lazy_import("absent_at_setup_time_xyz")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        mod.__dict__,
    )
    sys.modules["_test_setup_missing_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_setup_wants_missing", "_test_setup_missing_mod._WantsMissing"),
            ]
        )
        rec = SerializerStore._records["test_setup_wants_missing"]
        assert rec.state == SerializerState.PENDING_ON_IMPORTS
        assert "absent_at_setup_time_xyz" in rec.awaiting_modules
    finally:
        SerializerStore._all_serializers.pop("test_setup_wants_missing", None)
        SerializerStore._records.pop("test_setup_wants_missing", None)
        SerializerStore._pending_by_module.pop("absent_at_setup_time_xyz", None)
        del sys.modules["_test_setup_missing_mod"]


def test_bootstrap_setup_imports_other_exception_broken():
    """Non-ImportError from setup_imports moves entry to BROKEN."""
    mod = types.ModuleType("_test_setup_boom_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Boom(ArtifactSerializer):
    TYPE = "test_boom"
    @classmethod
    def setup_imports(cls, context=None):
        raise RuntimeError("explicit boom from test")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        mod.__dict__,
    )
    sys.modules["_test_setup_boom_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_boom", "_test_setup_boom_mod._Boom"),
            ]
        )
        rec = SerializerStore._records["test_boom"]
        assert rec.state == SerializerState.BROKEN
        assert "RuntimeError" in rec.last_error
        assert "explicit boom" in rec.last_error
    finally:
        SerializerStore._all_serializers.pop("test_boom", None)
        SerializerStore._records.pop("test_boom", None)
        del sys.modules["_test_setup_boom_mod"]


def test_bootstrap_disabled_toggle():
    """Entries whose name is in disabled_names land in DISABLED state."""
    mod = types.ModuleType("_test_disable_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _DisableMe(ArtifactSerializer):
    TYPE = "test_disable"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        mod.__dict__,
    )
    sys.modules["_test_disable_mod"] = mod
    try:
        SerializerStore.bootstrap_entries(
            [("test_disable", "_test_disable_mod._DisableMe")],
            disabled_names={"test_disable"},
        )
        rec = SerializerStore._records["test_disable"]
        assert rec.state == SerializerState.DISABLED
        # Class should NOT be in active pool
        assert mod._DisableMe not in SerializerStore._active_serializers
    finally:
        SerializerStore._all_serializers.pop("test_disable", None)
        SerializerStore._active_serializers.discard(mod._DisableMe)
        SerializerStore._records.pop("test_disable", None)
        del sys.modules["_test_disable_mod"]


def test_retry_activates_pending_record_on_module_import():
    """When a pending record's awaited module imports, the record retries to ACTIVE."""
    ser_mod = types.ModuleType("_test_retry_ser_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Pending(ArtifactSerializer):
    TYPE = "test_retry_pending"
    @classmethod
    def setup_imports(cls, context=None):
        cls.lazy_import("retry_dep_mod_name")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        ser_mod.__dict__,
    )
    sys.modules["_test_retry_ser_mod"] = ser_mod
    sys.modules.pop("retry_dep_mod_name", None)

    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_retry_pending", "_test_retry_ser_mod._Pending"),
            ]
        )
        rec = SerializerStore._records["test_retry_pending"]
        assert rec.state == SerializerState.PENDING_ON_IMPORTS
        assert "retry_dep_mod_name" in rec.awaiting_modules
        assert (
            "test_retry_pending"
            in SerializerStore._pending_by_module["retry_dep_mod_name"]
        )

        # Simulate the dep becoming available, then fire the retry hook.
        dep_mod = types.ModuleType("retry_dep_mod_name")
        sys.modules["retry_dep_mod_name"] = dep_mod
        SerializerStore._on_module_imported("retry_dep_mod_name", dep_mod)

        assert rec.state == SerializerState.ACTIVE
        assert rec.import_trigger == "hook"
        assert ser_mod._Pending in SerializerStore._active_serializers
        # _pending_by_module should no longer list this record under that module
        assert "test_retry_pending" not in SerializerStore._pending_by_module.get(
            "retry_dep_mod_name", []
        )
    finally:
        SerializerStore._all_serializers.pop("test_retry_pending", None)
        SerializerStore._active_serializers.discard(ser_mod._Pending)
        SerializerStore._records.pop("test_retry_pending", None)
        SerializerStore._pending_by_module.pop("retry_dep_mod_name", None)
        SerializerStore._ordered_cache = None
        sys.modules.pop("_test_retry_ser_mod", None)
        sys.modules.pop("retry_dep_mod_name", None)


def test_retry_hits_loop_guard_after_repeated_failure():
    """Calling _on_module_imported when setup_imports still fails on the same
    module name should transition to BROKEN via the loop guard."""
    ser_mod = types.ModuleType("_test_loop_ser_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _Loopy(ArtifactSerializer):
    TYPE = "test_loopy"
    @classmethod
    def setup_imports(cls, context=None):
        # Always raises on the same module name even if retried.
        cls.lazy_import("never_resolves_mod")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        ser_mod.__dict__,
    )
    sys.modules["_test_loop_ser_mod"] = ser_mod
    sys.modules.pop("never_resolves_mod", None)

    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_loopy", "_test_loop_ser_mod._Loopy"),
            ]
        )
        rec = SerializerStore._records["test_loopy"]
        assert rec.state == SerializerState.PENDING_ON_IMPORTS

        # Fake the dep appearing but NOT actually installing it — the retry
        # will re-run setup_imports, which will ImportError again on the same name.
        dep_mod = types.ModuleType("never_resolves_mod")
        # We deliberately DO NOT put dep_mod in sys.modules, so lazy_import
        # inside setup_imports will raise ModuleNotFoundError again.
        SerializerStore._on_module_imported("never_resolves_mod", dep_mod)

        assert rec.state == SerializerState.BROKEN
        assert "repeated" in rec.last_error.lower()
    finally:
        SerializerStore._all_serializers.pop("test_loopy", None)
        SerializerStore._records.pop("test_loopy", None)
        SerializerStore._pending_by_module.pop("never_resolves_mod", None)
        sys.modules.pop("_test_loop_ser_mod", None)
        sys.modules.pop("never_resolves_mod", None)


def test_retry_fires_via_real_import_hook(tmp_path, monkeypatch):
    """End-to-end: park a serializer on a missing module, install the hook,
    actually import the module, verify the serializer activates."""
    pkg_dir = tmp_path / "fixture_retry_dep"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("VALUE = 42\n")
    # NOTE: we prepend syspath AFTER bootstrap_entries below so the first
    # lazy_import() fails and the record parks on the missing module.

    ser_mod = types.ModuleType("_test_e2e_ser_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _E2E(ArtifactSerializer):
    TYPE = "test_e2e_retry"
    @classmethod
    def setup_imports(cls, context=None):
        cls.lazy_import("fixture_retry_dep")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        ser_mod.__dict__,
    )
    sys.modules["_test_e2e_ser_mod"] = ser_mod
    # Make sure the dep module isn't pre-imported
    sys.modules.pop("fixture_retry_dep", None)

    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_e2e_retry", "_test_e2e_ser_mod._E2E"),
            ]
        )
        rec = SerializerStore._records["test_e2e_retry"]
        assert rec.state == SerializerState.PENDING_ON_IMPORTS

        # Now make the dep discoverable on sys.path and actually import it.
        monkeypatch.syspath_prepend(str(tmp_path))
        import fixture_retry_dep  # noqa: F401

        # After real import, the hook chain should have fired.
        assert rec.state == SerializerState.ACTIVE
        assert rec.import_trigger == "hook"
        assert ser_mod._E2E in SerializerStore._active_serializers
    finally:
        SerializerStore._all_serializers.pop("test_e2e_retry", None)
        SerializerStore._active_serializers.discard(ser_mod._E2E)
        SerializerStore._records.pop("test_e2e_retry", None)
        SerializerStore._pending_by_module.pop("fixture_retry_dep", None)
        SerializerStore._ordered_cache = None
        sys.modules.pop("_test_e2e_ser_mod", None)
        sys.modules.pop("fixture_retry_dep", None)
        # Clean up interceptor state
        from metaflow.datastore.artifacts.lazy_registry import _interceptor

        _interceptor._watched.discard("fixture_retry_dep")
        _interceptor._processed.discard("fixture_retry_dep")


def test_bootstrap_with_no_extensions_still_runs_core():
    """bootstrap() reads core ARTIFACT_SERIALIZERS_DESC from metaflow.plugins
    and activates PickleSerializer."""
    from metaflow.plugins.datastores.serializers.pickle_serializer import (
        PickleSerializer,
    )

    # Snapshot and clear state
    saved_active = set(SerializerStore._active_serializers)
    saved_records = dict(SerializerStore._records)
    SerializerStore._active_serializers.clear()
    SerializerStore._records.clear()

    try:
        SerializerStore.bootstrap()
        # PickleSerializer should be in active pool (core entry)
        pickle_active = any(
            r.type == "pickle" and r.state == SerializerState.ACTIVE
            for r in SerializerStore._records.values()
        )
        assert pickle_active
    finally:
        SerializerStore._active_serializers.clear()
        SerializerStore._active_serializers.update(saved_active)
        SerializerStore._records.clear()
        SerializerStore._records.update(saved_records)


def test_bootstrap_stamps_core_source_on_record():
    """Core serializers bootstrap with source='metaflow' on their records."""
    saved_active = set(SerializerStore._active_serializers)
    saved_records = dict(SerializerStore._records)
    SerializerStore._active_serializers.clear()
    SerializerStore._records.clear()

    try:
        SerializerStore.bootstrap()
        pickle_rec = next(
            (r for r in SerializerStore._records.values() if r.type == "pickle"),
            None,
        )
        assert pickle_rec is not None
        assert pickle_rec.source == "metaflow"
    finally:
        SerializerStore._active_serializers.clear()
        SerializerStore._active_serializers.update(saved_active)
        SerializerStore._records.clear()
        SerializerStore._records.update(saved_records)


def test_bootstrap_entries_accepts_source_override():
    """bootstrap_entries accepts ``source`` and attaches it to each record."""
    import sys as _sys
    import types as _types

    saved_records = dict(SerializerStore._records)
    saved_active = set(SerializerStore._active_serializers)

    ser_mod = _types.ModuleType("_test_source_ser_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _SourceProbe(ArtifactSerializer):
    TYPE = "test_source_probe"
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        ser_mod.__dict__,
    )
    _sys.modules["_test_source_ser_mod"] = ser_mod

    try:
        SerializerStore.bootstrap_entries(
            [("test_source_probe", "_test_source_ser_mod._SourceProbe")],
            source="fake-extension",
        )
        rec = SerializerStore._records["test_source_probe"]
        assert rec.source == "fake-extension"
        assert SerializerStore.get_source_for(ser_mod._SourceProbe) == "fake-extension"
    finally:
        SerializerStore._all_serializers.pop("test_source_probe", None)
        SerializerStore._records.clear()
        SerializerStore._records.update(saved_records)
        SerializerStore._active_serializers.clear()
        SerializerStore._active_serializers.update(saved_active)
        SerializerStore._ordered_cache = None
        _sys.modules.pop("_test_source_ser_mod", None)


def test_bootstrap_applies_disabled_toggle(monkeypatch):
    """bootstrap() respects -name toggles in ENABLED_ARTIFACT_SERIALIZER config."""
    from metaflow import metaflow_config

    saved_active = set(SerializerStore._active_serializers)
    saved_records = dict(SerializerStore._records)
    SerializerStore._active_serializers.clear()
    SerializerStore._records.clear()

    monkeypatch.setattr(
        metaflow_config,
        "ENABLED_ARTIFACT_SERIALIZER",
        ["-pickle"],
        raising=False,
    )
    try:
        SerializerStore.bootstrap()
        pickle_rec = next(
            (r for r in SerializerStore._records.values() if r.name == "pickle"),
            None,
        )
        assert pickle_rec is not None
        assert pickle_rec.state == SerializerState.DISABLED
    finally:
        SerializerStore._active_serializers.clear()
        SerializerStore._active_serializers.update(saved_active)
        SerializerStore._records.clear()
        SerializerStore._records.update(saved_records)


def test_list_serializer_status_returns_dicts():
    """list_serializer_status returns one dict per _records entry, with the
    documented shape."""
    from metaflow.datastore.artifacts import list_serializer_status
    from metaflow.datastore.artifacts.diagnostic import (
        SerializerRecord,
        SerializerState,
    )

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

    try:
        status = list_serializer_status()
        assert isinstance(status, list)
        match = next((s for s in status if s["name"] == "fake_test_serializer"), None)
        assert match is not None
        assert match["state"] == "active"
        assert match["priority"] == 42
        assert match["type"] == "fake_test_serializer"
        assert match["import_trigger"] == "eager"
        assert match["class_path"] == "inline.FakeSer"
        for key in (
            "name",
            "class_path",
            "state",
            "awaiting_modules",
            "last_error",
            "priority",
            "type",
            "import_trigger",
            "dispatch_error_count",
        ):
            assert key in match, "missing key '%s' in status dict" % key
    finally:
        SerializerStore._records.pop("fake_test_serializer", None)


def test_reset_for_tests_clears_registry_state():
    """SerializerStore._reset_for_tests clears _records, _active_serializers,
    _pending_by_module, _ordered_cache, and per-class _lazy_imported_names."""
    import sys as _sys
    import types as _types
    from metaflow.datastore.artifacts.lazy_registry import _interceptor

    # Seed state by bootstrapping an inline serializer
    ser_mod = _types.ModuleType("_test_reset_ser_mod")
    exec(
        """
from metaflow.datastore.artifacts import ArtifactSerializer, SerializationFormat

class _ResetProbe(ArtifactSerializer):
    TYPE = "test_reset_probe"
    @classmethod
    def setup_imports(cls, context=None):
        cls.lazy_import("json")
    @classmethod
    def can_serialize(cls, obj): return False
    @classmethod
    def can_deserialize(cls, metadata): return False
    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        raise NotImplementedError
    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        raise NotImplementedError
""",
        ser_mod.__dict__,
    )
    _sys.modules["_test_reset_ser_mod"] = ser_mod

    try:
        SerializerStore.bootstrap_entries(
            [
                ("test_reset_probe", "_test_reset_ser_mod._ResetProbe"),
            ]
        )
        # Also seed a pending record to exercise _pending_by_module
        SerializerStore.bootstrap_entries(
            [
                ("test_reset_pending", "_never_exists_mod._Absent"),
            ]
        )

        # Snapshot pre-reset state
        assert "test_reset_probe" in SerializerStore._records
        assert ser_mod._ResetProbe in SerializerStore._active_serializers
        assert "_never_exists_mod" in SerializerStore._pending_by_module
        # ResetProbe should have _lazy_imported_names populated
        assert "json" in ser_mod._ResetProbe.__dict__.get("_lazy_imported_names", set())

        # Pre-reset, the interceptor should be watching _never_exists_mod.
        assert "_never_exists_mod" in _interceptor._watched

        # Call reset
        SerializerStore._reset_for_tests()

        # Post-reset: all registry state empty
        assert SerializerStore._records == {}
        assert len(SerializerStore._active_serializers) == 0
        assert SerializerStore._pending_by_module == {}
        assert SerializerStore._ordered_cache is None

        # The probe class should no longer have stashed attrs
        assert "json" not in ser_mod._ResetProbe.__dict__
        assert ser_mod._ResetProbe.__dict__.get("_lazy_imported_names", set()) == set()

        # Interceptor watches should also be cleared
        assert "_never_exists_mod" not in _interceptor._watched
    finally:
        # In case reset didn't clean up (e.g., test failed mid-way)
        SerializerStore._all_serializers.pop("test_reset_probe", None)
        SerializerStore._records.pop("test_reset_probe", None)
        SerializerStore._records.pop("test_reset_pending", None)
        SerializerStore._active_serializers.discard(ser_mod._ResetProbe)
        SerializerStore._pending_by_module.clear()
        SerializerStore._ordered_cache = None
        _sys.modules.pop("_test_reset_ser_mod", None)
        for attr in ("json",):
            if attr in ser_mod._ResetProbe.__dict__:
                delattr(ser_mod._ResetProbe, attr)
        # Re-bootstrap so subsequent tests see the normal active pool
        # (e.g. PickleSerializer in _active_serializers + _records).
        SerializerStore.bootstrap()
