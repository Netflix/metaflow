"""Tests for the lazy serializer registry and its import interceptor."""

import sys
import textwrap
import types

import pytest

from metaflow.datastore.artifacts import lazy_registry
from metaflow.datastore.artifacts.lazy_registry import (
    SerializerConfig,
    _interceptor,
    _reset_for_tests,
    iter_registered_configs,
    load_serializer_class,
    register_serializer_config,
    register_serializer_for_type,
)


@pytest.fixture(autouse=True)
def reset():
    _reset_for_tests()
    yield
    _reset_for_tests()


def test_config_requires_canonical_type_and_dotted_serializer():
    with pytest.raises(ValueError):
        SerializerConfig(canonical_type="", serializer="pkg.Cls")
    with pytest.raises(ValueError):
        SerializerConfig(canonical_type="builtins.dict", serializer="nodot")


def test_config_splits_serializer_path():
    cfg = SerializerConfig(
        canonical_type="builtins.dict", serializer="pkg.mod.Cls"
    )
    assert cfg.serializer_module == "pkg.mod"
    assert cfg.serializer_class == "Cls"


def test_register_config_is_immediate():
    cfg = SerializerConfig(
        canonical_type="builtins.dict",
        serializer="test_lazy_serializer_registry.DictSerializer",
    )
    register_serializer_config(cfg)
    assert cfg in iter_registered_configs()


def test_already_imported_type_registers_eagerly():
    """If the type's module is already in sys.modules, no hook install."""
    register_serializer_for_type(
        canonical_type="builtins.dict",
        serializer="test_lazy_serializer_registry.DictSerializer",
    )
    assert any(
        c.canonical_type == "builtins.dict" for c in iter_registered_configs()
    )
    # Hook should not be installed since dict was already imported.
    assert _interceptor not in sys.meta_path


def test_deferred_registration_fires_on_import(tmp_path, monkeypatch):
    """A not-yet-imported module triggers registration on first import."""
    pkg_dir = tmp_path / "_lazy_probe"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text(
        textwrap.dedent(
            """
            class ProbeClass:
                pass
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    register_serializer_for_type(
        canonical_type="_lazy_probe.ProbeClass",
        serializer="test_lazy_serializer_registry.DictSerializer",
    )
    # Hook must be installed since the probe module isn't loaded yet.
    assert _interceptor in sys.meta_path
    # No config registered yet.
    assert not any(
        c.canonical_type == "_lazy_probe.ProbeClass"
        for c in iter_registered_configs()
    )

    import _lazy_probe  # noqa: F401

    assert any(
        c.canonical_type == "_lazy_probe.ProbeClass"
        for c in iter_registered_configs()
    )


def test_load_serializer_class_resolves_dotted_path(monkeypatch):
    # Inject a fake serializer class into a throwaway module.
    fake = types.ModuleType("_fake_serializer_mod")

    class FakeSerializer:
        pass

    fake.FakeSerializer = FakeSerializer
    monkeypatch.setitem(sys.modules, "_fake_serializer_mod", fake)

    register_serializer_config(
        SerializerConfig(
            canonical_type="builtins.int",
            serializer="_fake_serializer_mod.FakeSerializer",
        )
    )
    cls = load_serializer_class("builtins.int")
    assert cls is FakeSerializer
    # Cached on second call.
    assert load_serializer_class("builtins.int") is FakeSerializer


def test_load_serializer_class_returns_none_for_unregistered():
    assert load_serializer_class("builtins.nonexistent") is None


def test_interceptor_find_spec_returns_none_for_unwatched():
    # Unwatched module — find_spec should decline to intercept.
    assert _interceptor.find_spec("json", None) is None


def test_wrapped_loader_forwards_unknown_attrs():
    """Loaders expose additional attributes (get_filename, is_package, ...).
    The wrapper must forward those so importers that poke at them keep
    working.
    """
    from metaflow.datastore.artifacts.lazy_registry import _WrappedLoader

    class _FakeLoader:
        def create_module(self, spec):
            return None

        def exec_module(self, module):
            return None

        def get_filename(self, fullname):
            return "/tmp/" + fullname

        custom_attr = "hello"

    wrapped = _WrappedLoader(_FakeLoader(), _interceptor)
    assert wrapped.get_filename("pkg.mod") == "/tmp/pkg.mod"
    assert wrapped.custom_attr == "hello"


def test_interceptor_recursion_guard(tmp_path, monkeypatch):
    """find_spec must temporarily remove itself from meta_path."""
    pkg_dir = tmp_path / "_lazy_recur"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("X = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    register_serializer_for_type(
        canonical_type="_lazy_recur.X",
        serializer="test_lazy_serializer_registry.DictSerializer",
    )
    # Just import — if the recursion guard is broken, this stack-overflows.
    import _lazy_recur  # noqa: F401


def test_lazy_registered_serializer_reaches_dispatch(monkeypatch):
    """A lazy-registered ArtifactSerializer must surface through
    SerializerStore.get_ordered_serializers() — otherwise the extension
    registration API is silently inert.
    """
    import types as _types

    from metaflow.datastore.artifacts.serializer import (
        ArtifactSerializer,
        SerializationMetadata,
        SerializedBlob,
        SerializerStore,
    )

    snapshot = dict(SerializerStore._all_serializers)

    class _LazyProbeSerializer(ArtifactSerializer):
        TYPE = "test_lazy_probe"
        PRIORITY = 33

        @classmethod
        def can_serialize(cls, obj):
            return False

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format="storage"):
            blob = b""
            return [SerializedBlob(blob)], SerializationMetadata("x", 0, "x", {})

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            return None

    # Remove it so lazy-registry has to pull it in.
    SerializerStore._all_serializers.pop("test_lazy_probe", None)
    SerializerStore._ordered_cache = None

    fake = _types.ModuleType("_lazy_probe_mod")
    fake._LazyProbeSerializer = _LazyProbeSerializer
    monkeypatch.setitem(sys.modules, "_lazy_probe_mod", fake)

    try:
        register_serializer_config(
            SerializerConfig(
                canonical_type="builtins.object",
                serializer="_lazy_probe_mod._LazyProbeSerializer",
            )
        )
        ordered = SerializerStore.get_ordered_serializers()
        assert _LazyProbeSerializer in ordered
    finally:
        SerializerStore._all_serializers.clear()
        SerializerStore._all_serializers.update(snapshot)
        SerializerStore._ordered_cache = None
