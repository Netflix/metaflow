"""
Lazy serializer registry driven by an import hook.

Extensions ship serializers whose implementation modules may import optional
heavy dependencies (``torch``, ``pyarrow``, ``fastavro``, ``protobuf``, ...).
Loading those serializer modules unconditionally at ``metaflow`` import time
would force every user to pay for dependencies they may not have installed.

This module defers both the serializer class import and its instantiation
until one of two things happens:

1. The target type's module is already present in :data:`sys.modules` when
   registration is called — registration then happens immediately.
2. The target type's module is imported later by the user's code. An
   ``importlib`` hook watches for that event and performs registration the
   first time the module is loaded.

The hook is installed on :data:`sys.meta_path` and removes itself from the
path during its own ``find_spec`` call to avoid recursion.
"""

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys

from dataclasses import dataclass, field


@dataclass
class SerializerConfig:
    """
    Declarative entry recording *which* serializer handles *which* type,
    without actually importing the serializer class. The class is imported on
    first use by :func:`load_serializer_class`.

    Parameters
    ----------
    canonical_type : str
        ``"module.ClassName"`` — e.g. ``"builtins.dict"``, ``"torch.Tensor"``.
    serializer : str
        Dotted import path to the serializer class, e.g.
        ``"my_extension.serializers.TorchSerializer"``.
    priority : int
        Dispatch priority. Lower is tried first. Matches the existing
        ``ArtifactSerializer.PRIORITY`` convention.
    extra_kwargs : dict
        Optional kwargs passed to the serializer class ``__init__``.
    """

    canonical_type: str
    serializer: str
    priority: int = 100
    extra_kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.canonical_type:
            raise ValueError("canonical_type cannot be empty")
        if not self.serializer or "." not in self.serializer:
            raise ValueError("serializer must be in 'module.ClassName' format")

    @property
    def serializer_module(self):
        return ".".join(self.serializer.split(".")[:-1])

    @property
    def serializer_class(self):
        return self.serializer.split(".")[-1]


# Module-level registry. Keyed by canonical_type -> SerializerConfig.
# A separate dict caches instantiated classes so repeated lookups are O(1).
_registered_configs = {}
_loaded_serializers = {}


def register_serializer_config(config):
    """Store a config immediately. The serializer class is not imported yet."""
    _registered_configs[config.canonical_type] = config
    # Any previously cached class for this type is now stale.
    _loaded_serializers.pop(config.canonical_type, None)


def load_serializer_class(canonical_type):
    """
    Resolve and cache the serializer class for ``canonical_type``. Returns
    ``None`` if no config is registered for that type.
    """
    cached = _loaded_serializers.get(canonical_type)
    if cached is not None:
        return cached
    config = _registered_configs.get(canonical_type)
    if config is None:
        return None
    module = importlib.import_module(config.serializer_module)
    cls = getattr(module, config.serializer_class)
    _loaded_serializers[canonical_type] = cls
    return cls


def iter_registered_configs():
    """Iterate all registered configs. Deterministic order (insertion)."""
    return list(_registered_configs.values())


class _WrappedLoader(importlib.abc.Loader):
    """Delegating loader that fires a callback after ``exec_module``.

    Only ``create_module`` and ``exec_module`` are overridden. Other loader
    attributes (``get_resource_reader``, ``get_filename``, ``get_data``,
    ``is_package``, ``get_source``, ...) are forwarded to the wrapped loader
    via ``__getattr__`` so importers that poke at those interfaces continue
    to work transparently.
    """

    def __init__(self, original_loader, interceptor):
        self._original = original_loader
        self._interceptor = interceptor

    def create_module(self, spec):
        return self._original.create_module(spec)

    def exec_module(self, module):
        self._original.exec_module(module)
        self._interceptor._on_module_imported(module)

    def __getattr__(self, name):
        return getattr(self._original, name)


class _SerializerImportInterceptor(importlib.abc.MetaPathFinder):
    """
    :class:`importlib.abc.MetaPathFinder` that watches for a fixed set of
    module names and fires :func:`_on_module_imported` once each has been
    fully executed.
    """

    def __init__(self):
        # module_name -> list[SerializerConfig]
        self._pending = {}
        self._processed = set()

    def watch(self, module_name, config):
        self._pending.setdefault(module_name, []).append(config)

    def find_spec(self, fullname, path, target=None):
        if fullname not in self._pending:
            return None
        # Remove ourselves from the path during the lookup below so Python's
        # normal finders (not us) can resolve the real spec. Reinstall after.
        was_installed = self in sys.meta_path
        if was_installed:
            sys.meta_path.remove(self)
        try:
            spec = importlib.util.find_spec(fullname)
        finally:
            if was_installed:
                sys.meta_path.insert(0, self)
        if spec is None or spec.loader is None:
            return None
        spec.loader = _WrappedLoader(spec.loader, self)
        return spec

    def _on_module_imported(self, module):
        module_name = module.__name__
        if module_name in self._processed:
            return
        self._processed.add(module_name)
        for config in self._pending.get(module_name, ()):
            class_name = config.canonical_type.rsplit(".", 1)[-1]
            if hasattr(module, class_name):
                register_serializer_config(config)


_interceptor = _SerializerImportInterceptor()


def _ensure_interceptor_installed():
    if _interceptor in sys.meta_path:
        sys.meta_path.remove(_interceptor)
    sys.meta_path.insert(0, _interceptor)


def register_serializer_for_type(canonical_type, serializer, **kwargs):
    """
    Public entry point for extensions.

    If the target type's module is already loaded, the config is stored
    immediately. Otherwise, an import hook is installed and registration is
    deferred to the first ``import`` of the target module.

    ``canonical_type`` is ``"module.ClassName"``. ``serializer`` is a dotted
    path to the serializer class. Additional ``priority`` / ``extra_kwargs``
    forwarded into :class:`SerializerConfig`.
    """
    config = SerializerConfig(
        canonical_type=canonical_type, serializer=serializer, **kwargs
    )
    module_name, class_name = canonical_type.rsplit(".", 1)
    mod = sys.modules.get(module_name)
    if mod is not None and hasattr(mod, class_name):
        register_serializer_config(config)
        return
    _ensure_interceptor_installed()
    _interceptor.watch(module_name, config)


def _reset_for_tests():
    """Clear all module-level state. Intended for unit tests only."""
    _registered_configs.clear()
    _loaded_serializers.clear()
    _interceptor._pending.clear()
    _interceptor._processed.clear()
    if _interceptor in sys.meta_path:
        sys.meta_path.remove(_interceptor)
