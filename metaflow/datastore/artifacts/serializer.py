import inspect
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from enum import Enum
from typing import Any, List, Optional, Tuple, Union


class SerializationFormat(str, Enum):
    """
    Serialization format for :class:`ArtifactSerializer`.

    ``STORAGE`` produces ``(blobs, metadata)`` for the datastore persist path;
    ``WIRE`` produces a ``str`` for CLI args, protobuf payloads, and
    cross-process IPC.

    This subclasses ``str`` so that existing equality checks and JSON / artifact
    metadata round-trips continue to work with the underlying ``"storage"`` /
    ``"wire"`` values.
    """

    STORAGE = "storage"
    WIRE = "wire"


def _call_setup_imports(cls, context=None):
    """Invoke ``cls.setup_imports`` respecting both signatures:
    ``def setup_imports(cls)`` and ``def setup_imports(cls, context=None)``.
    """
    func = cls.setup_imports.__func__  # unwrap classmethod
    # co_argcount counts positional args: (cls,) -> 1, (cls, context) -> 2.
    if func.__code__.co_argcount >= 2:
        return cls.setup_imports(context)
    return cls.setup_imports()


SerializationMetadata = namedtuple(
    "SerializationMetadata", ["obj_type", "size", "encoding", "serializer_info"]
)


class _OrderedSet:
    """
    Minimal insertion-ordered set used for ``_active_serializers``.

    Supports the subset of ``set`` API the codebase exercises:
    ``add``, ``discard``, ``update``, ``clear``, ``in``, iteration, ``len``.
    Iteration yields insertion order (dict keys preserve it since 3.7).
    """

    __slots__ = ("_d",)

    def __init__(self, iterable=None):
        self._d = {}
        if iterable is not None:
            self.update(iterable)

    def add(self, x):
        # Re-adding is a no-op so "last registered" reflects first-time
        # insertion order rather than most recent re-assertion.
        if x not in self._d:
            self._d[x] = None

    def discard(self, x):
        self._d.pop(x, None)

    def update(self, iterable):
        for x in iterable:
            self.add(x)

    def clear(self):
        self._d.clear()

    def __contains__(self, x):
        return x in self._d

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __repr__(self):
        return "_OrderedSet(%r)" % list(self._d)


class SerializedBlob(object):
    """
    Represents a single blob produced by a serializer.

    A serializer may produce multiple blobs per artifact. Each blob is either:
    - New bytes to be stored (is_reference=False, value is bytes)
    - A reference to already-stored data (is_reference=True, value is a string key)

    Parameters
    ----------
    value : Union[str, bytes]
        The blob data (bytes) or a reference key (str).
    is_reference : bool, optional
        If None, auto-detected from value type: str -> reference, bytes -> new data.
    """

    def __init__(self, value, is_reference=None):
        if not isinstance(value, (str, bytes)):
            raise TypeError(
                "SerializedBlob value must be str or bytes, got %s"
                % type(value).__name__
            )
        self.value = value
        if is_reference is None:
            self.is_reference = isinstance(value, str)
        else:
            self.is_reference = is_reference

    @property
    def needs_save(self):
        """True if this blob contains new bytes that need to be stored."""
        return not self.is_reference


class SerializerStore(ABCMeta):
    """
    Metaclass for ArtifactSerializer that auto-registers subclasses by TYPE.

    Provides deterministic ordering: serializers are sorted by (PRIORITY, registration_order).
    Lower PRIORITY values are tried first. Registration order breaks ties.
    """

    _all_serializers = {}
    # Dispatch pool — only classes whose state == ACTIVE are here. Uses an
    # insertion-ordered set so that ``get_ordered_serializers`` can honor
    # "last-registered wins" on PRIORITY ties regardless of how a class got
    # into the pool (bootstrap, retry hook, or a direct ``.add`` by a test).
    _active_serializers = _OrderedSet()
    # Diagnostic records, keyed by tuple name from ARTIFACT_SERIALIZERS_DESC.
    _records = {}
    # Map: awaited-module-name -> list of record names waiting on it.
    _pending_by_module = {}
    _ordered_cache = None

    def __init__(cls, name, bases, namespace):
        super().__init__(name, bases, namespace)
        # Skip the abstract base and any subclass that didn't implement all
        # abstract methods — registering a partially-abstract class would
        # blow up only at dispatch time.
        if cls.TYPE is None or inspect.isabstract(cls):
            return
        SerializerStore._all_serializers[cls.TYPE] = cls
        SerializerStore._ordered_cache = None

    @staticmethod
    def get_ordered_serializers():
        """
        Return classes in ``_active_serializers`` sorted for dispatch.

        Sort order:
        - PRIORITY ascending (lower tried first)
        - On PRIORITY tie: last-registered in ``_active_serializers`` order
          wins (i.e., later insertion dispatched first)
        - Deterministic lexicographic tertiary key on ``class_path`` for
          cross-environment reproducibility
        """
        if SerializerStore._ordered_cache is not None:
            return SerializerStore._ordered_cache

        # Use a list from the set in deterministic registration order where
        # possible. Python sets don't preserve order; we recover a stable
        # order by consulting ``_records`` (dict iteration preserves insertion
        # order) and picking classes whose type matches an active record.
        record_order = [
            rec
            for rec in SerializerStore._records.values()
            if rec.state.value == "active"
        ]
        # Find each active record's class via its type; tolerant of records
        # whose class isn't in _all_serializers (shouldn't happen in practice).
        active_in_order = []
        for rec in record_order:
            cls = SerializerStore._all_serializers.get(rec.type)
            if cls is not None and cls in SerializerStore._active_serializers:
                active_in_order.append(cls)
        # Include any _active_serializers entries that don't have a record
        # (e.g. inline-registered test classes) at the end of the base list.
        seen = set(active_in_order)
        for cls in SerializerStore._active_serializers:
            if cls not in seen:
                active_in_order.append(cls)
                seen.add(cls)

        # Sort: PRIORITY ascending; on ties, LAST-registered wins
        # (secondary key = -index); tertiary key = class path.
        def _sort_key(iv):
            idx, cls = iv
            class_path = "%s.%s" % (cls.__module__, cls.__qualname__)
            return (cls.PRIORITY, -idx, class_path)

        indexed = list(enumerate(active_in_order))
        SerializerStore._ordered_cache = [
            cls for _, cls in sorted(indexed, key=_sort_key)
        ]
        return SerializerStore._ordered_cache

    @classmethod
    def bootstrap(cls):
        """Walk every extension's ARTIFACT_SERIALIZERS_DESC, apply toggles
        from config, and drive each entry through the state machine. Called
        once at Metaflow startup from ``metaflow/plugins/__init__.py``.
        """
        entries = []

        # Bucketed by source so each bucket can be passed to
        # bootstrap_entries with its own source label.
        by_source = []  # list of (source, entries)

        # Core serializers (shipped with Metaflow). Class paths in the core
        # ARTIFACT_SERIALIZERS_DESC are relative to ``metaflow.plugins`` and
        # must be resolved here now that ``artifact_serializer`` is no longer
        # a registered plugin category (which previously handled this via
        # ``_resolve_relative_paths``).
        try:
            from metaflow.plugins import ARTIFACT_SERIALIZERS_DESC as core_desc

            core_entries = [
                (name, cls._resolve_relative_class_path(path, "metaflow.plugins"))
                for name, path in core_desc
            ]
            if core_entries:
                by_source.append(("metaflow", core_entries))
        except ImportError:
            # Possible during partial test imports; skip gracefully.
            pass

        # Extension serializers — stamp each with the extension's package name
        # so load errors can point at the right package to install.
        try:
            from metaflow.extension_support import plugins as ext_plugins

            for ext_entry in ext_plugins.get_modules("plugins"):
                mod = getattr(ext_entry, "module", None)
                if mod is None:
                    continue
                ext_pkg = getattr(mod, "__package__", None) or ""
                ext_desc = mod.__dict__.get("ARTIFACT_SERIALIZERS_DESC", [])
                ext_entries = [
                    (name, cls._resolve_relative_class_path(path, ext_pkg))
                    for name, path in ext_desc
                ]
                if not ext_entries:
                    continue
                source = (
                    getattr(ext_entry, "package_name", None)
                    or getattr(ext_entry, "tl_package", None)
                    or ext_pkg
                    or None
                )
                by_source.append((source, ext_entries))
        except Exception:
            # Do not let extension discovery failures kill Metaflow startup.
            pass

        # Resolve +/- toggles from config.
        disabled_names = set()
        try:
            from metaflow import metaflow_config

            enabled_list = (
                getattr(metaflow_config, "ENABLED_ARTIFACT_SERIALIZER", None) or []
            )
            for n in enabled_list:
                if isinstance(n, str) and n.startswith("-"):
                    disabled_names.add(n[1:])
        except Exception:
            pass

        for source, group in by_source:
            cls.bootstrap_entries(group, disabled_names=disabled_names, source=source)

    @staticmethod
    def _resolve_relative_class_path(class_path, pkg_path):
        """Convert a leading-dot class_path into an absolute dotted path.

        Mirrors ``metaflow.extension_support.plugins._resolve_relative_paths``'s
        ``resolve_path`` inner helper: the number of leading dots indicates
        how many levels to walk up from ``pkg_path``. Non-relative paths are
        returned unchanged. ``pkg_path`` should be the ``__package__`` of the
        module that declared the descriptor (e.g. ``metaflow.plugins``).
        """
        if not class_path or class_path[0] != ".":
            return class_path
        pkg_components = pkg_path.split(".") if pkg_path else []
        i = 1
        while i < len(class_path) and class_path[i] == ".":
            i += 1
        if i > len(pkg_components):
            raise ValueError(
                "Path '%s' exits out of Metaflow module at %s" % (class_path, pkg_path)
            )
        prefix = ".".join(pkg_components[: -i + 1] if i > 1 else pkg_components)
        return prefix + class_path[i - 1 :]

    @classmethod
    def bootstrap_entries(cls, entries, disabled_names=None, source=None):
        """Drive a list of (name, class_path) tuples through the state machine.

        Called once at Metaflow startup via ``bootstrap()`` and directly by
        tests for isolation.

        ``disabled_names``: set of tuple names to mark DISABLED without
        attempting import (from ``-name`` toggles in config).
        ``source``: human-readable identifier for where these entries came
        from (e.g. ``"metaflow"`` for core, an extension's ``package_name``
        for extension-shipped serializers). Stamped on each record and
        auto-injected into ``serializer_info["source"]`` at save time unless
        the serializer sets its own value.
        """
        import importlib
        from .diagnostic import SerializerRecord, SerializerState

        disabled_names = disabled_names or set()

        for name, class_path in entries:
            rec = SerializerRecord(name=name, class_path=class_path, source=source)
            cls._records[name] = rec

            if name in disabled_names:
                rec.state = SerializerState.DISABLED
                continue

            module_path, class_name = class_path.rsplit(".", 1)

            rec.state = SerializerState.IMPORTING
            try:
                module = importlib.import_module(module_path)
            except ImportError as e:
                cls._park_on_import_error(rec, e)
                continue

            serializer_cls = getattr(module, class_name, None)
            if serializer_cls is None:
                rec.state = SerializerState.BROKEN
                rec.last_error = "class '%s' not found in module '%s'" % (
                    class_name,
                    module_path,
                )
                continue

            if serializer_cls.TYPE != name:
                rec.state = SerializerState.BROKEN
                rec.last_error = "tuple name '%s' != class.TYPE '%s'" % (
                    name,
                    serializer_cls.TYPE,
                )
                continue

            rec.state = SerializerState.CLASS_LOADED
            rec.priority = serializer_cls.PRIORITY
            rec.type = serializer_cls.TYPE

            rec.state = SerializerState.IMPORTING_DEPS
            try:
                _call_setup_imports(serializer_cls, context=None)
            except ImportError as e:
                cls._park_on_import_error(rec, e)
                continue
            except Exception as e:
                rec.state = SerializerState.BROKEN
                rec.last_error = "%s: %s" % (type(e).__name__, e)
                continue

            rec.state = SerializerState.ACTIVE
            rec.import_trigger = "eager"
            cls._active_serializers.add(serializer_cls)
            cls._ordered_cache = None

    @classmethod
    def _park_on_import_error(cls, rec, exc):
        """Transition rec to PENDING_ON_IMPORTS and register the watched module.

        Used for ImportError / ModuleNotFoundError during module-import OR
        setup_imports. Loop guard: same e.name twice in a row -> BROKEN.

        Installs a ``sys.meta_path`` retry hook for the missing module so
        that the record is re-driven through the state machine when (and
        if) the module is eventually imported.
        """
        from .diagnostic import SerializerState

        if exc.name is None:
            rec.state = SerializerState.BROKEN
            rec.last_error = "ImportError with no module name: %s" % exc
            return

        # Loop guard: if we've parked on this exact module name before
        # (either currently awaiting or historically), the dep is not
        # recoverable — mark broken to prevent infinite retries.
        prev_seen = getattr(rec, "_previously_awaited", set())
        if exc.name in rec.awaiting_modules or exc.name in prev_seen:
            rec.state = SerializerState.BROKEN
            rec.last_error = "repeated ImportError on '%s': %s" % (exc.name, exc)
            return

        rec.awaiting_modules.append(exc.name)
        rec.state = SerializerState.PENDING_ON_IMPORTS
        rec.last_error = "%s: %s" % (type(exc).__name__, exc)
        cls._pending_by_module.setdefault(exc.name, []).append(rec.name)

        # Install the sys.meta_path hook so an eventual import of exc.name
        # drives _on_module_imported -> _retry_bootstrap.
        from .lazy_registry import _ensure_interceptor_installed, _interceptor

        _ensure_interceptor_installed()
        _interceptor.watch(exc.name)

    @classmethod
    def _on_module_imported(cls, module_name, module):
        """Called when a watched module finishes importing. Retries bootstrap
        for every record awaiting that module. Safe to call directly for
        tests; normally fired by the lazy_registry import interceptor.
        """
        waiting = cls._pending_by_module.pop(module_name, [])
        for record_name in waiting:
            rec = cls._records.get(record_name)
            if rec is None:
                continue
            # Clear this module from the rec's awaiting list before retry.
            # Loop guard reads awaiting_modules, so we must remove the current
            # module or a legitimate retry would short-circuit to BROKEN. The
            # history tracker ``_previously_awaited`` preserves the fact that
            # we did park on this module before, so a genuine repeat failure
            # still trips the guard.
            if not hasattr(rec, "_previously_awaited"):
                rec._previously_awaited = set()
            rec._previously_awaited.update(rec.awaiting_modules)
            rec.awaiting_modules = [m for m in rec.awaiting_modules if m != module_name]
            cls._retry_bootstrap(rec)

    @classmethod
    def _retry_bootstrap(cls, rec):
        """Re-run the import + setup_imports sequence for a single record."""
        import importlib
        from .diagnostic import SerializerState

        module_path, class_name = rec.class_path.rsplit(".", 1)

        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            cls._park_on_import_error(rec, e)
            return

        serializer_cls = getattr(module, class_name, None)
        if serializer_cls is None:
            rec.state = SerializerState.BROKEN
            rec.last_error = "class '%s' not found in '%s' on retry" % (
                class_name,
                module_path,
            )
            return

        try:
            _call_setup_imports(serializer_cls, context=None)
        except ImportError as e:
            cls._park_on_import_error(rec, e)
            return
        except Exception as e:
            rec.state = SerializerState.BROKEN
            rec.last_error = "%s: %s" % (type(e).__name__, e)
            return

        rec.state = SerializerState.ACTIVE
        rec.import_trigger = "hook"
        cls._active_serializers.add(serializer_cls)
        cls._ordered_cache = None

    @classmethod
    def get_source_for(cls, serializer_cls):
        """Return the ``source`` label attached to a serializer's record, or
        ``None`` if no record exists. Looked up by ``TYPE`` — tuple names are
        validated equal to ``TYPE`` at bootstrap time."""
        target_type = getattr(serializer_cls, "TYPE", None)
        if target_type is None:
            return None
        for rec in cls._records.values():
            if rec.type == target_type:
                return rec.source
        return None

    @classmethod
    def _reset_for_tests(cls):
        """Clear all registry state. Intended for unit tests only.

        Clears:
        - ``_records`` (per-entry diagnostic records)
        - ``_active_serializers`` (dispatch pool)
        - ``_pending_by_module`` (retry watch map)
        - ``_ordered_cache`` (dispatch sort cache)
        - per-class ``_lazy_imported_names`` (walks MRO of every class in
          ``_all_serializers`` and ``_active_serializers`` and delattr's
          tracked lazy-imported attribute names)
        - Interceptor's watched-module set so a future bootstrap does not
          re-fire stale hooks.

        Does NOT clear ``_all_serializers`` (metaclass-populated). Tests that
        need that swept should do it themselves.
        """
        # Collect candidate classes from both registries (metaclass-tracked
        # + active dispatch pool).
        candidate_classes = set(cls._all_serializers.values())
        candidate_classes.update(cls._active_serializers)

        # For each class (and its MRO), clear lazy-imported attributes.
        for serializer_cls in candidate_classes:
            for base in serializer_cls.__mro__:
                names = base.__dict__.get("_lazy_imported_names")
                if not names:
                    continue
                for name in list(names):
                    if name in base.__dict__:
                        try:
                            delattr(base, name)
                        except (AttributeError, TypeError):
                            pass
                names.clear()

        # Reset registry state.
        cls._records.clear()
        cls._active_serializers.clear()
        cls._pending_by_module.clear()
        cls._ordered_cache = None

        # Clear interceptor watches so a future bootstrap does not fire on
        # stale module names.
        try:
            from .lazy_registry import _interceptor

            if hasattr(_interceptor, "_watched"):
                _interceptor._watched.clear()
        except ImportError:
            pass


class ArtifactSerializer(object, metaclass=SerializerStore):
    """
    Abstract base class for artifact serializers.

    Subclasses must set TYPE to a unique string identifier and implement
    all four class methods. Subclasses are auto-registered by the SerializerStore
    metaclass on class definition.

    Attributes
    ----------
    TYPE : str or None
        Unique identifier for this serializer (e.g., "pickle", "iotype").
        Set to None in the base class to prevent registration.
    PRIORITY : int
        Dispatch priority. Lower values are tried first. Default 100.
        PickleSerializer uses 9999 as the universal fallback.
    """

    TYPE = None
    PRIORITY = 100

    @classmethod
    def setup_imports(cls, context=None):
        """Perform heavy imports. Called once by SerializerStore after the
        class module loads cleanly and before the class is added to the
        dispatch pool. If this method raises ``ImportError`` with a named
        missing module, Metaflow parks the serializer until that module
        appears in ``sys.modules`` and retries. Any other exception moves
        the serializer to the ``broken`` state.

        The ``context`` parameter is reserved for future use; it is always
        ``None`` today. Authors may omit it entirely
        (``def setup_imports(cls):``) or accept it with a default
        (``def setup_imports(cls, context=None):``).

        Default: no-op.
        """
        return None

    _RESERVED_LAZY_IMPORT_NAMES = frozenset(
        {
            "TYPE",
            "PRIORITY",
            "setup_imports",
            "can_serialize",
            "can_deserialize",
            "serialize",
            "deserialize",
            "lazy_import",
        }
    )

    @classmethod
    def lazy_import(cls, module_path, alias=None):
        """Import ``module_path``, stash on ``cls``, return the module.

        Alias defaults to the leaf of ``module_path``
        (``lazy_import("torch.nn.functional")`` -> ``cls.functional``).
        Names starting with ``_`` and methods of ``ArtifactSerializer`` are
        reserved and cannot be used as aliases.

        Propagates ``ImportError`` / ``ModuleNotFoundError`` from the
        import itself -- the state machine interprets these to install a
        retry hook.
        """
        import importlib

        name = alias if alias is not None else module_path.rsplit(".", 1)[-1]
        if name in cls._RESERVED_LAZY_IMPORT_NAMES or name.startswith("_"):
            raise ValueError("lazy_import alias '%s' is reserved or invalid" % name)
        # Track per-class so _reset_for_tests can clean up later.
        if "_lazy_imported_names" not in cls.__dict__:
            cls._lazy_imported_names = set()
        if name in cls._lazy_imported_names:
            # Idempotent re-import: the alias was set in a prior setup_imports
            # call (e.g. first attempt partially succeeded before parking on a
            # missing dep). Return the already-stashed module.
            return getattr(cls, name)
        mod = importlib.import_module(module_path)
        setattr(cls, name, mod)
        cls._lazy_imported_names.add(name)
        return mod

    @classmethod
    @abstractmethod
    def can_serialize(cls, obj: Any) -> bool:
        """
        Return True if this serializer can handle the given object.

        Parameters
        ----------
        obj : Any
            The Python object to serialize.

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def can_deserialize(cls, metadata: SerializationMetadata) -> bool:
        """
        Return True if this serializer can deserialize given the metadata.

        Parameters
        ----------
        metadata : SerializationMetadata
            Metadata stored alongside the artifact.

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def serialize(
        cls,
        obj: Any,
        format: SerializationFormat = SerializationFormat.STORAGE,
    ) -> Union[Tuple[List["SerializedBlob"], SerializationMetadata], str]:
        """
        Serialize obj. Must be side-effect-free: this method may be invoked
        multiple times (caching, retries, parallel dispatch) and must not
        perform I/O, mutate global state, or register the object elsewhere.
        Side effects that need to happen at persist time belong in hooks,
        not in the serializer.

        Parameters
        ----------
        obj : Any
            The Python object to serialize.
        format : SerializationFormat
            Either ``SerializationFormat.STORAGE`` (default) or
            ``SerializationFormat.WIRE``.
            - ``STORAGE`` returns a tuple ``(List[SerializedBlob], SerializationMetadata)``
              for persisting through the datastore.
            - ``WIRE`` returns a ``str`` representation for CLI args, protobuf
              payloads, and cross-process IPC. Serializers that cannot provide
              a wire encoding should raise ``NotImplementedError``.

        Returns
        -------
        tuple or str
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(
        cls,
        data: Union[List[bytes], str],
        metadata: Optional[SerializationMetadata] = None,
        format: SerializationFormat = SerializationFormat.STORAGE,
    ) -> Any:
        """
        Deserialize back to a Python object.

        Parameters
        ----------
        data : Union[List[bytes], str]
            ``List[bytes]`` when ``format=STORAGE``; ``str`` when ``format=WIRE``.
        metadata : SerializationMetadata, optional
            Metadata stored alongside the artifact. Required for STORAGE,
            ignored for WIRE.
        format : SerializationFormat
            Either ``SerializationFormat.STORAGE`` (default) or
            ``SerializationFormat.WIRE``.

        Returns
        -------
        Any
        """
        raise NotImplementedError
