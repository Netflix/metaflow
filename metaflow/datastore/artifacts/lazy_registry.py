"""
Import-hook plumbing that the serializer registry uses to retry a serializer's
``setup_imports`` after one of its required modules becomes importable.

Extensions ship serializers whose implementation modules may import optional
heavy dependencies (``torch``, ``pyarrow``, ``fastavro``, ``protobuf``, ...).
Loading those modules unconditionally at ``metaflow`` import time would force
every user to pay for dependencies they may not have installed. When
``SerializerStore.bootstrap_entries`` encounters such a missing module, it
parks the entry in ``pending_on_imports`` state and installs a watch here.
The first time the awaited module is imported by the user's code, this
interceptor fires ``SerializerStore._on_module_imported`` so the registry can
retry activation.

The interceptor is installed on :data:`sys.meta_path` and removes itself from
the path during its own ``find_spec`` call to avoid recursion.

This module has no public API — extensions declare serializers through
``ARTIFACT_SERIALIZERS_DESC`` in their ``mfextinit_*`` file and interact with
the registry via the state-machine public surface in
:mod:`metaflow.datastore.artifacts.serializer`.
"""

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys


class _WrappedLoader(importlib.abc.Loader):
    """Delegating loader that fires a callback after ``exec_module``.

    Only ``create_module`` and ``exec_module`` are overridden. Other loader
    attributes (``get_resource_reader``, ``get_filename``, ``get_data``,
    ``is_package``, ``get_source``, ...) are forwarded to the wrapped loader
    via ``__getattr__`` so importers that poke at those interfaces continue
    to work transparently.

    After ``exec_module`` (whether it succeeds or raises) the wrapper restores
    ``module.__spec__.loader`` to the original loader so downstream tooling
    (``pkgutil``, ``importlib.resources``, test runners) sees an unwrapped
    spec for the rest of the process lifetime.
    """

    def __init__(self, original_loader, interceptor):
        self._original = original_loader
        self._interceptor = interceptor

    def create_module(self, spec):
        return self._original.create_module(spec)

    def exec_module(self, module):
        try:
            self._original.exec_module(module)
        except BaseException:
            # Restore the spec.loader before propagating so a failed import
            # does not leave a wrapper attached to a half-initialised module.
            # Do NOT fire the success callback and do NOT mark the module as
            # processed — Python removes the module from ``sys.modules`` on
            # exec failure, so a future ``import`` will trip ``find_spec``
            # again and we want the wrapper to drive a fresh attempt.
            self._restore_spec_loader(module)
            raise
        self._restore_spec_loader(module)
        self._interceptor._on_module_imported(module)

    def _restore_spec_loader(self, module):
        spec = getattr(module, "__spec__", None)
        if spec is not None and getattr(spec, "loader", None) is self:
            spec.loader = self._original

    def __getattr__(self, name):
        return getattr(self._original, name)


class _SerializerImportInterceptor(importlib.abc.MetaPathFinder):
    """
    :class:`importlib.abc.MetaPathFinder` that watches a set of module names
    and notifies :class:`SerializerStore` once each has finished executing.
    """

    def __init__(self):
        # Module names to watch on behalf of SerializerStore records parked
        # via _park_on_import_error. Firing calls
        # SerializerStore._on_module_imported.
        self._watched = set()
        # Modules we have already notified about, to avoid firing twice if
        # the same module gets imported through multiple paths.
        self._processed = set()

    def watch(self, module_name):
        """Watch ``module_name``. When it finishes executing,
        :meth:`SerializerStore._on_module_imported` is called."""
        self._watched.add(module_name)

    def find_spec(self, fullname, path, target=None):
        if fullname not in self._watched:
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
        if module_name not in self._watched:
            return
        try:
            from .serializer import SerializerStore

            SerializerStore._on_module_imported(module_name, module)
        except Exception as e:
            # A broken callback must not break the host's import — but stay
            # visible so a registry-level bug does not silently disable
            # extension serializers. ``SerializerStore._on_module_imported``
            # converts per-record failures to ``rec.state = BROKEN``, so
            # reaching this branch implies a programming error in the
            # registry itself rather than an extension bug.
            import warnings

            warnings.warn(
                "Artifact serializer registry callback crashed for module "
                "'%s' (%s: %s); affected serializers may be left in a "
                "PENDING state." % (module_name, type(e).__name__, e),
                RuntimeWarning,
                stacklevel=2,
            )


_interceptor = _SerializerImportInterceptor()


def _ensure_interceptor_installed():
    # Already at position 0 — nothing to do; the remove+reinsert below would
    # be a no-op and creates a brief window where the interceptor is missing
    # from ``sys.meta_path`` for any concurrent import.
    if sys.meta_path and sys.meta_path[0] is _interceptor:
        return
    if _interceptor in sys.meta_path:
        sys.meta_path.remove(_interceptor)
    sys.meta_path.insert(0, _interceptor)


def _reset_for_tests():
    """Clear all module-level state. Intended for unit tests only."""
    _interceptor._watched.clear()
    _interceptor._processed.clear()
    if _interceptor in sys.meta_path:
        sys.meta_path.remove(_interceptor)
