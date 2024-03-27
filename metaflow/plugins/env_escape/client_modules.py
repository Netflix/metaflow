import atexit
import importlib
import itertools
import pickle
import re
import sys

from .consts import OP_CALLFUNC, OP_GETVAL, OP_SETVAL
from .client import Client
from .utils import get_canonical_name


def _clean_client(client):
    client.cleanup()


class _WrappedModule(object):
    def __init__(self, loader, prefix, exports, client):
        self._loader = loader
        self._prefix = prefix
        self._client = client
        is_match = re.compile(
            r"^%s\.([a-zA-Z_][a-zA-Z0-9_]*)$" % prefix.replace(".", r"\.")  # noqa W605
        )
        self._exports = {}
        self._aliases = exports.get("aliases", [])
        for k in ("classes", "functions", "values"):
            result = []
            for item in exports.get(k, []):
                m = is_match.match(item)
                if m:
                    result.append(m.group(1))
            self._exports[k] = result
        result = []
        for item, _ in exports.get("exceptions", []):
            m = is_match.match(item)
            if m:
                result.append(m.group(1))
        self._exports["exceptions"] = result

    def __getattr__(self, name):
        if name == "__loader__":
            return self._loader
        if name in ("__name__", "__package__"):
            return self._prefix
        if name in ("__file__", "__path__"):
            return self._client.name

        # Make the name canonical because the prefix is also canonical.
        name = get_canonical_name(self._prefix + "." + name, self._aliases)[
            len(self._prefix) + 1 :
        ]
        if name in self._exports["classes"] or name in self._exports["exceptions"]:
            # We load classes and exceptions lazily
            return self._client.get_local_class("%s.%s" % (self._prefix, name))
        elif name in self._exports["functions"]:
            # TODO: Grab doc back from the remote side like in _make_method
            def func(*args, **kwargs):
                return self._client.stub_request(
                    None, OP_CALLFUNC, "%s.%s" % (self._prefix, name), *args, **kwargs
                )

            func.__name__ = name
            func.__doc__ = "Unknown (TODO)"
            return func
        elif name in self._exports["values"]:
            return self._client.stub_request(
                None, OP_GETVAL, "%s.%s" % (self._prefix, name)
            )
        else:
            # Try to see if this is a submodule that we can load
            m = None
            try:
                m = self._loader.load_module(".".join([self._prefix, name]))
            except ImportError:
                pass
            if m is None:
                raise AttributeError(
                    "module '%s' has no attribute '%s' -- contact the author of the "
                    "configuration if this is something "
                    "you expect to work (support may be added if it exists in the "
                    "original library)" % (self._prefix, name)
                )
            return m

    def __setattr__(self, name, value):
        if name in (
            "package",
            "__spec__",
            "_loader",
            "_prefix",
            "_client",
            "_exports",
            "_exception_classes",
            "_aliases",
        ):
            object.__setattr__(self, name, value)
            return
        if isinstance(value, _WrappedModule):
            # This is when a module sets itself as an attribute of another
            # module when loading
            object.__setattr__(self, name, value)
            return

        # Make the name canonical because the prefix is also canonical.
        name = get_canonical_name(self._prefix + "." + name, self._aliases)[
            len(self._prefix) + 1 :
        ]
        if name in self._exports["values"]:
            self._client.stub_request(
                None, OP_SETVAL, "%s.%s" % (self._prefix, name), value
            )
        elif name in self._exports["classes"] or name in self._exports["functions"]:
            raise ValueError
        else:
            raise AttributeError(name)


class ModuleImporter(object):
    # This ModuleImporter implements the Importer Protocol defined in PEP 302
    def __init__(
        self,
        python_executable,
        pythonpath,
        max_pickle_version,
        config_dir,
        module_prefixes,
    ):
        self._module_prefixes = module_prefixes
        self._python_executable = python_executable
        self._pythonpath = pythonpath
        self._config_dir = config_dir
        self._client = None
        self._max_pickle_version = max_pickle_version
        self._handled_modules = None
        self._aliases = {}

    def find_module(self, fullname, path=None):
        if self._handled_modules is not None:
            if get_canonical_name(fullname, self._aliases) in self._handled_modules:
                return self
            return None
        if any([fullname.startswith(prefix) for prefix in self._module_prefixes]):
            # We potentially handle this
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        if self._client is None:
            if sys.version_info[0] < 3:
                raise NotImplementedError(
                    "Environment escape imports are not supported in Python 2"
                )
            # We initialize a client and query the modules we handle
            # The max_pickle_version is the pickle version that the server (so
            # the underlying interpreter we call into) supports; we determine
            # what version the current environment support and take the minimum
            # of those two
            max_pickle_version = min(self._max_pickle_version, pickle.HIGHEST_PROTOCOL)

            self._client = Client(
                self._module_prefixes,
                self._python_executable,
                self._pythonpath,
                max_pickle_version,
                self._config_dir,
            )
            atexit.register(_clean_client, self._client)

            # Get information about overrides and what the server knows about
            exports = self._client.get_exports()

            prefixes = set()
            export_classes = exports.get("classes", [])
            export_functions = exports.get("functions", [])
            export_values = exports.get("values", [])
            export_exceptions = exports.get("exceptions", [])
            self._aliases = exports.get("aliases", {})
            for name in itertools.chain(
                export_classes,
                export_functions,
                export_values,
                (e[0] for e in export_exceptions),
            ):
                splits = name.rsplit(".", 1)
                prefixes.add(splits[0])
            # We will make sure that we create modules even for "empty" prefixes
            # because packages are always loaded hierarchically so if we have
            # something in `a.b.c` but nothing directly in `a`, we still need to
            # create a module named `a`. There is probably a better way of doing this
            all_prefixes = list(prefixes)
            for prefix in all_prefixes:
                parts = prefix.split(".")
                cur = parts[0]
                for i in range(1, len(parts)):
                    prefixes.add(cur)
                    cur = ".".join([cur, parts[i]])

            # We now know all the modules that we can handle. We update
            # handled_module and return the module if we have it or raise ImportError
            self._handled_modules = {}
            for prefix in prefixes:
                self._handled_modules[prefix] = _WrappedModule(
                    self, prefix, exports, self._client
                )
        canonical_fullname = get_canonical_name(fullname, self._aliases)
        # Modules are created canonically but we need to return something for any
        # of the aliases.
        module = self._handled_modules.get(canonical_fullname)
        if module is None:
            raise ImportError
        sys.modules[fullname] = module
        return module


def create_modules(python_executable, pythonpath, max_pickle_version, path, prefixes):
    # This is an extra verification to make sure we are not trying to use the
    # environment escape for something that is in the system
    for prefix in prefixes:
        try:
            importlib.import_module(prefix)
        except ImportError:
            pass
        else:
            # pass
            raise RuntimeError(
                "Trying to override %s when module exists in system" % prefix
            )

    # The first version forces the use of the environment escape even if the module
    # exists in the system. This is useful for testing to make sure that the
    # environment escape is used. The second version is more production friendly and
    # will only use the environment escape if the module cannot be found

    # sys.meta_path.insert(0, ModuleImporter(python_path, path, prefixes))
    sys.meta_path.append(
        ModuleImporter(
            python_executable, pythonpath, max_pickle_version, path, prefixes
        )
    )
