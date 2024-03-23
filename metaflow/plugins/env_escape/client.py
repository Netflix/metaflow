import fcntl
import gc
import os
import importlib
import itertools
import select
from subprocess import Popen, PIPE
import sys
import threading
import time

from . import data_transferer

from .consts import (
    FIELD_ARGS,
    FIELD_CONTENT,
    FIELD_KWARGS,
    FIELD_MSGTYPE,
    FIELD_OPTYPE,
    FIELD_TARGET,
    MSG_CONTROL,
    MSG_EXCEPTION,
    MSG_INTERNAL_ERROR,
    MSG_OP,
    MSG_REPLY,
    OP_GETMETHODS,
    VALUE_LOCAL,
    VALUE_REMOTE,
    CONTROL_GETEXPORTS,
    CONTROL_SHUTDOWN,
)

from .communication.channel import Channel
from .communication.socket_bytestream import SocketByteStream

from .data_transferer import DataTransferer, ObjReference
from .exception_transferer import ExceptionMetaClass, load_exception
from .override_decorators import (
    LocalAttrOverride,
    LocalExceptionDeserializer,
    LocalOverride,
)
from .stub import create_class
from .utils import get_canonical_name

BIND_TIMEOUT = 0.1
BIND_RETRY = 0


class Client(object):
    def __init__(
        self, modules, python_executable, pythonpath, max_pickle_version, config_dir
    ):
        # Wrap with ImportError so that if users are just using the escaped module
        # as optional, the typical logic of catching ImportError works properly
        try:
            self.inner_init(
                python_executable, pythonpath, max_pickle_version, config_dir
            )
        except Exception as e:
            # Typically it's one override per config so we just use the first one.
            raise ImportError("Error loading module: %s" % str(e), name=modules[0])

    def inner_init(self, python_executable, pythonpath, max_pickle_version, config_dir):
        # Make sure to init these variables (used in __del__) early on in case we
        # have an exception
        self._poller = None
        self._poller_lock = threading.Lock()
        self._active_pid = os.getpid()
        self._server_process = None
        self._socket_path = None

        data_transferer.defaultProtocol = max_pickle_version

        self._config_dir = config_dir
        server_path, server_config = os.path.split(config_dir)
        # The client launches the server when created; we use
        # Unix sockets for now
        server_module = ".".join([__package__, "server"])
        self._socket_path = "/tmp/%s_%d" % (server_config, self._active_pid)
        if os.path.exists(self._socket_path):
            raise RuntimeError("Existing socket: %s" % self._socket_path)
        env = os.environ.copy()
        env["PYTHONPATH"] = pythonpath
        # When coming from a conda environment, LD_LIBRARY_PATH may be set to
        # first include the Conda environment's library. When breaking out to
        # the underlying python, we need to reset it to the original LD_LIBRARY_PATH
        ld_lib_path = env.get("LD_LIBRARY_PATH")
        orig_ld_lib_path = env.get("MF_ORIG_LD_LIBRARY_PATH")
        if ld_lib_path is not None and orig_ld_lib_path is not None:
            env["LD_LIBRARY_PATH"] = orig_ld_lib_path
            if orig_ld_lib_path is not None:
                del env["MF_ORIG_LD_LIBRARY_PATH"]
        self._server_process = Popen(
            [
                python_executable,
                "-u",
                "-m",
                server_module,
                str(max_pickle_version),
                server_config,
                self._socket_path,
            ],
            cwd=server_path,
            env=env,
            stdout=PIPE,
            stderr=PIPE,
            bufsize=1,
            universal_newlines=True,  # Forces text as well
        )

        # Read override configuration
        # We can't just import the "overrides" module because that does not
        # distinguish it from other modules named "overrides" (either a third party
        # lib -- there is one -- or just other escaped modules). We therefore load
        # a fuller path to distinguish them from one another.
        # This is a bit tricky though:
        #  - it requires all `configurations` directories to NOT have a __init__.py
        #    so that configurations can be loaded through extensions too. If this is
        #    not the case, we will have a `configurations` module that will be registered
        #    and not be a proper namespace package
        #  - we want to import a specific file so we:
        #    - set a prefix that is specific enough to NOT include anything outside
        #      of the configuration so we end the prefix with "env_escape" (we know
        #      that is in the path of all configurations). We could technically go
        #      up to metaflow or metaflow_extensions BUT this then causes issues with
        #      the extension mechanism and _resolve_relative_path in plugins (because
        #      there are files loaded from plugins that refer to something outside of
        #      plugins and if we load plugins and NOT metaflow.plugins, this breaks).
        #    - set the package root from this prefix to everything up to overrides
        #    - load the overrides file
        #
        # This way, we are sure that we are:
        #  - loading this specific overrides
        #  - not adding extra stuff to the prefix that we don't care about
        #  - able to support configurations in both metaflow and extensions at the
        #    same time
        pkg_components = []
        prefix, last_basename = os.path.split(config_dir)
        while True:
            pkg_components.append(last_basename)
            possible_prefix, last_basename = os.path.split(prefix)
            if last_basename == "env_escape":
                break
            prefix = possible_prefix

        try:
            sys.path.insert(0, prefix)
            override_module = importlib.import_module(
                ".overrides", package=".".join(reversed(pkg_components))
            )
            override_values = override_module.__dict__.values()
        except ImportError:
            # We ignore so the file can be non-existent if not needed
            override_values = []
        except Exception as e:
            raise RuntimeError(
                "Cannot import overrides from '%s': %s" % (sys.path[0], str(e))
            )
        finally:
            sys.path = sys.path[1:]

        self._proxied_objects = {}

        # Wait for the socket to be up on the other side; we also check if the
        # server had issues starting up in which case we report that and crash out
        while not os.path.exists(self._socket_path):
            returncode = self._server_process.poll()
            if returncode is not None:
                raise RuntimeError(
                    "Server did not properly start: %s"
                    % self._server_process.stderr.read(),
                )
            time.sleep(1)
        # Open up the channel and set up the datatransferer pipeline
        self._channel = Channel(SocketByteStream.unixconnect(self._socket_path))
        self._datatransferer = DataTransferer(self)

        # Make PIPEs non-blocking; this is helpful to be able to
        # order the messages properly
        for f in (self._server_process.stdout, self._server_process.stderr):
            fl = fcntl.fcntl(f, fcntl.F_GETFL)
            fcntl.fcntl(f, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        # Set up poller
        with self._poller_lock:
            self._poller = select.poll()
            self._poller.register(self._server_process.stdout, select.POLLIN)
            self._poller.register(self._server_process.stderr, select.POLLIN)
            self._poller.register(self._channel, select.POLLIN | select.POLLHUP)

        # Get all exports that we are proxying
        response = self._communicate(
            {FIELD_MSGTYPE: MSG_CONTROL, FIELD_OPTYPE: CONTROL_GETEXPORTS}
        )

        self._proxied_classes = {
            k: None
            for k in itertools.chain(
                response[FIELD_CONTENT]["classes"],
                response[FIELD_CONTENT]["proxied"],
                (e[0] for e in response[FIELD_CONTENT]["exceptions"]),
            )
        }

        self._exception_hierarchy = dict(response[FIELD_CONTENT]["exceptions"])
        self._proxied_classnames = set(response[FIELD_CONTENT]["classes"]).union(
            response[FIELD_CONTENT]["proxied"]
        )
        self._aliases = response[FIELD_CONTENT]["aliases"]

        # Determine all overrides
        self._overrides = {}
        self._getattr_overrides = {}
        self._setattr_overrides = {}
        self._exception_deserializers = {}
        for override in override_values:
            if isinstance(override, (LocalOverride, LocalAttrOverride)):
                for obj_name, obj_funcs in override.obj_mapping.items():
                    canonical_name = get_canonical_name(obj_name, self._aliases)
                    if canonical_name not in self._proxied_classes:
                        raise ValueError(
                            "%s does not refer to a proxied or override type" % obj_name
                        )
                    if isinstance(override, LocalOverride):
                        override_dict = self._overrides.setdefault(canonical_name, {})
                    elif override.is_setattr:
                        override_dict = self._setattr_overrides.setdefault(
                            canonical_name, {}
                        )
                    else:
                        override_dict = self._getattr_overrides.setdefault(
                            canonical_name, {}
                        )
                    if isinstance(obj_funcs, str):
                        obj_funcs = (obj_funcs,)
                    for name in obj_funcs:
                        if name in override_dict:
                            raise ValueError(
                                "%s was already overridden for %s" % (name, obj_name)
                            )
                        override_dict[name] = override.func
            if isinstance(override, LocalExceptionDeserializer):
                canonical_name = get_canonical_name(override.class_path, self._aliases)
                if canonical_name not in self._exception_hierarchy:
                    raise ValueError(
                        "%s does not refer to an exception type" % override.class_path
                    )
                cur_des = self._exception_deserializers.get(canonical_name, None)
                if cur_des is not None:
                    raise ValueError(
                        "Exception %s has multiple deserializers" % override.class_path
                    )
                self._exception_deserializers[canonical_name] = override.deserializer

        # Proxied standalone functions are functions that are proxied
        # as part of other objects like defaultdict for which we create a
        # on-the-fly simple class that is just a callable. This is therefore
        # a special type of self._proxied_classes
        self._proxied_standalone_functions = {}

        self._export_info = {
            "classes": response[FIELD_CONTENT]["classes"],
            "functions": response[FIELD_CONTENT]["functions"],
            "values": response[FIELD_CONTENT]["values"],
            "exceptions": response[FIELD_CONTENT]["exceptions"],
            "aliases": response[FIELD_CONTENT]["aliases"],
        }

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        # Clean up the server; we drain all messages if any
        if self._poller is not None:
            # If we have self._poller, we have self._server_process
            with self._poller_lock:
                self._poller.unregister(self._channel)
                last_evts = self._poller.poll(5)
            for fd, _ in last_evts:
                # Readlines will never block here because `bufsize` is set to
                # 1 (line buffering)
                if fd == self._server_process.stdout.fileno():
                    sys.stdout.write(self._server_process.stdout.readline())
                elif fd == self._server_process.stderr.fileno():
                    sys.stderr.write(self._server_process.stderr.readline())
            sys.stdout.flush()
            sys.stderr.flush()
            self._poller = None
        if self._server_process is not None:
            # Attempt to send it a terminate signal and then wait and kill
            try:
                self._channel.send(
                    {FIELD_MSGTYPE: MSG_CONTROL, FIELD_OPTYPE: CONTROL_SHUTDOWN}
                )
                self._channel.recv(timeout=10)  # If we receive, we are sure we
                # are good
            except:  # noqa E722
                pass  # If there is any issue sending this message, just ignore it
            self._server_process.kill()
            self._server_process = None
        if self._socket_path is not None and os.path.exists(self._socket_path):
            os.unlink(self._socket_path)
            self._socket_path = None

    @property
    def name(self):
        return self._config_dir

    def get_exports(self):
        return self._export_info

    def get_exception_deserializer(self, name):
        cannonical_name = get_canonical_name(name, self._aliases)
        return self._exception_deserializers.get(cannonical_name)

    def stub_request(self, stub, request_type, *args, **kwargs):
        # Encode the operation to send over the wire and wait for the response
        target = self.encode(stub)
        encoded_args = []
        for arg in args:
            encoded_args.append(self.encode(arg))
        encoded_kwargs = []
        for k, v in kwargs.items():
            encoded_kwargs.append((self.encode(k), self.encode(v)))
        response = self._communicate(
            {
                FIELD_MSGTYPE: MSG_OP,
                FIELD_OPTYPE: request_type,
                FIELD_TARGET: target,
                FIELD_ARGS: self.encode(args),
                FIELD_KWARGS: self.encode([(k, v) for k, v in kwargs.items()]),
            }
        )
        response_type = response[FIELD_MSGTYPE]
        if response_type == MSG_REPLY:
            return self.decode(response[FIELD_CONTENT])
        elif response_type == MSG_EXCEPTION:
            raise load_exception(self, response[FIELD_CONTENT])
        elif response_type == MSG_INTERNAL_ERROR:
            raise RuntimeError(
                "Error in the server runtime:\n\n===== SERVER TRACEBACK =====\n%s"
                % response[FIELD_CONTENT]
            )

    def encode(self, obj):
        # This encodes an object to transfer back out
        # Basic data types will be sent over directly.
        # In this direction (client -> server), we error on non-basic
        # types. This could be changed by modifying the pickle_object method
        # here.
        return self._datatransferer.dump(obj)

    def decode(self, json_obj):
        # This decodes an object that was transferred in. This will call
        # unpickle_object where needed. Any remote object that is handled by
        # this connection will be converted to a local stub.
        return self._datatransferer.load(json_obj)

    def get_local_class(
        self, name, obj_id=None, is_returned_exception=False, is_parent=False
    ):
        # Gets (and creates if needed), the class mapping to the remote
        # class of name 'name'.

        # We actually deal with four types of classes:
        # - proxied functions
        # - classes that are proxied regular classes AND proxied exceptions
        # - classes that are proxied regular classes AND NOT proxied exceptions
        # - classes that are NOT proxied regular classes AND are proxied exceptions
        name = get_canonical_name(name, self._aliases)

        def name_to_parent_name(name):
            return "parent:%s" % name

        if is_parent:
            lookup_name = name_to_parent_name(name)
        else:
            lookup_name = name

        if name == "function":
            # Special handling of pickled functions. We create a new class that
            # simply has a __call__ method that will forward things back to
            # the server side.
            if obj_id is None:
                raise RuntimeError("Local function unpickling without an object ID")
            if obj_id not in self._proxied_standalone_functions:
                self._proxied_standalone_functions[obj_id] = create_class(
                    self,
                    "__main__.__function_%s" % obj_id,
                    {},
                    {},
                    {},
                    {"__call__": ""},
                    [],
                )
            return self._proxied_standalone_functions[obj_id]
        local_class = self._proxied_classes.get(lookup_name, None)
        if local_class is not None:
            return local_class

        is_proxied_exception = name in self._exception_hierarchy
        is_proxied_non_exception = name in self._proxied_classnames

        if not is_proxied_exception and not is_proxied_non_exception:
            if is_returned_exception or is_parent:
                # In this case, it may be a local exception that we need to
                # recreate
                try:
                    ex_module, ex_name = name.rsplit(".", 1)
                    __import__(ex_module, None, None, "*")
                except Exception:
                    pass
                if ex_module in sys.modules and issubclass(
                    getattr(sys.modules[ex_module], ex_name), BaseException
                ):
                    # This is a local exception that we can recreate
                    local_exception = getattr(sys.modules[ex_module], ex_name)
                    wrapped_exception = ExceptionMetaClass(
                        ex_name,
                        (local_exception,),
                        dict(getattr(local_exception, "__dict__", {})),
                    )
                    wrapped_exception.__module__ = ex_module
                    self._proxied_classes[lookup_name] = wrapped_exception
                    return wrapped_exception

            raise ValueError("Class '%s' is not known" % name)

        # At this stage:
        # - we don't have a local_class for this
        # - it is not an inbuilt exception so it is either a proxied exception, a
        #   proxied class or a proxied object that is both an exception and a class.

        parents = []
        if is_proxied_exception:
            # If exception, we need to get the parents from the exception
            ex_parents = self._exception_hierarchy[name]
            for parent in ex_parents:
                # We always consider it to be an exception so that we wrap even non
                # proxied builtins exceptions
                parents.append(self.get_local_class(parent, is_parent=True))
        # For regular classes, we get what it exposes from the server
        if is_proxied_non_exception:
            remote_methods = self.stub_request(None, OP_GETMETHODS, name)
        else:
            remote_methods = {}

        parent_local_class = None
        local_class = None
        if is_proxied_exception:
            # If we are a proxied exception AND a proxied class, we create two classes:
            # actually:
            #  - the class itself (which is a stub)
            #  - the class in the capacity of a parent class (to another exception
            #    presumably). The reason for this is that if we have an exception/proxied
            #    class A and another B and B inherits from A, the MRO order would be all
            #    wrong since both A and B would also inherit from `Stub`. Here what we
            #    do is:
            #      - A_parent inherits from the actual parents of A (let's assume a
            #        builtin exception)
            #      - A inherits from (Stub, A_parent)
            #      - B_parent inherits from A_parent and the builtin Exception
            #      - B inherits from (Stub, B_parent)
            ex_module, ex_name = name.rsplit(".", 1)
            parent_local_class = ExceptionMetaClass(ex_name, (*parents,), {})
            parent_local_class.__module__ = ex_module

        if is_proxied_non_exception:
            local_class = create_class(
                self,
                name,
                self._overrides.get(name, {}),
                self._getattr_overrides.get(name, {}),
                self._setattr_overrides.get(name, {}),
                remote_methods,
                (parent_local_class,) if parent_local_class else None,
            )
        if parent_local_class:
            self._proxied_classes[name_to_parent_name(name)] = parent_local_class
        if local_class:
            self._proxied_classes[name] = local_class
        else:
            # This is for the case of pure proxied exceptions -- we want the lookup of
            # foo.MyException to be the same class as looking of foo.MyException as a parent
            # of another exception so `isinstance` works properly
            self._proxied_classes[name] = parent_local_class

        if is_parent:
            # This should never happen but making sure
            if not parent_local_class:
                raise RuntimeError(
                    "Exception parent class %s is not a proxied exception" % name
                )
            return parent_local_class
        return self._proxied_classes[name]

    def can_pickle(self, obj):
        return getattr(obj, "___connection___", None) == self

    def pickle_object(self, obj):
        # This function is called to pickle obj to be transferred back to the
        # server. In this direction, we only allow objects that already exist
        # on the remote side so if this is not a stub, we do not allow it to be
        # transferred
        if getattr(obj, "___connection___", None) == self:
            # This is something we can transfer over
            return ObjReference(
                VALUE_LOCAL, obj.___remote_class_name___, obj.___identifier___
            )

        raise ValueError(
            "Cannot send object of type %s from client to server" % type(obj)
        )

    def unpickle_object(self, obj):
        # This function is called when the server sends a remote reference.
        # We create a local stub for it locally
        if (not isinstance(obj, ObjReference)) or obj.value_type != VALUE_REMOTE:
            raise ValueError("Invalid transferred object: %s" % str(obj))
        remote_class_name = obj.class_name
        obj_id = obj.identifier
        local_instance = self._proxied_objects.get(obj_id)
        if not local_instance:
            local_class = self.get_local_class(remote_class_name, obj_id=obj_id)
            local_instance = local_class(self, remote_class_name, obj_id)
        return local_instance

    def _communicate(self, msg):
        if os.getpid() != self._active_pid:
            raise RuntimeError(
                "You cannot use the environment escape across process boundaries."
            )
        # We also disable the GC because in some rare cases, it may try to delete
        # a remote object while we are communicating which will cause a deadlock
        try:
            gc.disable()
            with self._poller_lock:
                return self._locked_communicate(msg)
        finally:
            gc.enable()

    def _locked_communicate(self, msg):
        self._channel.send(msg)
        response_ready = False
        while not response_ready:
            evt_list = self._poller.poll()
            for fd, _ in evt_list:
                if fd == self._channel.fileno():
                    # We deal with this last as this basically gives us the
                    # response, so we stop looking at things on stdout/stderr
                    response_ready = True
                # Readlines will never block here because `bufsize` is set to 1
                # (line buffering)
                elif fd == self._server_process.stdout.fileno():
                    sys.stdout.write(self._server_process.stdout.readline())
                elif fd == self._server_process.stderr.fileno():
                    sys.stderr.write(self._server_process.stderr.readline())
        # We make sure there is nothing left to read. On the server side a
        # flush happens before we respond, so we read until we get an exception;
        # this is non-blocking
        while True:
            try:
                line = self._server_process.stdout.readline()
                if not line:
                    break
                sys.stdout.write(line)
            except (OSError, TypeError):
                break
        while True:
            try:
                line = self._server_process.stderr.readline()
                if not line:
                    break
                sys.stderr.write(line)
            except (OSError, TypeError):
                break
        sys.stdout.flush()
        sys.stderr.flush()
        return self._channel.recv()
