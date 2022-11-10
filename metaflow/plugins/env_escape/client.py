import fcntl
import os
import importlib
import itertools
import select
from subprocess import Popen, PIPE
import sys
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
from .exception_transferer import load_exception
from .override_decorators import LocalAttrOverride, LocalException, LocalOverride
from .stub import create_class

BIND_TIMEOUT = 0.1
BIND_RETRY = 0


class Client(object):
    def __init__(self, python_executable, pythonpath, max_pickle_version, config_dir):
        # Make sure to init these variables (used in __del__) early on in case we
        # have an exception
        self._poller = None
        self._server_process = None
        self._socket_path = None

        data_transferer.defaultProtocol = max_pickle_version

        self._config_dir = config_dir
        server_path, server_config = os.path.split(config_dir)
        # The client launches the server when created; we use
        # Unix sockets for now
        server_module = ".".join([__package__, "server"])
        self._socket_path = "/tmp/%s_%d" % (server_config, os.getpid())
        if os.path.exists(self._socket_path):
            raise RuntimeError("Existing socket: %s" % self._socket_path)
        env = os.environ.copy()
        env["PYTHONPATH"] = pythonpath
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
        pkg_components = []
        prefix, last_basename = os.path.split(config_dir)
        while last_basename not in ("metaflow", "metaflow_extensions"):
            pkg_components.append(last_basename)
            prefix, last_basename = os.path.split(prefix)
        pkg_components.append(last_basename)

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
                response[FIELD_CONTENT]["classes"], response[FIELD_CONTENT]["proxied"]
            )
        }

        # Determine all overrides
        self._overrides = {}
        self._getattr_overrides = {}
        self._setattr_overrides = {}
        self._exception_overrides = {}
        for override in override_values:
            if isinstance(override, (LocalOverride, LocalAttrOverride)):
                for obj_name, obj_funcs in override.obj_mapping.items():
                    if obj_name not in self._proxied_classes:
                        raise ValueError(
                            "%s does not refer to a proxied or override type" % obj_name
                        )
                    if isinstance(override, LocalOverride):
                        override_dict = self._overrides.setdefault(obj_name, {})
                    elif override.is_setattr:
                        override_dict = self._setattr_overrides.setdefault(obj_name, {})
                    else:
                        override_dict = self._getattr_overrides.setdefault(obj_name, {})
                    if isinstance(obj_funcs, str):
                        obj_funcs = (obj_funcs,)
                    for name in obj_funcs:
                        if name in override_dict:
                            raise ValueError(
                                "%s was already overridden for %s" % (name, obj_name)
                            )
                        override_dict[name] = override.func
            if isinstance(override, LocalException):
                cur_ex = self._exception_overrides.get(override.class_path, None)
                if cur_ex is not None:
                    raise ValueError("Exception %s redefined" % override.class_path)
                self._exception_overrides[override.class_path] = override.wrapped_class

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

        self._aliases = response[FIELD_CONTENT]["aliases"]

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        # Clean up the server; we drain all messages if any
        if self._poller is not None:
            # If we have self._poller, we have self._server_process
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

    def get_local_exception_overrides(self):
        return self._exception_overrides

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
            raise load_exception(self._datatransferer, response[FIELD_CONTENT])
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

    def get_local_class(self, name, obj_id=None):
        # Gets (and creates if needed), the class mapping to the remote
        # class of name 'name'.
        name = self._get_canonical_name(name)
        if name == "function":
            # Special handling of pickled functions. We create a new class that
            # simply has a __call__ method that will forward things back to
            # the server side.
            if obj_id is None:
                raise RuntimeError("Local function unpickling without an object ID")
            if obj_id not in self._proxied_standalone_functions:
                self._proxied_standalone_functions[obj_id] = create_class(
                    self, "__function_%s" % obj_id, {}, {}, {}, {"__call__": ""}
                )
            return self._proxied_standalone_functions[obj_id]

        if name not in self._proxied_classes:
            raise ValueError("Class '%s' is not known" % name)
        local_class = self._proxied_classes[name]
        if local_class is None:
            # We need to build up this class. To do so, we take everything that the
            # remote class has and remove UNSUPPORTED things and overridden things
            remote_methods = self.stub_request(None, OP_GETMETHODS, name)
            local_class = create_class(
                self,
                name,
                self._overrides.get(name, {}),
                self._getattr_overrides.get(name, {}),
                self._setattr_overrides.get(name, {}),
                remote_methods,
            )
            self._proxied_classes[name] = local_class
        return local_class

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
            local_class = self.get_local_class(remote_class_name, obj_id)
            local_instance = local_class(self, remote_class_name, obj_id)
        return local_instance

    def _get_canonical_name(self, name):
        # We look at the aliases looking for the most specific match first
        base_name = self._aliases.get(name)
        if base_name is not None:
            return base_name
        for idx in reversed([pos for pos, char in enumerate(name) if char == "."]):
            base_name = self._aliases.get(name[:idx])
            if base_name is not None:
                return ".".join([base_name, name[idx + 1 :]])
        return name

    def _communicate(self, msg):
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
