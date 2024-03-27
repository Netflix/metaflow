import importlib
import itertools
import pickle
import socket
import sys
import traceback

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
    OP_GETATTR,
    OP_SETATTR,
    OP_DELATTR,
    OP_CALL,
    OP_CALLATTR,
    OP_REPR,
    OP_STR,
    OP_HASH,
    OP_PICKLE,
    OP_DEL,
    OP_GETMETHODS,
    OP_DIR,
    OP_CALLFUNC,
    OP_CALLONCLASS,
    OP_GETVAL,
    OP_SETVAL,
    OP_INIT,
    OP_SUBCLASSCHECK,
    VALUE_LOCAL,
    VALUE_REMOTE,
    CONTROL_GETEXPORTS,
    CONTROL_SHUTDOWN,
)

from .communication.channel import Channel
from .communication.socket_bytestream import SocketByteStream
from .communication.utils import __try_op__

from .data_transferer import DataTransferer, ObjReference
from .override_decorators import (
    RemoteAttrOverride,
    RemoteOverride,
    RemoteExceptionSerializer,
)
from .exception_transferer import dump_exception
from .utils import get_methods, get_canonical_name

BIND_TIMEOUT = 0.1
BIND_RETRY = 1


class Server(object):
    def __init__(self, config_dir, max_pickle_version):
        self._max_pickle_version = data_transferer.defaultProtocol = max_pickle_version
        try:
            mappings = importlib.import_module(".server_mappings", package=config_dir)
        except Exception as e:
            raise RuntimeError(
                "Cannot import server_mappings from '%s': %s" % (sys.path[0], str(e))
            )
        try:
            # Import module as a relative package to make sure that it is consistent
            # with how the client does it -- this enables us to do the same type of
            # relative imports in overrides
            override_module = importlib.import_module(".overrides", package=config_dir)
            override_values = override_module.__dict__.values()
        except ImportError:
            # We ignore so the file can be non-existent if not needed
            override_values = []
        except Exception as e:
            raise RuntimeError(
                "Cannot import overrides from '%s': %s" % (sys.path[0], str(e))
            )

        self._aliases = {}
        self._known_classes, a1 = self._flatten_dict(mappings.EXPORTED_CLASSES)
        self._class_types_to_names = {v: k for k, v in self._known_classes.items()}
        self._known_funcs, a2 = self._flatten_dict(mappings.EXPORTED_FUNCTIONS)
        self._known_vals, a3 = self._flatten_dict(mappings.EXPORTED_VALUES)
        self._known_exceptions, a4 = self._flatten_dict(mappings.EXPORTED_EXCEPTIONS)
        self._proxied_types = {
            "%s.%s" % (t.__module__, t.__name__): t for t in mappings.PROXIED_CLASSES
        }
        self._class_types_to_names.update(
            {v: k for k, v in self._proxied_types.items()}
        )

        # We will also proxy functions from objects as needed. This is useful
        # for defaultdict for example since the `default_factory` function is a
        # lambda that needs to be transferred.
        self._class_types_to_names[type(lambda x: x)] = "function"

        # Update all alias information
        for base_name, aliases in itertools.chain(
            a1.items(), a2.items(), a3.items(), a4.items()
        ):
            for alias in aliases:
                a = self._aliases.setdefault(alias, base_name)
                if a != base_name:
                    # Technically we could have a that aliases b and b that aliases c
                    # and then a that aliases c. This would error out in that case
                    # even though it is valid. It is easy for the user to get around
                    # this by listing aliases in the same order so we don't support
                    # it for now.
                    raise ValueError(
                        "%s is an alias to both %s and %s -- make sure all aliases "
                        "are listed in the same order" % (alias, base_name, a)
                    )

        # Detect circular aliases. If a user lists ("a", "b") and then ("b", "a"), we
        # will have an entry in aliases saying b is an alias for a and a is an alias
        # for b which is a recipe for disaster since we no longer have a cannonical name
        # for things.
        for alias, base_name in self._aliases.items():
            if base_name in self._aliases:
                raise ValueError(
                    "%s and %s are circular aliases -- make sure all aliases "
                    "are listed in the same order" % (alias, base_name)
                )

        # Determine if we have any overrides
        self._overrides = {}
        self._getattr_overrides = {}
        self._setattr_overrides = {}
        self._exception_serializers = {}
        for override in override_values:
            if isinstance(override, (RemoteAttrOverride, RemoteOverride)):
                for obj_name, obj_funcs in override.obj_mapping.items():
                    canonical_name = get_canonical_name(obj_name, self._aliases)
                    obj_type = self._known_classes.get(
                        canonical_name, self._proxied_types.get(obj_name)
                    )
                    if obj_type is None:
                        raise ValueError(
                            "%s does not refer to a proxied or exported type" % obj_name
                        )
                    if isinstance(override, RemoteOverride):
                        override_dict = self._overrides.setdefault(obj_type, {})
                    elif override.is_setattr:
                        override_dict = self._setattr_overrides.setdefault(obj_type, {})
                    else:
                        override_dict = self._getattr_overrides.setdefault(obj_type, {})
                    if isinstance(obj_funcs, str):
                        obj_funcs = (obj_funcs,)
                    for name in obj_funcs:
                        if name in override_dict:
                            raise ValueError(
                                "%s was already overridden for %s" % (name, obj_name)
                            )
                        override_dict[name] = override.func
            elif isinstance(override, RemoteExceptionSerializer):
                canonical_name = get_canonical_name(override.class_path, self._aliases)
                if canonical_name not in self._known_exceptions:
                    raise ValueError(
                        "%s does not refer to an exported exception"
                        % override.class_path
                    )
                if override.class_path in self._exception_serializers:
                    raise ValueError(
                        "%s exception serializer already defined" % override.class_path
                    )
                self._exception_serializers[canonical_name] = override.serializer

        # Process the exceptions making sure we have all the ones we need and building a
        # topologically sorted list for the client to instantiate
        name_to_parent_count = {}
        name_to_parents = {}
        parent_to_child = {}

        for ex_name, ex_cls in self._known_exceptions.items():
            ex_name_canonical = get_canonical_name(ex_name, self._aliases)
            parents = []
            for base in ex_cls.__mro__[1:]:
                if base is object:
                    raise ValueError(
                        "Exported exceptions not rooted in a builtin exception "
                        "are not supported: %s." % ex_name
                    )
                if base.__module__ == "builtins":
                    # We found our base exception
                    parents.append("builtins." + base.__name__)
                    break
                else:
                    fqn = ".".join([base.__module__, base.__name__])
                    canonical_fqn = get_canonical_name(fqn, self._aliases)
                    if canonical_fqn in self._known_exceptions:
                        parents.append(canonical_fqn)
                        children = parent_to_child.setdefault(canonical_fqn, [])
                        children.append(ex_name_canonical)
                    else:
                        raise ValueError(
                            "Exported exception %s has non exported and non builtin parent "
                            "exception: %s (%s). Known exceptions: %s."
                            % (ex_name, fqn, canonical_fqn, str(self._known_exceptions))
                        )
            name_to_parent_count[ex_name_canonical] = len(parents) - 1
            name_to_parents[ex_name_canonical] = parents

        # We now form the exceptions and put them in self._known_exceptions in
        # the proper order (topologically)
        self._known_exceptions = []
        # Find roots
        to_process = []
        for name, count in name_to_parent_count.items():
            if count == 0:
                to_process.append(name)

        # Topologically process the exceptions
        while to_process:
            next_round = []
            for name in to_process:
                self._known_exceptions.append((name, name_to_parents[name]))
                del name_to_parent_count[name]
                for child in parent_to_child.get(name, []):
                    cur_child_count = name_to_parent_count[child]
                    if cur_child_count == 1:
                        next_round.append(child)
                    else:
                        name_to_parent_count[child] = cur_child_count - 1
            to_process = next_round

        if name_to_parent_count:
            raise ValueError(
                "Badly rooted exceptions: %s" % ", ".join(name_to_parent_count.keys())
            )
        self._active = False
        self._channel = None
        self._datatransferer = DataTransferer(self)

        self._handlers = {
            OP_GETATTR: self._handle_getattr,
            OP_SETATTR: self._handle_setattr,
            OP_DELATTR: self._handle_delattr,
            OP_CALL: self._handle_call,
            OP_CALLATTR: self._handle_callattr,
            OP_REPR: self._handle_repr,
            OP_STR: self._handle_str,
            OP_HASH: self._handle_hash,
            OP_PICKLE: self._handle_pickle,
            OP_DEL: self._handle_del,
            OP_GETMETHODS: self._handle_getmethods,
            OP_DIR: self._handle_dir,
            OP_CALLFUNC: self._handle_callfunc,
            OP_CALLONCLASS: self._handle_callonclass,
            OP_GETVAL: self._handle_getval,
            OP_SETVAL: self._handle_setval,
            OP_INIT: self._handle_init,
            OP_SUBCLASSCHECK: self._handle_subclasscheck,
        }

        self._local_objects = {}

    def serve(self, path=None, port=None):
        # Open up a connection
        if path is not None:
            # print("SERVER: Starting at %s" % path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            __try_op__("bind", sock.bind, BIND_RETRY, path)
        elif port is not None:
            family, socktype, proto, _, sockaddr = socket.getaddrinfo(
                "127.0.0.1", port, socket.AF_INET, socket.SOCK_STREAM
            )
            sock = socket.socket(family=family, type=socktype)
            __try_op__("bind", sock.bind, BIND_RETRY, sockaddr)

        # We assume -- and this is true for this use case -- that we will have
        # one and only one "client". The client will connect once and we assume
        # that if the connection drops, we lost the client and we can die.
        sock.listen(0)
        self._active = True
        connection, remote = sock.accept()
        self._channel = Channel(SocketByteStream(connection))
        while self._active:
            # This will block until we get a full message
            self._dispatch_request(self._channel.recv())

    def encode(self, obj):
        # This encodes an object to transfer back out
        # Basic data types will be sent over directly
        # and other ones will be saved locally and an ID will be returned
        return self._datatransferer.dump(obj)

    def encode_exception(self, ex_type, ex, trace_back):
        try:
            full_name = "%s.%s" % (ex_type.__module__, ex_type.__name__)
            get_canonical_name(full_name, self._aliases)
            serializer = self._exception_serializers.get(full_name)
        except AttributeError:
            # Ignore if no __module__ for example -- definitely not something we built
            serializer = None
        extra_content = None
        if serializer is not None:
            extra_content = serializer(ex)
        return dump_exception(
            self._datatransferer, ex_type, ex, trace_back, extra_content
        )

    def decode(self, json_obj):
        # This decodes an object that was transferred in
        return self._datatransferer.load(json_obj)

    def _dispatch_request(self, json_request):
        try:
            if json_request[FIELD_MSGTYPE] == MSG_CONTROL:
                if json_request[FIELD_OPTYPE] == CONTROL_SHUTDOWN:
                    self._active = False
                    self._reply(MSG_REPLY, self.encode(None))
                    return
                elif json_request[FIELD_OPTYPE] == CONTROL_GETEXPORTS:
                    self._reply(
                        MSG_REPLY,
                        {
                            "classes": list(self._known_classes),
                            "functions": list(self._known_funcs),
                            "values": list(self._known_vals),
                            "exceptions": self._known_exceptions,
                            "proxied": list(self._proxied_types),
                            "aliases": self._aliases,
                        },
                    )
                    return
                else:
                    raise ValueError("Unknown control message")
            if json_request[FIELD_MSGTYPE] != MSG_OP:
                raise ValueError("Invalid message received: %s" % json_request)
            op_type = json_request[FIELD_OPTYPE]
            op_target = json_request.get(FIELD_TARGET)
            if op_target is not None:
                op_target = self.decode(op_target)
            args = json_request.get(FIELD_ARGS)
            if args is not None:
                args = self.decode(args)
            kwargs = json_request.get(FIELD_KWARGS)
            if kwargs is not None:
                kwargs = dict(self.decode(kwargs))

            result = self._handlers[op_type](op_target, *args, **kwargs)
        except:  # noqa E722
            ex_type, ex, trace_back = sys.exc_info()
            if ex_type is SystemExit or ex_type is KeyboardInterrupt:
                raise
            try:
                self._reply(
                    MSG_EXCEPTION, self.encode_exception(ex_type, ex, trace_back)
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except:  # noqa E722
                internal_err = traceback.format_exc()
                self._reply(MSG_INTERNAL_ERROR, internal_err)
        else:
            try:
                self._reply(MSG_REPLY, self.encode(result))
            except (SystemExit, KeyboardInterrupt):
                raise
            except:  # noqa E722
                internal_err = traceback.format_exc()
                self._reply(MSG_INTERNAL_ERROR, internal_err)

    def _reply(self, reply_type, content):
        sys.stdout.flush()
        sys.stderr.flush()
        self._channel.send({FIELD_MSGTYPE: reply_type, FIELD_CONTENT: content})

    def can_pickle(self, obj):
        return self._class_types_to_names.get(type(obj)) is not None

    def pickle_object(self, obj):
        # This function is called to pickle obj to be transferred to the client
        # when the data layer can't transfer it by itself. We basically will save
        # locally and transfer an identifier for it
        identifier = id(obj)
        mapped_class_name = self._class_types_to_names.get(type(obj))
        if mapped_class_name is None:
            raise ValueError("Cannot proxy value of type %s" % type(obj))
        self._local_objects[identifier] = obj
        return ObjReference(VALUE_REMOTE, mapped_class_name, identifier)

    def unpickle_object(self, obj):
        if (not isinstance(obj, ObjReference)) or obj.value_type != VALUE_LOCAL:
            raise ValueError("Invalid transferred object: %s" % str(obj))
        obj = self._local_objects.get(obj.identifier)
        if obj:
            return obj
        raise ValueError("Invalid object -- id %s not known" % obj.identifier)

    @staticmethod
    def _flatten_dict(d):
        # Takes a dictionary of ("name1", "name2"): {"sub1": X, "sub2": Y} and
        # returns one of "name1.sub1": X, "name1.sub2": Y, etc. as well as a
        # dictionary of aliases {"name1": ["name2"]}...
        result = {}
        aliases = {}
        for base, values in d.items():
            if isinstance(base, tuple):
                aliases[base[0]] = list(base[1:])
                base = base[0]
            for name, value in values.items():
                result["%s.%s" % (base, name)] = value
        return result, aliases

    def _handle_getattr(self, target, name):
        override_mapping = self._getattr_overrides.get(type(target))
        if override_mapping:
            override_func = override_mapping.get(name)
            if override_func:
                return override_func(target, name)
        return getattr(target, name)

    def _handle_setattr(self, target, name, value):
        override_mapping = self._setattr_overrides.get(type(target))
        if override_mapping:
            override_func = override_mapping.get(name)
            if override_func:
                return override_func(target, name, value)
        return setattr(target, name, value)

    def _handle_delattr(self, target, name):
        delattr(target, name)

    def _handle_call(self, target, *args, **kwargs):
        return self._handle_callattr(target, "__call__", *args, **kwargs)

    def _handle_callattr(self, target, name, *args, **kwargs):
        attr = getattr(target, name)
        override_mapping = self._overrides.get(type(target))
        if override_mapping:
            override_func = override_mapping.get(name)
            if override_func:
                return override_func(target, attr, *args, **kwargs)
        return attr(*args, **kwargs)

    def _handle_repr(self, target):
        return repr(target)

    def _handle_str(self, target):
        return str(target)

    def _handle_hash(self, target):
        return hash(target)

    def _handle_pickle(self, target, proto):
        effective_protocol = min(self._max_pickle_version, proto)
        return pickle.dumps(target, protocol=effective_protocol)

    def _handle_del(self, target):
        del target

    def _handle_getmethods(self, target, class_name):
        class_type = self._known_classes.get(class_name)
        if class_type is None:
            class_type = self._proxied_types.get(class_name)
        if class_type is None:
            raise ValueError("Unknown class %s" % class_name)
        return get_methods(class_type)

    def _handle_dir(self, target):
        return dir(target)

    def _handle_callfunc(self, target, name, *args, **kwargs):
        func_to_call = self._known_funcs.get(name)
        if func_to_call is None:
            raise ValueError("Unknown function %s" % name)
        return func_to_call(*args, **kwargs)

    def _handle_callonclass(self, target, class_name, name, is_static, *args, **kwargs):
        class_type = self._known_classes.get(class_name)
        if class_type is None:
            raise ValueError("Unknown class for static/class method %s" % class_name)
        attr = getattr(class_type, name)
        override_mapping = self._overrides.get(class_type)
        if override_mapping:
            override_func = override_mapping.get(name)
            if override_func:
                if is_static:
                    return override_func(attr, *args, **kwargs)
                else:
                    return override_func(class_type, attr, *args, **kwargs)
        return attr(*args, **kwargs)

    def _handle_getval(self, target, name):
        if name in self._known_vals:
            return self._known_vals[name]
        else:
            raise ValueError("Unknown value %s" % name)

    def _handle_setval(self, target, name, value):
        if name in self._known_vals:
            self._known_vals[name] = value

    def _handle_init(self, target, class_name, *args, **kwargs):
        class_type = self._known_classes.get(class_name)
        if class_type is None:
            raise ValueError("Unknown class %s" % class_name)
        return class_type(*args, **kwargs)

    def _handle_subclasscheck(self, target, class_name, otherclass_name, reverse=False):
        class_type = self._known_classes.get(class_name)
        if class_type is None:
            raise ValueError("Unknown class %s" % class_name)
        try:
            sub_module, sub_name = otherclass_name.rsplit(".", 1)
            __import__(sub_module, None, None, "*")
        except Exception:
            sub_module = None
        if sub_module is None:
            return False
        if reverse:
            return issubclass(class_type, getattr(sys.modules[sub_module], sub_name))
        return issubclass(getattr(sys.modules[sub_module], sub_name), class_type)


if __name__ == "__main__":
    max_pickle_version = int(sys.argv[1])
    config_dir = sys.argv[2]
    socket_path = sys.argv[3]
    s = Server(config_dir, max_pickle_version)
    s.serve(path=socket_path)
