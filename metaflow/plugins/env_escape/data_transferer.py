import base64
import functools
import pickle
import sys

from collections import OrderedDict, defaultdict, namedtuple
from copy import copy
from datetime import datetime, timedelta

ObjReference = namedtuple("ObjReference", "value_type class_name identifier")

# This file encodes/decodes values to send over the wire

if sys.version_info[0] >= 3:

    class InvalidLong:
        pass

    class InvalidUnicode:
        pass


# Types that we can marshall/unmarshall
_types = [
    type(None),
    bool,
    int,
    float,
    complex,
    str,
    list,
    tuple,
    bytearray,
    bytes,
    set,
    frozenset,
    dict,
    defaultdict,
    OrderedDict,
    datetime,
    timedelta,
]

_container_types = (list, tuple, set, frozenset, dict, defaultdict, OrderedDict)

if sys.version_info[0] >= 3:
    _types.extend([InvalidLong, InvalidUnicode])
    _simple_types = (bool, int, float, complex, bytearray, bytes, datetime, timedelta)
else:
    _types.extend([long, unicode])  # noqa F821
    _simple_types = (
        bool,
        int,
        float,
        complex,
        bytearray,
        bytes,
        unicode,  # noqa F821
        long,  # noqa F821
        datetime,
        timedelta,
    )

_types_to_encoding = {x: idx for idx, x in enumerate(_types)}

_dumpers = {}  # Key: type -> Value: function to call to dump
_loaders = {}  # Key: int -> Value: function to call to load that type

FIELD_TYPE = "t"
FIELD_ANNOTATION = "a"  # Additional information that loader/dumper can use
FIELD_INLINE_VALUE = "v"
FIELD_INLINE_KEY = "k"

# Protocol to use
defaultProtocol = pickle.HIGHEST_PROTOCOL


def _register_dumper(what):
    def wrapper(func):
        for w in what:
            _dumpers[w] = functools.partial(func, w)
        return func

    return wrapper


def _register_loader(what):
    def wrapper(func):
        for w in what:
            _loaders[_types_to_encoding[w]] = functools.partial(func, w)
        return func

    return wrapper


@_register_dumper((type(None),))
def _dump_none(obj_type, transferer, obj):
    return (None, False)  # Does not matter what we return


@_register_loader((type(None),))
def _load_none(obj_type, transferer, json_annotation, json_obj):
    return None


@_register_dumper(_simple_types)
def _dump_simple(obj_type, transferer, obj):
    return (
        None,
        base64.b64encode(pickle.dumps(obj, protocol=defaultProtocol)).decode("utf-8"),
    )


@_register_loader(_simple_types)
def _load_simple(obj_type, transferer, json_annotation, json_obj):
    new_obj = pickle.loads(base64.b64decode(json_obj), encoding="utf-8")
    if not isinstance(new_obj, obj_type):
        raise RuntimeError("Pickle didn't create an object of the proper type")
    return new_obj


@_register_dumper(_container_types)
def _dump_container(obj_type, transferer, obj):
    try:
        new_obj = transferer.pickle_container(obj)
    except RuntimeError as e:
        raise RuntimeError("Cannot dump container %s: %s" % (str(obj), e))
    if new_obj is None:
        return _dump_simple(obj_type, transferer, obj)
    else:
        _, dump = _dump_simple(obj_type, transferer, new_obj)
        return True, dump


@_register_loader(_container_types)
def _load_container(obj_type, transferer, json_annotation, json_obj):
    obj = _load_simple(obj_type, transferer, json_annotation, json_obj)
    if json_annotation:
        # This means we had pickled references
        obj = transferer.unpickle_container(obj)
    return obj


if sys.version_info[0] >= 3:

    @_register_dumper((str,))
    def _dump_str(obj_type, transferer, obj):
        return _dump_simple(obj_type, transferer, obj)

    @_register_loader((str,))
    def _load_str(obj_type, transferer, json_annotation, json_obj):
        return _load_simple(obj_type, transferer, json_annotation, json_obj)

    @_register_dumper((InvalidLong,))
    def _dump_invalidlong(obj_type, transferer, obj):
        return _dump_simple(int, transferer, obj)

    @_register_loader((InvalidLong,))
    def _load_invalidlong(obj_type, transferer, json_annotation, json_obj):
        return _load_simple(int, transferer, json_annotation, json_obj)

    @_register_dumper((InvalidUnicode,))
    def _dump_invalidunicode(obj_type, transferer, obj):
        return _dump_simple(str, transferer, obj)

    @_register_loader((InvalidUnicode,))
    def _load_invalidunicode(obj_type, transferer, json_annotation, json_obj):
        return _load_simple(str, transferer, json_annotation, json_obj)

else:

    @_register_dumper((str,))
    def _dump_str(obj_type, transferer, obj):
        return _dump_simple(obj_type, transferer, obj.encode("utf-8"))

    @_register_loader((str,))
    def _load_str(obj_type, transferer, json_annotation, json_obj):
        # The object is actually bytes
        return (_load_simple(bytes, json_annotation, json_obj)).decode("utf-8")

    @_register_dumper((unicode, long))  # noqa F821
    def _dump_py2_simple(obj_type, transferer, obj):
        return _dump_simple(obj_type, transferer, obj)

    @_register_loader((unicode, long))  # noqa F821
    def _load_py2_simple(obj_type, transferer, json_annotation, json_obj):
        return _load_simple(obj_type, transferer, json_annotation, json_obj)


class DataTransferer(object):
    def __init__(self, connection):
        self._dumpers = _dumpers.copy()
        self._loaders = _loaders.copy()
        self._types_to_encoding = _types_to_encoding.copy()

        self._connection = connection

    @staticmethod
    def can_simple_dump(obj):
        return DataTransferer._can_dump(DataTransferer.can_simple_dump, obj)

    def can_dump(self, obj):
        r = DataTransferer._can_dump(self.can_dump, obj)
        if not r:
            return self._connection.can_encode(obj)
        return False

    def dump(self, obj):
        obj_type = type(obj)
        handler = self._dumpers.get(type(obj))
        if handler:
            attr, v = handler(self, obj)
            return {
                FIELD_TYPE: self._types_to_encoding[obj_type],
                FIELD_ANNOTATION: attr,
                FIELD_INLINE_VALUE: v,
            }
        else:
            # We will see if the connection can encode and transfer this object
            # This is primarily used to transfer a reference to an object
            try:
                json_obj = base64.b64encode(
                    pickle.dumps(
                        self._connection.pickle_object(obj), protocol=defaultProtocol
                    )
                ).decode("utf-8")
            except ValueError as e:
                raise RuntimeError("Unable to dump non base type: %s" % e)
            return {FIELD_TYPE: -1, FIELD_INLINE_VALUE: json_obj}

    def load(self, json_obj):
        obj_type = json_obj.get(FIELD_TYPE)
        if obj_type is None:
            raise RuntimeError(
                "Malformed message -- missing %s: %s" % (FIELD_TYPE, str(json_obj))
            )
        if obj_type == -1:
            # This is something that the connection handles
            try:
                return self._connection.unpickle_object(
                    pickle.loads(
                        base64.b64decode(json_obj[FIELD_INLINE_VALUE]), encoding="utf-8"
                    )
                )
            except ValueError as e:
                raise RuntimeError("Unable to load non base type: %s" % e)
        handler = self._loaders.get(obj_type)
        if handler:
            json_subobj = json_obj.get(FIELD_INLINE_VALUE)
            if json_subobj is not None:
                return handler(
                    self, json_obj.get(FIELD_ANNOTATION), json_obj[FIELD_INLINE_VALUE]
                )
            raise RuntimeError("Non inline value not supported")
        raise RuntimeError("Unable to find handler for type %s" % obj_type)

    # _container_types = (list, tuple, set, frozenset, dict, OrderedDict)
    def _transform_container(self, checker, processor, recursor, obj, in_place=True):
        def _sub_process(obj):
            obj_type = type(obj)
            if obj is None or obj_type in _simple_types or obj_type == str:
                return None
            elif obj_type in _container_types:
                return recursor(obj)
            elif checker(obj):
                return processor(obj)
            else:
                raise RuntimeError(
                    "Cannot pickle object of type %s: %s" % (obj_type, str(obj))
                )

        cast_to = None
        key_change_allowed = True
        update_default_factory = False
        has_changes = False
        if isinstance(obj, (tuple, set, frozenset)):
            cast_to = type(obj)
            obj = list(obj)
            in_place = True  # We can do in place since we copied the object
        if isinstance(obj, OrderedDict):
            key_change_allowed = False
        if isinstance(obj, defaultdict):
            # In this case, we use a hack to store the default_factory
            # function inside the dictionary which will get it pickled by
            # the server (as a reference). This is because if this is a lambda
            # it won't pickle well.
            if callable(obj.default_factory):
                if not in_place:
                    obj = copy(obj)
                    in_place = True
                obj["__default_factory"] = obj.default_factory
                obj.default_factory = None
            elif obj.get("__default_factory") is not None:
                # This is in the unpickle path, we need to reset the factory properly
                update_default_factory = True
            has_changes = True
        # We now deal with list or dict
        if isinstance(obj, list):
            for idx in range(len(obj)):
                sub_obj = _sub_process(obj[idx])
                if sub_obj is not None:
                    has_changes = True
                    if not in_place:
                        obj = list(obj)
                        in_place = True
                    obj[idx] = sub_obj
        elif isinstance(obj, dict):
            new_items = {}
            del_keys = []
            for k, v in obj.items():
                sub_key = _sub_process(k)
                if sub_key is not None:
                    if not key_change_allowed:
                        raise RuntimeError(
                            "OrderedDict key cannot contain references -- "
                            "this would change the order"
                        )
                    has_changes = True
                sub_val = _sub_process(v)
                if sub_val is not None:
                    has_changes = True
                if has_changes and not in_place:
                    obj = copy(obj)
                    in_place = True
                if sub_key:
                    if sub_val:
                        new_items[sub_key] = sub_val
                    else:
                        new_items[sub_key] = v
                    del_keys.append(k)
                else:
                    if sub_val:
                        obj[k] = sub_val
            for k in del_keys:
                del obj[k]
            obj.update(new_items)
        else:
            raise RuntimeError("Unknown container type: %s" % type(obj))
        if update_default_factory:
            # We do this here because we now unpickled the reference
            # to default_dict and can set it back up again.
            obj.default_factory = obj["__default_factory"]
            del obj["__default_factory"]
        if has_changes:
            if cast_to:
                return cast_to(obj)
            return obj
        return None

    def pickle_container(self, obj):
        return self._transform_container(
            self._connection.can_pickle,
            self._connection.pickle_object,
            self.pickle_container,
            obj,
            in_place=False,
        )

    def unpickle_container(self, obj):
        return self._transform_container(
            lambda x: isinstance(x, ObjReference),
            self._connection.unpickle_object,
            self.unpickle_container,
            obj,
        )

    @staticmethod
    def _can_dump(recursive_func, obj):
        obj_type = type(obj)
        if obj is None:
            return True
        if obj_type in _simple_types:
            return True
        if obj_type == str:
            return True
        if obj_type == dict or obj_type == OrderedDict:
            return all(
                (recursive_func(k) and recursive_func(v) for k, v in obj.items())
            )
        if obj_type in _container_types:
            return all((recursive_func(x) for x in obj))
        return False
