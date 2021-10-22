class Override(object):
    def __init__(self, obj_mapping, wrapped_function):
        self._obj_mapping = obj_mapping
        self._wrapped = wrapped_function

    @property
    def obj_mapping(self):
        return self._obj_mapping

    @property
    def func(self):
        return self._wrapped


class AttrOverride(Override):
    def __init__(self, is_setattr, obj_mapping, wrapped_function):
        super(AttrOverride, self).__init__(obj_mapping, wrapped_function)
        self._is_setattr = is_setattr

    @property
    def is_setattr(self):
        return self._is_setattr


class LocalOverride(Override):
    pass


class LocalAttrOverride(AttrOverride):
    pass


class RemoteOverride(Override):
    pass


class RemoteAttrOverride(AttrOverride):
    pass


def local_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@local_override takes a dictionary: <class name> -> [<overridden method>]"
        )

    def _wrapped(func):
        return LocalOverride(obj_mapping, func)

    return _wrapped


def local_getattr_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@local_getattr_override takes a dictionary: <class name> -> [<overridden attribute>]"
        )

    def _wrapped(func):
        return LocalAttrOverride(False, obj_mapping, func)

    return _wrapped


def local_setattr_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@local_setattr_override takes a dictionary: <class name> -> [<overridden attribute>]"
        )

    def _wrapped(func):
        return LocalAttrOverride(True, obj_mapping, func)

    return _wrapped


def remote_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@remote_override takes a dictionary: <class name> -> [<overridden method>]"
        )

    def _wrapped(func):
        return RemoteOverride(obj_mapping, func)

    return _wrapped


def remote_getattr_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@remote_getattr_override takes a dictionary: <class name> -> [<overridden attribute>]"
        )

    def _wrapped(func):
        return RemoteAttrOverride(False, obj_mapping, func)

    return _wrapped


def remote_setattr_override(obj_mapping):
    if not isinstance(obj_mapping, dict):
        raise ValueError(
            "@remote_setattr_override takes a dictionary: <class name> -> [<overridden attribute>]"
        )

    def _wrapped(func):
        return RemoteAttrOverride(True, obj_mapping, func)

    return _wrapped


class LocalException(object):
    def __init__(self, class_path, wrapped_class):
        self._class_path = class_path
        self._class = wrapped_class

    @property
    def class_path(self):
        return self._class_path

    @property
    def wrapped_class(self):
        return self._class


class RemoteExceptionSerializer(object):
    def __init__(self, class_path, serializer):
        self._class_path = class_path
        self._serializer = serializer

    @property
    def class_path(self):
        return self._class_path

    @property
    def serializer(self):
        return self._serializer


def local_exception(class_path):
    def _wrapped(cls):
        return LocalException(class_path, cls)

    return _wrapped


def remote_exception_serialize(class_path):
    def _wrapped(func):
        return RemoteExceptionSerializer(class_path, func)

    return _wrapped
