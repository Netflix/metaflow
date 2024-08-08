from collections import namedtuple
from typing import Tuple, Union
import pickle
import io
import os
from .exceptions import DataException, UnpicklableArtifactException

SerializationInfo = namedtuple(
    "SerializationInfo",
    [
        "type",
        "size",
        "encode_type",
    ],
)


class Serializer:

    TYPE = None

    ENCODING_TYPE = None

    def serialize(
        self,
        obj,
    ) -> Union[Tuple[bytes, SerializationInfo], None]:
        raise NotImplementedError

    def deserialize(
        self,
        blob,
    ):
        raise NotImplementedError


def handle_import_error(method):
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except ImportError:
            return None

    return wrapper


class TensorFlowSerializer(Serializer):

    _tensorflow = None

    TYPE = "tensorflow"

    ENCODING_TYPE = "tensorflow"

    @classmethod
    def _load_module(cls):
        if cls._tensorflow is None:
            # We add these lines because otherwise tensorflow pollutes the stderr
            # with warning logs that are not necessary for the purpose of serialization.
            os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
            import tensorflow as tf

            cls._tensorflow = tf
            del os.environ["TF_CPP_MIN_LOG_LEVEL"]
        return cls._tensorflow

    # TODO[FIXME] : Importing top level modules can be really HEAVY!.
    # See if we can have a better way to do this (i.e.) rely on some
    # heuritics (like via class names / module paths) to ensure that these
    # are only loaded when needed.
    @handle_import_error
    def should_be_serialized(self, obj):
        return isinstance(obj, self._load_module().keras.Model)

    def serialize(self, obj) -> Union[Tuple[bytes, SerializationInfo], None]:
        if not self.should_be_serialized(obj):
            return None
        tf = self._load_module()
        buffer = io.BytesIO()
        tf.keras.models.save_model(obj, buffer, save_format="h5")
        return buffer.getvalue(), SerializationInfo(
            str(type(obj)),
            buffer.getbuffer().nbytes,
            self.ENCODING_TYPE,
        )

    def deserialize(self, blob):
        tf = self._load_module()
        buffer = io.BytesIO(blob)
        return tf.keras.models.load_model(buffer)


class KerasSerializer(Serializer):

    _keras = None

    TYPE = "keras"

    ENCODING_TYPE = "keras"

    @classmethod
    def _load_module(cls):
        if cls._keras is None:
            import keras

            cls._keras = keras
        return cls._keras

    # TODO[FIXME] : Importing top level modules can be really HEAVY!.
    # See if we can have a better way to do this (i.e.) rely on some
    # heuritics (like via class names / module paths) to ensure that these
    # are only loaded when needed.
    @handle_import_error
    def should_be_serialized(self, obj):
        return isinstance(obj, self._load_module().Model)

    def serialize(self, obj) -> Union[Tuple[bytes, SerializationInfo], None]:
        if not self.should_be_serialized(obj):
            return None
        keras = self._load_module()
        buffer = io.BytesIO()
        keras.models.save_model(obj, buffer, save_format="h5")
        return buffer.getvalue(), SerializationInfo(
            str(type(obj)),
            buffer.getbuffer().nbytes,
            self.ENCODING_TYPE,
        )

    def deserialize(self, blob):
        keras = self._load_module()
        buffer = io.BytesIO(blob)
        return keras.models.load_model(buffer)


class XGBoostSerializer(Serializer):

    _xgboost = None

    TYPE = "xgboost"

    ENCODING_TYPE = "xgboost"

    @classmethod
    def _load_module(cls):
        if cls._xgboost is None:
            import xgboost as xgb

            cls._xgboost = xgb
        return cls._xgboost

    # TODO[FIXME] : Importing top level modules can be really HEAVY!.
    # See if we can have a better way to do this (i.e.) rely on some
    # heuritics (like via class names / module paths) to ensure that these
    # are only loaded when needed.
    @handle_import_error
    def should_be_serialized(self, obj):
        return isinstance(obj, self._load_module().XGBModel)

    def serialize(self, obj) -> Union[Tuple[bytes, SerializationInfo], None]:
        if not self.should_be_serialized(obj):
            return None
        xgb = self._load_module()
        buffer = io.BytesIO()
        obj.save_model(buffer)
        return buffer.getvalue(), SerializationInfo(
            str(type(obj)),
            buffer.getbuffer().nbytes,
            self.ENCODING_TYPE,
        )

    def deserialize(self, blob):
        xgb = self._load_module()
        buffer = io.BytesIO(blob)
        model = xgb.XGBModel()
        model.load_model(buffer)
        return model


class PytorchSerializer(Serializer):
    """
    ## Where this doesn't work:
    - scoped `nn.Module` objects ; i.e. `nn.Module` objects that are defined within a function scope and not at the top level of the module.

    ## todos:
    - Ensure that objects are put to CPU before we run the serialization.
      There is an off chance that torch.load may try to place them on CPUs.
    """

    _torch = None

    TYPE = "pytorch"

    ENCODING_TYPE = "pytorch"

    @classmethod
    def _load_module(cls):
        if cls._torch is None:
            import torch

            cls._torch = torch
        return cls._torch

    # TODO[FIXME] : Importing top level modules can be really HEAVY!.
    # See if we can have a better way to do this (i.e.) rely on some
    # heuritics (like via class names / module paths) to ensure that these
    # are only loaded when needed.
    @handle_import_error
    def should_be_serialized(self, obj):
        return isinstance(obj, self._load_module().nn.Module)

    def serialize(self, obj) -> Union[Tuple[bytes, SerializationInfo], None]:
        if not self.should_be_serialized(obj):
            return None
        torch = self._load_module()
        buffer = io.BytesIO()
        torch.save(obj, buffer)
        return buffer.getvalue(), SerializationInfo(
            str(type(obj)),
            buffer.getbuffer().nbytes,
            self.ENCODING_TYPE,
        )

    def deserialize(self, blob):
        torch = self._load_module()
        buffer = io.BytesIO(blob)
        return torch.load(buffer)


SERIALIZERS = [
    PytorchSerializer,
    TensorFlowSerializer,
    KerasSerializer,
    XGBoostSerializer,
]


def serialize_object(name, obj, allowed_encodings, force_v4=False):
    for s in SERIALIZERS:
        op = s().serialize(obj)
        if op is not None:
            return op
    else:
        return _pickle_dump(name, obj, allowed_encodings, force_v4=force_v4)


def deserialize_object(blob, encoding_type):
    for s in SERIALIZERS:
        if s.ENCODING_TYPE == encoding_type:
            return s().deserialize(blob)
    else:
        return _pickle_loads(blob)


def _pickle_dump(name, obj, allowed_encodings, force_v4=False):
    do_v4 = (
        force_v4 and force_v4
        if isinstance(force_v4, bool)
        else force_v4.get(name, False)
    )
    if do_v4:
        encode_type = "gzip+pickle-v4"
        if encode_type not in allowed_encodings:
            raise DataException(
                "Artifact *%s* requires a serialization encoding that "
                "requires Python 3.4 or newer." % name
            )
        try:
            blob = pickle.dumps(obj, protocol=4)
        except TypeError as e:
            raise UnpicklableArtifactException(name)
    else:
        try:
            blob = pickle.dumps(obj, protocol=2)
            encode_type = "gzip+pickle-v2"
        except (SystemError, OverflowError):
            encode_type = "gzip+pickle-v4"
            if encode_type not in allowed_encodings:
                raise DataException(
                    "Artifact *%s* is very large (over 2GB). "
                    "You need to use Python 3.4 or newer if you want to "
                    "serialize large objects." % name
                )
            try:
                blob = pickle.dumps(obj, protocol=4)
            except TypeError as e:
                raise UnpicklableArtifactException(name)
        except TypeError as e:
            raise UnpicklableArtifactException(name)

    return blob, SerializationInfo(
        str(type(obj)),
        len(blob),
        encode_type,
    )


def _pickle_loads(blob):
    return pickle.loads(blob)
