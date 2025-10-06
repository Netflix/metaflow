import json
import sys
import base64
import datetime
from collections import namedtuple

TypeResolvedObject = namedtuple("TypeResolvedObject", ["data", "is_image", "is_table"])


TIME_FORMAT = "%Y-%m-%d %I:%M:%S %p"
# Maximum artifact size to render in cards: 200MB (in bytes)
# Artifacts larger than this will be skipped during card rendering to avoid memory issues
MAX_ARTIFACT_SIZE = 256 * 1024 * 1024  # 256 MB = 268435456 bytes


def _get_object_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([_get_object_size(v, seen) for v in obj.values()])
        size += sum([_get_object_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([_get_object_size(i, seen) for i in obj])
        if hasattr(obj, "__dict__"):
            size += _get_object_size(obj.__dict__.values(), seen)
    elif hasattr(obj, "__dict__"):
        size += _get_object_size(obj.__dict__, seen)
    return size


def _full_classname(obj):
    cls = type(obj)
    module = cls.__module__
    name = cls.__qualname__
    if module is not None and module != "__builtin__" and module != "builtins":
        name = module + "." + name
    return name


class TaskToDict:
    def __init__(self, only_repr=False, runtime=False, max_artifact_size=None):
        # this dictionary holds all the supported functions
        import reprlib
        import pprint

        self._pretty_print = pprint

        r = reprlib.Repr()
        r.maxarray = 100
        r.maxstring = 100
        r.maxother = 100
        r.maxtuple = 100
        r.maxlist = 100
        r.maxlevel = 3
        self._repr = r
        self._runtime = runtime
        self._only_repr = only_repr
        # Use the global MAX_ARTIFACT_SIZE constant if not specified
        self._max_artifact_size = (
            max_artifact_size if max_artifact_size is not None else MAX_ARTIFACT_SIZE
        )
        self._supported_types = {
            "tuple": self._parse_tuple,
            "NoneType": self._parse_nonetype,
            "set": self._parse_set,
            "frozenset": self._parse_frozenset,
            "bytearray": self._parse_bytearray,
            "str": self._parse_str,
            "datetime.datetime": self._parse_datetime_datetime,
            "bool": self._parse_bool,
            "decimal.Decimal": self._parse_decimal_decimal,
            "type": self._parse_type,
            "range": self._parse_range,
            "pandas.core.frame.DataFrame": self._parse_pandas_dataframe,
            "numpy.ndarray": self._parse_numpy_ndarray,
            "dict": self._parse_dict,
            "float": self._parse_float,
            "complex": self._parse_complex,
            "int": self._parse_int,
            "Exception": self._parse_exception,
            "list": self._parse_list,
            "bytes": self._parse_bytes,
        }

    def __call__(self, task, graph=None):
        # task_props = ['stderr','stdout','created_at','finished_at','pathspec']
        # todo : dictionary Pretty printing.
        task_dict = dict(
            stderr=task.stderr,
            stdout=task.stdout,
            created_at=task.created_at.strftime(TIME_FORMAT),
            finished_at=None,
            pathspec=task.pathspec,
            graph=graph,
            data={},
        )
        if not self._runtime:
            if task.finished_at is not None:
                task_dict.update(
                    dict(finished_at=task.finished_at.strftime(TIME_FORMAT))
                )
        task_dict["data"], type_infered_objects = self._create_task_data_dict(task)
        task_dict.update(type_infered_objects)
        return task_dict

    def _create_task_data_dict(self, task):

        task_data_dict = {}
        type_inferred_objects = {"images": {}, "tables": {}}
        for data in task:
            # Check if artifact size exceeds the maximum allowed size
            if data.size > self._max_artifact_size:
                # Skip artifacts that are too large
                task_data_dict[data.id] = dict(
                    type="skipped",
                    data=f"<artifact too large: {data.size} bytes, max: {self._max_artifact_size} bytes>",
                    large_object=True,
                    supported_type=False,
                    only_repr=self._only_repr,
                    name=data.id,
                )
                continue

            try:
                data_object = data.data
                task_data_dict[data.id] = self._convert_to_native_type(data_object)
                task_data_dict[data.id]["name"] = data.id
            except ModuleNotFoundError as e:
                data_object = "<unable to unpickle>"
                # this means pickle couldn't find the module.
                task_data_dict[data.id] = dict(
                    type=e.name,
                    data=data_object,
                    large_object=False,
                    supported_type=False,
                    only_repr=self._only_repr,
                    name=data.id,
                )

            # Resolve special types.
            type_resolved_obj = self._extract_type_infered_object(data_object)
            if type_resolved_obj is not None:
                if type_resolved_obj.is_image:
                    type_inferred_objects["images"][data.id] = type_resolved_obj.data
                elif type_resolved_obj.is_table:
                    type_inferred_objects["tables"][data.id] = type_resolved_obj.data

        return task_data_dict, type_inferred_objects

    def object_type(self, object):
        return self._get_object_type(object)

    def parse_image(self, data_object):
        obj_type_name = self._get_object_type(data_object)
        if obj_type_name == "bytes":
            # Works for python 3.1+
            # Python 3.13 removes the standard ``imghdr`` module. Metaflow
            # vendors a copy so we can keep using ``what`` to detect image
            # formats irrespective of the Python version.
            import warnings

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=DeprecationWarning, module="imghdr"
                )
                from metaflow._vendor import imghdr

            resp = imghdr.what(None, h=data_object)
            # Only accept types supported on the web
            # https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
            if resp is not None and resp in ["gif", "png", "jpeg", "webp"]:
                return self._parse_image(data_object, resp)
        return None

    def _extract_type_infered_object(self, data_object):
        # check images
        obj_type_name = self._get_object_type(data_object)
        if obj_type_name == "bytes":
            # Works for python 3.1+
            from metaflow._vendor import imghdr

            resp = imghdr.what(None, h=data_object)
            # Only accept types supported on the web
            # https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
            if resp is not None and resp in ["gif", "png", "jpeg", "webp"]:
                return TypeResolvedObject(
                    self._parse_image(data_object, resp), True, False
                )
        elif obj_type_name == "pandas.core.frame.DataFrame":
            return TypeResolvedObject(
                self._parse_pandas_dataframe(data_object), False, True
            )
        return None

    def _parse_image(self, dataobject, img_type):
        return "data:image/%s;base64, %s" % (
            img_type.lower(),
            self._parse_bytes(dataobject),
        )

    @staticmethod
    def _get_object_type(obj_val):
        """returns string or None"""
        try:
            return _full_classname(obj_val)
        except AttributeError as e:
            pass

        return None

    def infer_object(self, artifact_object):
        return self._convert_to_native_type(artifact_object)

    def _convert_to_native_type(
        self,
        artifact_object,
    ):
        # For he current iteration return a dictionary.
        #
        data_dict = dict(
            type=None,
            data=None,
            large_object=False,
            supported_type=False,
            only_repr=self._only_repr,
        )
        (
            data_dict["data"],
            data_dict["type"],
            data_dict["supported_type"],
            data_dict["large_object"],
        ) = self._to_native_type(artifact_object)
        # when obj_type_dict name is none means type was not resolvable.
        return data_dict

    def _to_native_type(self, data_object):
        # returns data_obj, obj_type_name, supported_type, large_object
        rep = self._get_repr()
        supported_type = False
        large_object = False
        obj_type_name = self._get_object_type(data_object)
        if obj_type_name is None:
            return rep.repr(data_object), obj_type_name, supported_type, large_object
        elif self._only_repr:
            return (
                self._pretty_print_obj(data_object),
                obj_type_name,
                supported_type,
                large_object,
            )
        if obj_type_name in self._supported_types:
            supported_type = True
            type_parsing_func = self._supported_types[obj_type_name]
            data_obj = type_parsing_func(data_object)
            # Secondary check: if the in-memory object size exceeds our limit, use repr instead
            if _get_object_size(data_obj) > self._max_artifact_size:
                data_obj = rep.repr(data_obj)
                large_object = True
        else:
            # If object is not in supported types get its REPR
            data_obj = rep.repr(data_object)

        return data_obj, obj_type_name, supported_type, large_object

    def _pretty_print_obj(self, data_object):
        data = self._repr.repr(data_object)
        if "..." in data:
            return data
        else:
            pretty_print_op = self._pretty_print.pformat(
                data_object, indent=2, width=50, compact=True
            )
            if pretty_print_op is None:
                return data
            return pretty_print_op

    def _get_repr(self):
        return self._repr

    def _parse_tuple(self, data_object):
        return self._parse_list([obj for obj in data_object])

    def _parse_nonetype(self, data_object):
        return data_object

    def _parse_set(self, data_object):
        return self._parse_frozenset(data_object)

    def _parse_frozenset(self, data_object):
        ret_vals = []
        for obj in list(data_object):
            (
                data_obj,
                obj_type_name,
                supported_type,
                large_object,
            ) = self._to_native_type(obj)
            ret_vals.append(data_obj)
        return ret_vals

    def _parse_bytearray(self, data_object):
        try:
            return data_object.decode("utf-8")
        except UnicodeDecodeError as e:
            return self._get_repr().repr(data_object)

    def _parse_str(self, data_object):
        return data_object

    def _parse_datetime_datetime(self, data_object):
        return data_object.strftime(TIME_FORMAT)

    def _parse_bool(self, data_object):
        return data_object

    def _parse_decimal_decimal(self, data_object):
        return float(data_object)

    def _parse_type(self, data_object):
        return data_object.__name__

    def _parse_range(self, data_object):
        return self._get_repr().repr(data_object)

    @staticmethod
    def _parse_pandas_column(column_object):
        # There are two types of parsing we do here.
        # 1. We explicitly parse the types we know how to parse
        # 2. We try to partially match a type name to the column's type.
        #   - We do this because `datetime64` can match `datetime64[ns]` and `datetime64[ns, UTC]`
        #   - We do this because period can match `period[D]` and `period[2D]` etc.
        #   - There are just too many types to explicitly parse so we go by this heuristic
        # We have a default parser called `truncate_long_objects` which type casts any column to string
        # and truncates it to 30 characters.
        # If there is any form of TypeError or ValueError we set the column value to "Unsupported Type"
        # We also set columns which are have null values to "null" strings
        time_format = "%Y-%m-%dT%H:%M:%S%Z"
        truncate_long_objects = lambda x: (
            x.astype("string").str.slice(0, 30) + "..."
            if len(x) > 0 and x.astype("string").str.len().max() > 30
            else x.astype("string")
        )
        type_parser = {
            "int64": lambda x: x,
            "float64": lambda x: x,
            "bool": lambda x: x,
            "object": lambda x: truncate_long_objects(x.fillna("null")),
            "category": truncate_long_objects,
        }

        partial_type_name_match_parsers = {
            "complex": {
                "complex": lambda x: x.astype("string"),
            },
            "datetime": {
                "datetime64": lambda x: x.dt.strftime(time_format),
                "timedelta": lambda x: x.dt.total_seconds(),
            },
            "interval": {
                "interval": lambda x: x.astype("string"),
            },
            "period": {
                "period": lambda x: x.astype("string"),
            },
        }

        def _match_partial_type():
            col_type = column_object.dtype
            for _, type_parsers in partial_type_name_match_parsers.items():
                for type_name, parser in type_parsers.items():
                    if type_name in str(col_type):
                        return parser(column_object)
            return None

        try:
            col_type = str(column_object.dtype)
            if col_type in type_parser:
                return type_parser[col_type](column_object.fillna("null"))
            else:
                parsed_col = _match_partial_type()
                if parsed_col is not None:
                    return parsed_col.fillna("null")
            return truncate_long_objects(column_object.fillna("null"))
        except ValueError as e:
            return "Unsupported type: {0}".format(col_type)
        except TypeError as e:
            return "Unsupported type: {0}".format(col_type)

    def _parse_pandas_dataframe(self, data_object, truncate=True):
        headers = list(data_object.columns)
        data = data_object
        if truncate:
            data = data_object.head()
        index_column = data.index

        # We explicitly cast the `index_column` object to an `Index` or `MultiIndex` having JSON-castable values.
        if index_column.__class__.__name__ == "MultiIndex":
            from pandas import MultiIndex

            cols = [
                self._parse_pandas_column(
                    index_column.get_level_values(name).to_series()
                )
                for name in index_column.names
            ]
            index_column = MultiIndex.from_arrays(cols, names=index_column.names)
        else:
            from pandas import Index

            index_column = Index(self._parse_pandas_column(index_column.to_series()))

        for col in data.columns:
            data[col] = self._parse_pandas_column(data[col])

        data_vals = data.values.tolist()
        for row, idx in zip(data_vals, index_column.values.tolist()):
            row.insert(0, idx)
        return dict(
            full_size=(
                # full_size is a tuple of (num_rows,num_columns)
                len(data_object),
                len(headers),
            ),
            headers=[""] + headers,
            data=data_vals,
            truncated=truncate,
        )

    def _parse_numpy_ndarray(self, data_object):
        return data_object.tolist()

    def _parse_dict(self, data_object):
        data_dict = {}
        for d in data_object:
            (
                data_obj,
                obj_type_name,
                supported_type,
                large_object,
            ) = self._to_native_type(data_object[d])
            data_dict[d] = data_obj
        return data_dict

    def _parse_float(self, data_object):
        return data_object

    def _parse_complex(self, data_object):
        return str(data_object)

    def _parse_int(self, data_object):
        return data_object

    def _parse_exception(self, data_object):
        repr = self._get_repr()
        return repr.repr(data_object)

    def _parse_list(self, data_object):
        data_list = []
        for obj in data_object:
            data_obj, _, _, _ = self._to_native_type(obj)
            data_list.append(data_obj)
        return data_list

    def _parse_bytes(self, data_object):
        # encode bytes to base64 as they maybe images.
        return base64.encodebytes(data_object).decode("utf8")
