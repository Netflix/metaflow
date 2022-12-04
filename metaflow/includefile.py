from collections import namedtuple
import gzip

import io
import json
import os

from hashlib import sha1

from metaflow._vendor import click

from .exception import MetaflowException
from .parameters import (
    DelayedEvaluationParameter,
    DeployTimeField,
    Parameter,
    ParameterContext,
)
from .util import get_username

import functools

# _tracefunc_depth = 0


# def tracefunc(func):
#     """Decorates a function to show its trace."""

#     @functools.wraps(func)
#     def tracefunc_closure(*args, **kwargs):
#         global _tracefunc_depth
#         """The closure."""
#         print(f"{_tracefunc_depth}: {func.__name__}(args={args}, kwargs={kwargs})")
#         _tracefunc_depth += 1
#         result = func(*args, **kwargs)
#         _tracefunc_depth -= 1
#         print(f"{_tracefunc_depth} => {result}")
#         return result

#     return tracefunc_closure


_DelayedExecContext = namedtuple(
    "_DelayedExecContext", "flow_name path is_text encoding handler_type echo"
)


# From here on out, this is the IncludeFile implementation.
from metaflow.plugins.datatools import Local, S3
from metaflow.plugins.azure.includefile_support import Azure
from metaflow.plugins.gcp.includefile_support import GS

DATACLIENTS = {
    "local": Local,
    "s3": S3,
    "azure": Azure,
    "gs": GS,
}


class IncludedFile(object):
    # Thin wrapper to indicate to the MF client that this object is special
    # and should be handled as an IncludedFile when returning it (ie: fetching
    # the actual content)

    # @tracefunc
    def __init__(self, descriptor):
        self._descriptor = descriptor
        self._cached_size = None

    @property
    def descriptor(self):
        return self._descriptor

    @property
    # @tracefunc
    def size(self):
        if self._cached_size is not None:
            return self._cached_size
        handler = UPLOADERS.get(self.descriptor.get("type", None), None)
        if handler is None:
            raise MetaflowException(
                "Could not interpret size of IncludedFile: %s"
                % json.dumps(self.descriptor)
            )
        self._cached_size = handler.size(self._descriptor)
        return self._cached_size

    # @tracefunc
    def decode(self, name, var_type="Artifact"):
        # We look for the uploader for it and decode it
        handler = UPLOADERS.get(self.descriptor.get("type", None), None)
        if handler is None:
            raise MetaflowException(
                "%s '%s' could not be loaded (IncludedFile) because no handler found: %s"
                % (var_type, name, json.dumps(self.descriptor))
            )
        return handler.load(self._descriptor)


class FilePathClass(click.ParamType):
    name = "FilePath"

    def __init__(self, is_text, encoding):
        self._is_text = is_text
        self._encoding = encoding

    def convert(self, value, param, ctx):
        # Click can call convert multiple times, so we need to make sure to only
        # convert once. This function will return a DelayedEvaluationParameter
        # (if it needs to still perform an upload) or an IncludedFile if not
        if isinstance(value, (DelayedEvaluationParameter, IncludedFile)):
            return value

        # Value will be a string containing one of two things:
        #  - Scenario A: a JSON blob indicating that the file has already been uploaded.
        #    This scenario this happens in is as follows:
        #      + `step-functions create` is called and the IncludeFile has a default
        #        value. At the time of creation, the file is uploaded and a URL is
        #        returned; this URL is packaged in a blob by Uploader and passed to
        #        step-functions as the value of the parameter.
        #      + when the step function actually runs, the value is passed to click
        #        through METAFLOW_INIT_XXX; this value is the one returned above
        #  - Scenario B: A path. The path can either be:
        #      + B.1: <prefix>://<something> like s3://foo/bar or local:///foo/bar
        #        (right now, we are disabling support for this because the artifact
        #        can change unlike all other artifacts. It is trivial to re-enable
        #      + B.2: an actual path to a local file like /foo/bar
        #    In the first case, we just store an *external* reference to it (so we
        #    won't upload anything). In the second case we will want to upload something,
        #    but we only do that in the DelayedEvaluationParameter step.

        # ctx can be one of two things:
        #  - the click context (when called normally)
        #  - the ParameterContext (when called through _eval_default)
        # If not a ParameterContext, we convert it to that
        if not isinstance(ctx, ParameterContext):
            ctx = ParameterContext(
                flow_name=ctx.obj.flow.name,
                user_name=get_username(),
                parameter_name=param.name,
                logger=ctx.obj.echo,
                ds_type=ctx.obj.datastore_impl.TYPE,
            )

        if len(value) > 0 and value[0] == "{":
            # This is a blob; no URL starts with `{`. We are thus in scenario A
            try:
                value = json.loads(value)
            except json.JSONDecodeError as e:
                raise MetaflowException(
                    "IncludeFile '%s' (value: %s) is malformed" % (param.name, value)
                )
            # All processing has already been done, so we just convert to an `IncludedFile`
            return IncludedFile(value)

        path = os.path.expanduser(value)

        prefix_pos = path.find("://")
        if prefix_pos > 0:
            # Scenario B.1
            raise MetaflowException(
                "IncludeFile using a direct reference to a file in cloud storage is no "
                "longer supported. Contact the Metaflow team if you need this supported"
            )
            # if DATACLIENTS.get(path[:prefix_pos]) is None:
            #     self.fail(
            #         "IncludeFile: no handler for external file of type '%s' "
            #         "(given path is '%s')" % (path[:prefix_pos], path)
            #     )
            # # We don't need to do anything more -- the file is already uploaded so we
            # # just return a blob indicating how to get the file.
            # return IncludedFile(
            #     CURRENT_UPLOADER.encode_url(
            #         "external", path, is_text=self._is_text, encoding=self._encoding
            #     )
            # )
        else:
            # Scenario B.2
            # Check if this is a valid local file
            try:
                with open(path, mode="r") as _:
                    pass
            except OSError:
                self.fail("IncludeFile: could not open file '%s' for reading" % path)
            handler = DATACLIENTS.get(ctx.ds_type)
            if handler is None:
                self.fail(
                    "IncludeFile: no data-client for datastore of type '%s'"
                    % ctx.ds_type
                )

            # Now that we have done preliminary checks, we will delay uploading it
            # until later (so it happens after PyLint checks the flow, but we prepare
            # everything for it)
            lambda_ctx = _DelayedExecContext(
                flow_name=ctx.flow_name,
                path=path,
                is_text=self._is_text,
                encoding=self._encoding,
                handler_type=ctx.ds_type,
                echo=ctx.logger,
            )

            def _delayed_eval_func(ctx=lambda_ctx, return_str=False):
                incl_file = IncludedFile(
                    CURRENT_UPLOADER.store(
                        ctx.flow_name,
                        ctx.path,
                        ctx.is_text,
                        ctx.encoding,
                        DATACLIENTS[ctx.handler_type],
                        ctx.echo,
                    )
                )
                if return_str:
                    return json.dumps(incl_file.descriptor)
                return incl_file

            return DelayedEvaluationParameter(
                ctx.parameter_name,
                "default",
                functools.partial(_delayed_eval_func, ctx=lambda_ctx),
            )

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "FilePath"


class IncludeFile(Parameter):
    """
    Includes a local file as a parameter for the flow.

    `IncludeFile` behaves like `Parameter` except that it reads its value from a file instead of
    the command line. The user provides a path to a file on the command line. The file contents
    are saved as a read-only artifact which is available in all steps of the flow.

    Parameters
    ----------
    name : str
        User-visible parameter name.
    default : str or a function
        Default path to a local file. A function
        implies that the parameter corresponds to a *deploy-time parameter*.
    is_text : bool
        Convert the file contents to a string using the provided `encoding` (default: True).
        If False, the artifact is stored in `bytes`.
    encoding : str
        Use this encoding to decode the file contexts if `is_text=True` (default: `utf-8`).
    required : bool
        Require that the user specified a value for the parameter.
        `required=True` implies that the `default` is not used.
    help : str
        Help text to show in `run --help`.
    show_default : bool
        If True, show the default value in the help text (default: True).
    """

    def __init__(
        self, name, required=False, is_text=True, encoding=None, help=None, **kwargs
    ):
        # If a default is specified, it needs to be uploaded when the flow is deployed
        # (for example when doing a `step-functions create`) so we make the default
        # be a DeployTimeField. This means that it will be evaluated in two cases:
        #  - by deploy_time_eval for `step-functions create` and related.
        #  - by Click when evaluating the parameter.
        #
        # In the first case, we will need to fully upload the file whereas in the
        # second case, we can just return the string as the FilePath.convert method
        # will take care of evaluating things.
        v = kwargs.get("default")
        if v is not None:
            # If the default is a callable, we have two DeployTimeField:
            #  - the callable nature of the default will require us to "call" the default
            #    (so that is the outer DeployTimeField)
            #  - IncludeFile defaults are always DeployTimeFields (since they need to be
            #    uploaded)
            #
            # Therefore, if the default value is itself a callable, we will have
            # a DeployTimeField (upload the file) wrapping another DeployTimeField
            # (call the default)
            if callable(v) and not isinstance(v, DeployTimeField):
                # If default is a callable, make it a DeployTimeField (the inner one)
                v = DeployTimeField(name, str, "default", v, return_str=True)
            kwargs["default"] = DeployTimeField(
                name,
                str,
                "default",
                IncludeFile._eval_default(is_text, encoding, v),
                print_representation=v,
            )

        super(IncludeFile, self).__init__(
            name,
            required=required,
            help=help,
            type=FilePathClass(is_text, encoding),
            **kwargs,
        )

    def load_parameter(self, v):
        if v is None:
            return v
        return v.decode(self.name, var_type="Parameter")

    @staticmethod
    def _eval_default(is_text, encoding, default_path):
        # NOTE: If changing name of this function, check comments that refer to it to
        # update it.
        def do_eval(ctx, deploy_time):
            if isinstance(default_path, DeployTimeField):
                d = default_path(deploy_time=deploy_time)
            else:
                d = default_path
            if deploy_time:
                fp = FilePathClass(is_text, encoding)
                val = fp.convert(d, None, ctx)
                if isinstance(val, DelayedEvaluationParameter):
                    val = val()
                # At this point this is an IncludedFile, but we need to make it
                # into a string so that it can be properly saved.
                return json.dumps(val.descriptor)
            else:
                return d

        return do_eval


class UploaderV1:
    file_type = "uploader-v1"

    @classmethod
    def encode_url(cls, url_type, url, **kwargs):
        return_value = {"type": url_type, "url": url}
        return_value.update(kwargs)
        return return_value

    @classmethod
    def store(cls, flow_name, path, is_text, encoding, handler, echo):
        sz = os.path.getsize(path)
        unit = ["B", "KB", "MB", "GB", "TB"]
        pos = 0
        while pos < len(unit) and sz >= 1024:
            sz = sz // 1024
            pos += 1
        if pos >= 3:
            extra = "(this may take a while)"
        else:
            extra = ""
        echo("Including file %s of size %d%s %s" % (path, sz, unit[pos], extra))
        try:
            input_file = io.open(path, mode="rb").read()
        except IOError:
            # If we get an error here, since we know that the file exists already,
            # it means that read failed which happens with Python 2.7 for large files
            raise MetaflowException(
                "Cannot read file at %s -- this is likely because it is too "
                "large to be properly handled by Python 2.7" % path
            )
        sha = sha1(input_file).hexdigest()
        path = os.path.join(handler.get_root_from_config(echo, True), flow_name, sha)
        buf = io.BytesIO()

        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=3) as f:
            f.write(input_file)
        buf.seek(0)

        with handler() as client:
            url = client.put(path, buf.getvalue(), overwrite=False)

        return cls.encode_url(cls.file_type, url, is_text=is_text, encoding=encoding)

    @classmethod
    def size(cls, descriptor):
        # We never have the size so we look it up
        url = descriptor["url"]
        handler = cls._get_handler(url)
        with handler() as client:
            obj = client.info(url, return_missing=True)
            if obj.exists:
                return obj.size
        raise FileNotFoundError("File at '%s' does not exist" % url)

    @classmethod
    def load(cls, descriptor):
        url = descriptor["url"]
        handler = cls._get_handler(url)
        with handler() as client:
            obj = client.get(url, return_missing=True)
            if obj.exists:
                if descriptor["type"] == cls.file_type:
                    # We saved this file directly, so we know how to read it out
                    with gzip.GzipFile(filename=obj.path, mode="rb") as f:
                        if descriptor["is_text"]:
                            return io.TextIOWrapper(
                                f, encoding=descriptor.get("encoding")
                            ).read()
                        return f.read()
                else:
                    # We open this file according to the is_text and encoding information
                    if descriptor["is_text"]:
                        return io.open(
                            obj.path, mode="rt", encoding=descriptor.get("encoding")
                        ).read()
                    else:
                        return io.open(obj.path, mode="rb").read()
            raise FileNotFoundError("File at '%s' does not exist" % descriptor["url"])

    @staticmethod
    def _get_handler(url):
        prefix_pos = url.find("://")
        if prefix_pos < 0:
            raise MetaflowException("Malformed URL: '%s'" % url)
        prefix = url[:prefix_pos]
        handler = DATACLIENTS.get(prefix)
        if handler is None:
            raise MetaflowException("Could not find data client for '%s'" % prefix)
        return handler


class UploaderV2:

    file_type = "uploader-v2"

    @classmethod
    def encode_url(cls, url_type, url, **kwargs):
        return_value = {
            "note": "Internal representation of IncludeFile(%s)" % url,
            "type": cls.file_type,
            "sub-type": url_type,
            "url": url,
        }
        return_value.update(kwargs)
        return return_value

    @classmethod
    def store(cls, flow_name, path, is_text, encoding, handler, echo):
        r = UploaderV1.store(flow_name, path, is_text, encoding, handler, echo)

        # In V2, we store size for faster access
        r["note"] = "Internal representation of IncludeFile(%s)" % path
        r["type"] = cls.file_type
        r["sub-type"] = "uploaded"
        r["size"] = os.stat(path).st_size
        return r

    @classmethod
    def size(cls, descriptor):
        if descriptor["sub-type"] == "uploaded":
            return descriptor["size"]
        else:
            # This was a file that was external, so we get information on it
            url = descriptor["url"]
            handler = cls._get_handler(url)
            with handler() as client:
                obj = client.info(url, return_missing=True)
                if obj.exists:
                    return obj.size
            raise FileNotFoundError(
                "%s file at '%s' does not exist"
                % (descriptor["sub-type"].capitalize(), url)
            )

    @classmethod
    def load(cls, descriptor):
        url = descriptor["url"]
        # We know the URL is in a <prefix>:// format so we just extract the handler
        handler = cls._get_handler(url)
        with handler() as client:
            obj = client.get(url, return_missing=True)
            if obj.exists:
                if descriptor["sub-type"] == "uploaded":
                    # We saved this file directly, so we know how to read it out
                    with gzip.GzipFile(filename=obj.path, mode="rb") as f:
                        if descriptor["is_text"]:
                            return io.TextIOWrapper(
                                f, encoding=descriptor.get("encoding")
                            ).read()
                        return f.read()
                else:
                    # We open this file according to the is_text and encoding information
                    if descriptor["is_text"]:
                        return io.open(
                            obj.path, mode="rt", encoding=descriptor.get("encoding")
                        ).read()
                    else:
                        return io.open(obj.path, mode="rb").read()
            # If we are here, the file does not exist
            raise FileNotFoundError(
                "%s file at '%s' does not exist"
                % (descriptor["sub-type"].capitalize(), url)
            )

    @staticmethod
    def _get_handler(url):
        return UploaderV1._get_handler(url)


UPLOADERS = {
    "uploader-v1": UploaderV1,
    "external": UploaderV1,
    "uploader-v2": UploaderV2,
}
CURRENT_UPLOADER = UploaderV2
