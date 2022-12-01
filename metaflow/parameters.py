import json
from collections import namedtuple

from metaflow._vendor import click

from .util import get_username, is_stringish
from .exception import (
    ParameterFieldFailed,
    ParameterFieldTypeMismatch,
    MetaflowException,
)

try:
    # Python2
    strtype = basestring
except NameError:
    # Python3
    strtype = str

# ParameterContext allows deploy-time functions modify their
# behavior based on the context. We can add fields here without
# breaking backwards compatibility but don't remove any fields!
ParameterContext = namedtuple(
    "ParameterContext",
    [
        "flow_name",
        "user_name",
        "parameter_name",
        "logger",
        "ds_type",
    ],
)

# currently we execute only one flow per process, so we can treat
# Parameters globally. If this was to change, it should/might be
# possible to move these globals in a FlowSpec (instance) specific
# closure.
parameters = []
context_proto = None


class JSONTypeClass(click.ParamType):
    name = "JSON"

    def convert(self, value, param, ctx):
        if not isinstance(value, strtype):
            # Already a correct type
            return value
        try:
            return json.loads(value)
        except:
            self.fail("%s is not a valid JSON object" % value, param, ctx)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "JSON"


class DeployTimeField(object):
    """
    This a wrapper object for a user-defined function that is called
    at deploy time to populate fields in a Parameter. The wrapper
    is needed to make Click show the actual value returned by the
    function instead of a function pointer in its help text. Also, this
    object curries the context argument for the function, and pretty
    prints any exceptions that occur during evaluation.
    """

    def __init__(
        self,
        parameter_name,
        parameter_type,
        field,
        fun,
        return_str=True,
        print_representation=None,
    ):

        self.fun = fun
        self.field = field
        self.parameter_name = parameter_name
        self.parameter_type = parameter_type
        self.return_str = return_str
        self.print_representation = (
            self.user_print_representation
        ) = print_representation
        if self.print_representation is None:
            self.print_representation = str(self.fun)

    def __call__(self, deploy_time=False):
        # This is called in two ways:
        #  - through the normal Click default parameter evaluation: if a default
        #    value is a callable, Click will call it without any argument. In other
        #    words, deploy_time=False. This happens for a normal "run" or the "trigger"
        #    functions for step-functions for example. Anything that has the
        #    @add_custom_parameters decorator will trigger this. Once click calls this,
        #    it will then pass the resulting value to the convert() functions for the
        #    type for that Parameter.
        #  - by deploy_time_eval which is invoked to process the parameters at
        #    deploy_time and outside of click processing (ie: at that point, Click
        #    is not involved since anytime deploy_time_eval is called, no custom parameters
        #    have been added). In that situation, deploy_time will be True. Note that in
        #    this scenario, the value should be something that can be converted to JSON.
        # The deploy_time value can therefore be used to determine which type of
        # processing is requested.
        ctx = context_proto._replace(parameter_name=self.parameter_name)
        try:
            try:
                # Most user-level functions may not care about the deploy_time parameter
                # but IncludeFile does.
                val = self.fun(ctx, deploy_time)
            except TypeError:
                val = self.fun(ctx)
        except:
            raise ParameterFieldFailed(self.parameter_name, self.field)
        else:
            return self._check_type(val, deploy_time)

    def _check_type(self, val, deploy_time):
        # it is easy to introduce a deploy-time function that accidentally
        # returns a value whose type is not compatible with what is defined
        # in Parameter. Let's catch those mistakes early here, instead of
        # showing a cryptic stack trace later.

        # note: this doesn't work with long in Python2 or types defined as
        # click types, e.g. click.INT
        TYPES = {bool: "bool", int: "int", float: "float", list: "list"}

        msg = (
            "The value returned by the deploy-time function for "
            "the parameter *%s* field *%s* has a wrong type. "
            % (self.parameter_name, self.field)
        )

        if self.parameter_type in TYPES:
            if type(val) != self.parameter_type:
                msg += "Expected a %s." % TYPES[self.parameter_type]
                raise ParameterFieldTypeMismatch(msg)
            return str(val) if self.return_str else val
        else:
            if deploy_time:
                try:
                    if not is_stringish(val):
                        val = json.dumps(val)
                except TypeError:
                    msg += "Expected a JSON-encodable object or a string."
                    raise ParameterFieldTypeMismatch(msg)
                return val
            # If not deploy_time, we expect a string
            if not is_stringish(val):
                msg += "Expected a string."
                raise ParameterFieldTypeMismatch(msg)
            return val

    @property
    def description(self):
        return self.print_representation

    def __str__(self):
        if self.user_print_representation:
            return self.user_print_representation
        return self()

    def __repr__(self):
        if self.user_print_representation:
            return self.user_print_representation
        return self()


def deploy_time_eval(value):
    if isinstance(value, DeployTimeField):
        return value(deploy_time=True)
    else:
        return value


# this is called by cli.main
def set_parameter_context(flow_name, echo, datastore):
    global context_proto
    context_proto = ParameterContext(
        flow_name=flow_name,
        user_name=get_username(),
        parameter_name=None,
        logger=echo,
        ds_type=datastore.TYPE,
    )


class DelayedEvaluationParameter(object):
    """
    This is a very simple wrapper to allow parameter "conversion" to be delayed until
    the `_set_constants` function in FlowSpec. Typically, parameters are converted
    by click when the command line option is processed. For some parameters, like
    IncludeFile, this is too early as it would mean we would trigger the upload
    of the file too early. If a parameter converts to a DelayedEvaluationParameter
    object through the usual click mechanisms, `_set_constants` knows to invoke the
    __call__ method on that DelayedEvaluationParameter; in that case, the __call__
    method is invoked without any parameter. The return_str parameter will be used
    by schedulers when they need to convert DelayedEvaluationParameters to a
    string to store them
    """

    def __init__(self, name, field, fun):
        self._name = name
        self._field = field
        self._fun = fun

    def __call__(self, return_str=False):
        try:
            return self._fun(return_str=return_str)
        except Exception as e:
            raise ParameterFieldFailed(self._name, self._field)


class Parameter(object):
    """
    Defines a parameter for a flow.

    Parameters must be instantiated as class variables in flow classes, e.g.
    ```
    class MyFlow(FlowSpec):
        param = Parameter('myparam')
    ```
    in this case, the parameter is specified on the command line as
    ```
    python myflow.py run --myparam=5
    ```
    and its value is accessible through a read-only artifact like this:
    ```
    print(self.param == 5)
    ```
    Note that the user-visible parameter name, `myparam` above, can be
    different from the artifact name, `param` above.

    The parameter value is converted to a Python type based on the `type`
    argument or to match the type of `default`, if it is set.

    Parameters
    ----------
    name : str
        User-visible parameter name.
    default : str or float or int or bool or `JSONType` or a function.
        Default value for the parameter. Use a special `JSONType` class to
        indicate that the value must be a valid JSON object. A function
        implies that the parameter corresponds to a *deploy-time parameter*.
        The type of the default value is used as the parameter `type`.
    type : type
        If `default` is not specified, define the parameter type. Specify
        one of `str`, `float`, `int`, `bool`, or `JSONType` (default: str).
    help : str
        Help text to show in `run --help`.
    required : bool
        Require that the user specified a value for the parameter.
        `required=True` implies that the `default` is not used.
    show_default : bool
        If True, show the default value in the help text (default: True).
    """

    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs
        # TODO: check that the type is one of the supported types
        param_type = self.kwargs["type"] = self._get_type(kwargs)
        reserved_params = [
            "params",
            "with",
            "max-num-splits",
            "max-workers",
            "tag",
            "run-id-file",
            "namespace",
        ]

        if self.name in reserved_params:
            raise MetaflowException(
                "Parameter name '%s' is a reserved "
                "word. Please use a different "
                "name for your parameter." % (name)
            )

        # make sure the user is not trying to pass a function in one of the
        # fields that don't support function-values yet
        for field in ("show_default", "separator", "required"):
            if callable(kwargs.get(field)):
                raise MetaflowException(
                    "Parameter *%s*: Field '%s' cannot "
                    "have a function as its value" % (name, field)
                )

        self.kwargs["show_default"] = self.kwargs.get("show_default", True)

        # default can be defined as a function
        default_field = self.kwargs.get("default")
        if callable(default_field) and not isinstance(default_field, DeployTimeField):
            self.kwargs["default"] = DeployTimeField(
                name, param_type, "default", self.kwargs["default"], return_str=True
            )

        # note that separator doesn't work with DeployTimeFields unless you
        # specify type=str
        self.separator = self.kwargs.pop("separator", None)
        if self.separator and not self.is_string_type:
            raise MetaflowException(
                "Parameter *%s*: Separator is only allowed "
                "for string parameters." % name
            )
        parameters.append(self)

    def option_kwargs(self, deploy_mode):
        kwargs = self.kwargs
        if isinstance(kwargs.get("default"), DeployTimeField) and not deploy_mode:
            ret = dict(kwargs)
            help_msg = kwargs.get("help")
            help_msg = "" if help_msg is None else help_msg
            ret["help"] = help_msg + "[default: deploy-time value of '%s']" % self.name
            ret["default"] = None
            ret["required"] = False
            return ret
        else:
            return kwargs

    def load_parameter(self, v):
        return v

    def _get_type(self, kwargs):
        default_type = str

        default = kwargs.get("default")
        if default is not None and not callable(default):
            default_type = type(default)

        return kwargs.get("type", default_type)

    @property
    def is_string_type(self):
        return self.kwargs.get("type", str) == str and isinstance(
            self.kwargs.get("default", ""), strtype
        )

    # this is needed to appease Pylint for JSONType'd parameters,
    # which may do self.param['foobar']
    def __getitem__(self, x):
        pass


def add_custom_parameters(deploy_mode=False):
    # deploy_mode determines whether deploy-time functions should or should
    # not be evaluated for this command
    def wrapper(cmd):
        # Iterate over parameters in reverse order so cmd.params lists options
        # in the order they are defined in the FlowSpec subclass
        for arg in parameters[::-1]:
            kwargs = arg.option_kwargs(deploy_mode)
            cmd.params.insert(0, click.Option(("--" + arg.name,), **kwargs))
        return cmd

    return wrapper
