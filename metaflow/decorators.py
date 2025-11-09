import importlib
import json
import re

from functools import partial
from typing import Any, Callable, Dict, List, NewType, Tuple, TypeVar, Union, overload

from .flowspec import FlowSpec, _FlowState
from .exception import (
    MetaflowInternalError,
    MetaflowException,
    InvalidDecoratorAttribute,
)

from .debug import debug
from .parameters import current_flow
from .user_configs.config_parameters import (
    UNPACK_KEY,
    resolve_delayed_evaluator,
    unpack_delayed_evaluator,
)
from .user_decorators.mutable_flow import MutableFlow
from .user_decorators.mutable_step import MutableStep
from .user_decorators.user_flow_decorator import FlowMutator, FlowMutatorMeta
from .user_decorators.user_step_decorator import (
    StepMutator,
    UserStepDecoratorBase,
    UserStepDecoratorMeta,
)
from .metaflow_config import SPIN_ALLOWED_DECORATORS
from metaflow._vendor import click


class BadStepDecoratorException(MetaflowException):
    headline = "Syntax error"

    def __init__(self, deco, func):
        msg = (
            "You tried to apply decorator '{deco}' on '{func}' which is "
            "not declared as a @step. Make sure you apply this decorator "
            "on a function which has @step on the line just before the "
            "function name and @{deco} is above @step.".format(
                deco=deco, func=getattr(func, "__name__", str(func))
            )
        )
        super(BadStepDecoratorException, self).__init__(msg)


class BadFlowDecoratorException(MetaflowException):
    headline = "Syntax error"

    def __init__(self, deconame):
        msg = (
            "Decorator '%s' can be applied only to FlowSpecs. Make sure "
            "the decorator is above a class definition." % deconame
        )
        super(BadFlowDecoratorException, self).__init__(msg)


class UnknownStepDecoratorException(MetaflowException):
    headline = "Unknown step decorator"

    def __init__(self, deconame):
        decos = ", ".join(
            [
                x
                for x in UserStepDecoratorMeta.all_decorators().keys()
                if not x.endswith("_internal")
            ]
        )

        msg = (
            "Unknown step decorator *{deconame}*. The following decorators are "
            "supported: *{decos}*".format(deconame=deconame, decos=decos)
        )
        super(UnknownStepDecoratorException, self).__init__(msg)


class DuplicateStepDecoratorException(MetaflowException):
    headline = "Duplicate decorators"

    def __init__(self, deco, func):
        msg = (
            "Step '{step}' already has a decorator '{deco}'. "
            "You can specify this decorator only once.".format(
                step=func.__name__, deco=deco
            )
        )
        super(DuplicateStepDecoratorException, self).__init__(msg)


class UnknownFlowDecoratorException(MetaflowException):
    headline = "Unknown flow decorator"

    def __init__(self, deconame):
        decos = ", ".join(FlowMutatorMeta.all_decorators().keys())
        msg = (
            "Unknown flow decorator *{deconame}*. The following decorators are "
            "supported: *{decos}*".format(deconame=deconame, decos=decos)
        )
        super(UnknownFlowDecoratorException, self).__init__(msg)


class DuplicateFlowDecoratorException(MetaflowException):
    headline = "Duplicate decorators"

    def __init__(self, deco):
        msg = (
            "Flow already has a decorator '{deco}'. "
            "You can specify each decorator only once.".format(deco=deco)
        )
        super(DuplicateFlowDecoratorException, self).__init__(msg)


class Decorator(object):
    """
    Base class for all decorators.
    """

    name = "NONAME"
    defaults = {}
    # `allow_multiple` allows setting many decorators of the same type to a step/flow.
    allow_multiple = False

    def __init__(self, attributes=None, statically_defined=False, inserted_by=None):
        self.attributes = self.defaults.copy()
        self.statically_defined = statically_defined
        self.inserted_by = inserted_by
        self._user_defined_attributes = set()
        self._ran_init = False

        if attributes:
            for k, v in attributes.items():
                if k in self.defaults or k.startswith(UNPACK_KEY):
                    self.attributes[k] = v
                    if not k.startswith(UNPACK_KEY):
                        self._user_defined_attributes.add(k)
                else:
                    raise InvalidDecoratorAttribute(self.name, k, self.defaults)

    def init(self):
        """
        Initializes the decorator. In general, any operation you would do in __init__
        should be done here.
        """
        pass

    def external_init(self):
        # In some cases (specifically when using remove_decorator), we may need to call
        # init multiple times. Short-circuit re-evaluating.
        if self._ran_init:
            return

        # Note that by design, later values override previous ones.
        self.attributes, new_user_attributes = unpack_delayed_evaluator(self.attributes)
        self._user_defined_attributes.update(new_user_attributes)
        self.attributes = resolve_delayed_evaluator(self.attributes, to_dict=True)

        if "init" in self.__class__.__dict__:
            self.init()
        self._ran_init = True

    @classmethod
    def extract_args_kwargs_from_decorator_spec(cls, deco_spec):
        if len(deco_spec) == 0:
            return [], {}

        attrs = {}
        # TODO: Do we really want to allow spaces in the names of attributes?!?
        for a in re.split(r""",(?=[\s\w]+=)""", deco_spec):
            name, val = a.split("=", 1)
            try:
                val_parsed = json.loads(val.strip().replace('\\"', '"'))
            except json.JSONDecodeError:
                # In this case, we try to convert to either an int or a float or
                # leave as is. Prefer ints if possible.
                try:
                    val_parsed = int(val.strip())
                except ValueError:
                    try:
                        val_parsed = float(val.strip())
                    except ValueError:
                        val_parsed = val.strip()

            attrs[name.strip()] = val_parsed

        return [], attrs

    @classmethod
    def parse_decorator_spec(cls, deco_spec):
        if len(deco_spec) == 0:
            return cls()

        _, kwargs = cls.extract_args_kwargs_from_decorator_spec(deco_spec)
        return cls(attributes=kwargs)

    def make_decorator_spec(self):
        # Make sure all attributes are evaluated
        self.external_init()
        attrs = {k: v for k, v in self.attributes.items() if v is not None}
        if attrs:
            attr_list = []
            # We dump simple types directly as string to get around the nightmare quote
            # escaping but for more complex types (typically dictionaries or lists),
            # we dump using JSON.
            for k, v in attrs.items():
                if isinstance(v, (int, float, str)):
                    attr_list.append("%s=%s" % (k, str(v)))
                else:
                    attr_list.append("%s=%s" % (k, json.dumps(v).replace('"', '\\"')))

            attrstr = ",".join(attr_list)
            return "%s:%s" % (self.name, attrstr)
        else:
            return self.name

    def get_args_kwargs(self) -> Tuple[List[Any], Dict[str, Any]]:
        """
        Get the arguments and keyword arguments of the decorator.

        Returns
        -------
        Tuple[List[Any], Dict[str, Any]]
            A tuple containing a list of arguments and a dictionary of keyword arguments.
        """
        return [], dict(self.attributes)

    def __str__(self):
        mode = "static" if self.statically_defined else "dynamic"
        if self.inserted_by:
            mode += " (inserted by %s)" % " from ".join(self.inserted_by)
        attrs = " ".join("%s=%s" % x for x in self.attributes.items())
        if attrs:
            attrs = " " + attrs
        fmt = "%s<%s%s>" % (self.name, mode, attrs)
        return fmt


class FlowDecorator(Decorator):
    options = {}

    def __init__(self, *args, **kwargs):
        super(FlowDecorator, self).__init__(*args, **kwargs)

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        """
        Called when all decorators have been created for this flow.
        """
        pass

    def get_top_level_options(self):
        """
        Return a list of option-value pairs that correspond to top-level
        options that should be passed to subprocesses (tasks). The option
        names should be a subset of the keys in self.options.

        If the decorator has a non-empty set of options in `self.options`, you
        probably want to return the assigned values in this method.
        """
        return []


# compare this to parameters.add_custom_parameters
def add_decorator_options(cmd):
    flow_cls = getattr(current_flow, "flow_cls", None)
    if flow_cls is None:
        return cmd

    seen = {}
    existing_params = set(p.name.lower() for p in cmd.params)
    # Add decorator options
    for deco in flow_decorators(flow_cls):
        for option, kwargs in deco.options.items():
            if option in seen:
                msg = (
                    "Flow decorator '%s' uses an option '%s' which is also "
                    "used by the decorator '%s'. This is a bug in Metaflow. "
                    "Please file a ticket on GitHub."
                    % (deco.name, option, seen[option])
                )
                raise MetaflowInternalError(msg)
            elif deco.name.lower() in existing_params:
                raise MetaflowInternalError(
                    "Flow decorator '%s' uses an option '%s' which is a reserved "
                    "keyword. Please use a different option name." % (deco.name, option)
                )
            else:
                kwargs["envvar"] = "METAFLOW_FLOW_%s" % option.upper()
                seen[option] = deco.name
                cmd.params.insert(0, click.Option(("--" + option,), **kwargs))
    return cmd


def flow_decorators(flow_cls):
    return [d for deco_list in flow_cls._flow_decorators.values() for d in deco_list]


class StepDecorator(Decorator):
    """
    Base class for all step decorators.

    Example:

    @my_decorator
    @step
    def a(self):
        pass

    @my_decorator
    @step
    def b(self):
        pass

    To make the above work, define a subclass

    class MyDecorator(StepDecorator):
        name = "my_decorator"

    and include it in plugins.STEP_DECORATORS. Now both a() and b()
    get an instance of MyDecorator, so you can keep step-specific
    state easily.

    TODO (savin): Initialize the decorators with flow, graph,
                  step.__name__ etc., so that we don't have to
                  pass them around with every lifecycle call.
    """

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        """
        Called when all decorators have been created for this step
        """
        pass

    def package_init(self, flow, step_name, environment):
        """
        Called to determine package components
        """
        pass

    def add_to_package(self):
        """
        Called to add custom files needed for this environment. This hook will be
        called in the `MetaflowPackage` class where metaflow compiles the code package
        tarball. This hook can return one of two things (the first is for backwards
        compatibility -- move to the second):
          - a generator yielding a tuple of `(file_path, arcname)` to add files to
            the code package. `file_path` is the path to the file on the local filesystem
            and `arcname` is the path relative to the packaged code.
          - a generator yielding a tuple of `(content, arcname, type)` where:
            - type is one of
            ContentType.{USER_CONTENT, CODE_CONTENT, MODULE_CONTENT, OTHER_CONTENT}
            - for USER_CONTENT:
              - the file will be included relative to the directory containing the
                user's flow file.
              - content: path to the file to include
              - arcname: path relative to the directory containing the user's flow file
            - for CODE_CONTENT:
              - the file will be included relative to the code directory in the package.
                This will be the directory containing `metaflow`.
              - content: path to the file to include
              - arcname: path relative to the code directory in the package
            - for MODULE_CONTENT:
              - the module will be added to the code package as a python module. It will
                be accessible as usual (import <module_name>)
              - content: name of the module
              - arcname: None (ignored)
            - for OTHER_CONTENT:
              - the file will be included relative to any other configuration/metadata
                files for the flow
              - content: path to the file to include
              - arcname: path relative to the config directory in the package
        """
        return []

    def step_task_retry_count(self):
        """
        Called to determine the number of times this task should be retried.
        Returns a tuple of (user_code_retries, error_retries). Error retries
        are attempts to run the process after the user code has failed all
        its retries.

        Typically, the runtime takes the maximum of retry counts across
        decorators and user specification to determine the task retry count.
        If you want to force no retries, return the special values (None, None).
        """
        return 0, 0

    def runtime_init(self, flow, graph, package, run_id):
        """
        Top-level initialization before anything gets run in the runtime
        context.
        """
        pass

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        """
        Called when the runtime has created a task related to this step.
        """
        pass

    def runtime_finished(self, exception):
        """
        Called when the runtime created task finishes or encounters an interrupt/exception.
        """
        pass

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        """
        Access the command line for a step execution in the runtime context.
        """
        pass

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        """
        Run before the step function in the task context.
        """
        pass

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return step_func

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        """
        Run after the step function has finished successfully in the task
        context.
        """
        pass

    def task_exception(
        self, exception, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        """
        Run if the step function raised an exception in the task context.

        If this method returns True, it is assumed that the exception has
        been taken care of and the flow may continue.
        """
        pass

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        """
        Run after the task context has been finalized.

        is_task_ok is set to False if the user code raised an exception that
        was not handled by any decorator.

        Note that you can't create or modify data artifacts in this method
        since the task has been finalized by the time this method
        is called. Also note that the task may fail after this method has been
        called, so this method may get called multiple times for a task over
        multiple attempts, similar to all task_ methods.
        """
        pass


def _base_flow_decorator(decofunc, *args, **kwargs):
    """
    Decorator prototype for all flow (class) decorators. This function gets
    specialized and imported for all decorators types by
    _import_plugin_decorators().
    """
    if args:
        # No keyword arguments specified for the decorator, e.g. @foobar.
        # The first argument is the class to be decorated.
        cls = args[0]
        if isinstance(cls, type) and issubclass(cls, FlowSpec):
            # flow decorators add attributes in the class dictionary,
            # _flow_decorators. _flow_decorators is of type `{key:[decos]}`
            if decofunc.name in cls._flow_decorators and not decofunc.allow_multiple:
                raise DuplicateFlowDecoratorException(decofunc.name)
            else:
                deco_instance = decofunc(attributes=kwargs, statically_defined=True)
                cls._flow_decorators.setdefault(decofunc.name, []).append(deco_instance)
        else:
            raise BadFlowDecoratorException(decofunc.name)
        return cls
    else:
        # Keyword arguments specified, e.g. @foobar(a=1, b=2).
        # Return a decorator function that will get the actual
        # function to be decorated as the first argument.
        def wrap(f):
            return _base_flow_decorator(decofunc, f, **kwargs)

        return wrap


def _base_step_decorator(decotype, *args, **kwargs):
    """
    Decorator prototype for all step decorators. This function gets specialized
    and imported for all decorators types by _import_plugin_decorators().
    """

    if args:
        # No keyword arguments specified for the decorator, e.g. @foobar.
        # The first argument is the function to be decorated.
        func = args[0]
        if isinstance(func, (StepMutator, UserStepDecoratorBase)):
            func = func._my_step
        if not hasattr(func, "is_step"):
            raise BadStepDecoratorException(decotype.name, func)

        # if `allow_multiple` is not `True` then only one decorator type is allowed per step
        if (
            decotype.name in [deco.name for deco in func.decorators]
            and not decotype.allow_multiple
        ):
            raise DuplicateStepDecoratorException(decotype.name, func)
        else:
            func.decorators.append(decotype(attributes=kwargs, statically_defined=True))

        return func
    else:
        # Keyword arguments specified, e.g. @foobar(a=1, b=2).
        # Return a decorator function that will get the actual
        # function to be decorated as the first argument.
        def wrap(f):
            return _base_step_decorator(decotype, f, **kwargs)

        return wrap


_all_step_decos = None
_all_flow_decos = None


def get_all_step_decos():
    global _all_step_decos
    if _all_step_decos is None:
        from .plugins import STEP_DECORATORS

        _all_step_decos = {decotype.name: decotype for decotype in STEP_DECORATORS}
    return _all_step_decos


def get_all_flow_decos():
    global _all_flow_decos
    if _all_flow_decos is None:
        from .plugins import FLOW_DECORATORS

        _all_flow_decos = {decotype.name: decotype for decotype in FLOW_DECORATORS}
    return _all_flow_decos


def extract_step_decorator_from_decospec(decospec: str):
    splits = decospec.split(":", 1)
    deconame = splits[0]

    # Check if it is a user-defined decorator or metaflow decorator
    deco_cls = UserStepDecoratorMeta.get_decorator_by_name(deconame)
    if deco_cls is not None:
        return (
            deco_cls.parse_decorator_spec(splits[1] if len(splits) > 1 else ""),
            len(splits) > 1,
        )

    # Check if this is a decorator we can import
    if "." in deconame:
        # We consider this to be a import path to a user decorator so
        # something like "my_package.my_decorator"
        module_name, class_name = deconame.rsplit(".", 1)
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            raise MetaflowException(
                "Could not import user decorator %s" % deconame
            ) from e
        deco_cls = getattr(module, class_name, None)
        if (
            deco_cls is None
            or not isinstance(deco_cls, type)
            or not issubclass(deco_cls, UserStepDecoratorBase)
        ):
            raise UnknownStepDecoratorException(deconame)
        return (
            deco_cls.parse_decorator_spec(splits[1] if len(splits) > 1 else ""),
            len(splits) > 1,
        )

    raise UnknownStepDecoratorException(deconame)


def extract_flow_decorator_from_decospec(decospec: str):
    splits = decospec.split(":", 1)
    deconame = splits[0]
    # Check if it is a user-defined decorator or metaflow decorator
    deco_cls = FlowMutatorMeta.get_decorator_by_name(deconame)
    if deco_cls is not None:
        return (
            deco_cls.parse_decorator_spec(splits[1] if len(splits) > 1 else ""),
            len(splits) > 1,
        )
    else:
        raise UnknownFlowDecoratorException(deconame)


def _attach_decorators(flow, decospecs):
    """
    Attach decorators to all steps during runtime. This has the same
    effect as if you defined the decorators statically in the source for
    every step. Used by --with command line parameter.
    """
    # Attach the decorator to all steps that don't have this decorator
    # already. This means that statically defined decorators are always
    # preferred over runtime decorators.
    #
    # Note that each step gets its own instance of the decorator class,
    # so decorator can maintain step-specific state.

    for step in flow:
        _attach_decorators_to_step(step, decospecs)


def _attach_decorators_to_step(step, decospecs):
    """
    Attach decorators to a step during runtime. This has the same
    effect as if you defined the decorators statically in the source for
    the step.
    """
    for decospec in decospecs:
        step_deco, _ = extract_step_decorator_from_decospec(decospec)
        if isinstance(step_deco, StepDecorator):
            # Check multiple
            if (
                step_deco.name not in [deco.name for deco in step.decorators]
                or step_deco.allow_multiple
            ):
                step.decorators.append(step_deco)
            # Else it is ignored -- this is a non-static decorator

        else:
            step_deco.add_or_raise(step, False, 1, None)


def _should_skip_decorator_for_spin(
    deco, is_spin, skip_decorators, logger, decorator_type="decorator"
):
    """
    Determine if a decorator should be skipped for spin steps.

    Parameters:
    -----------
    deco : Decorator
        The decorator instance to check
    is_spin : bool
        Whether this is a spin step
    skip_decorators : bool
        Whether to skip all decorators
    logger : callable
        Logger function for warnings
    decorator_type : str
        Type of decorator ("Flow decorator" or "Step decorator") for logging

    Returns:
    --------
    bool
        True if the decorator should be skipped, False otherwise
    """
    if not is_spin:
        return False

    # Skip all decorator hooks if skip_decorators is True
    if skip_decorators:
        return True

    # Run decorator hooks for spin steps only if they are in the whitelist
    if deco.name not in SPIN_ALLOWED_DECORATORS:
        logger(
            f"[Warning] Ignoring {decorator_type} '{deco.name}' as it is not supported in spin steps.",
            system_msg=True,
            timestamp=False,
            bad=True,
        )
        return True

    return False


def _init(flow, only_non_static=False):
    for decorators in flow._flow_decorators.values():
        for deco in decorators:
            deco.external_init()

    for flowstep in flow:
        for deco in flowstep.decorators:
            deco.external_init()
        for deco in flowstep.config_decorators or []:
            deco.external_init()
        for deco in flowstep.wrappers or []:
            deco.external_init()


def _init_flow_decorators(
    flow,
    graph,
    environment,
    flow_datastore,
    metadata,
    logger,
    echo,
    deco_options,
    is_spin=False,
    skip_decorators=False,
):
    # Since all flow decorators are stored as `{key:[deco]}` we iterate through each of them.
    for decorators in flow._flow_decorators.values():
        # First resolve the `options` for the flow decorator.
        # Options are passed from cli.
        # For example `@project` can take a `--name` / `--branch` from the cli as options.
        deco_flow_init_options = {}
        deco = decorators[0]
        # If a flow decorator allow multiple of same type then we don't allow multiple options for it.
        if deco.allow_multiple:
            if len(deco.options) > 0:
                raise MetaflowException(
                    "Flow decorator `@%s` has multiple options, which is not allowed. "
                    "Please ensure the FlowDecorator `%s` has no options since flow decorators with "
                    "`allow_mutiple=True` are not allowed to have options"
                    % (deco.name, deco.__class__.__name__)
                )
        else:
            # Each "non-multiple" flow decorator is only allowed to have one set of options
            # Note that there may be no deco_options if a MutableFlow config injected
            # the decorator.
            deco_flow_init_options = {
                option: deco_options.get(
                    option.replace("-", "_"), option_info["default"]
                )
                for option, option_info in deco.options.items()
            }
        for deco in decorators:
            if _should_skip_decorator_for_spin(
                deco, is_spin, skip_decorators, logger, "Flow decorator"
            ):
                continue
            deco.flow_init(
                flow,
                graph,
                environment,
                flow_datastore,
                metadata,
                logger,
                echo,
                deco_flow_init_options,
            )


def _init_step_decorators(
    flow,
    graph,
    environment,
    flow_datastore,
    logger,
    is_spin=False,
    skip_decorators=False,
):
    # NOTE: We don't need the graph but keeping it for backwards compatibility with
    # extensions that use it directly. We will remove it at some point.

    # We call the mutate method for both the flow and step mutators.
    cls = flow.__class__
    # Run all the decorators. We first run the flow-level decorators
    # and then the step level ones to maintain a consistent order with how
    # other decorators are run.

    for deco in cls._flow_state.get(_FlowState.FLOW_MUTATORS, []):
        if isinstance(deco, FlowMutator):
            inserted_by_value = [deco.decorator_name] + (deco.inserted_by or [])
            mutable_flow = MutableFlow(
                cls,
                pre_mutate=False,
                statically_defined=deco.statically_defined,
                inserted_by=inserted_by_value,
            )
            # Sanity check to make sure we are applying the decorator to the right
            # class
            if not deco._flow_cls == cls and not issubclass(cls, deco._flow_cls):
                raise MetaflowInternalError(
                    "FlowMutator registered on the wrong flow -- "
                    "expected %s but got %s" % (deco._flow_cls.__name__, cls.__name__)
                )
            debug.userconf_exec(
                "Evaluating flow level decorator %s (mutate)" % deco.__class__.__name__
            )
            deco.mutate(mutable_flow)
            # We reset cached_parameters on the very off chance that the user added
            # more configurations based on the configuration
            if _FlowState.CACHED_PARAMETERS in cls._flow_state:
                del cls._flow_state[_FlowState.CACHED_PARAMETERS]
        else:
            raise MetaflowInternalError(
                "A non FlowMutator found in flow custom decorators"
            )

    for step in cls._steps:
        for deco in step.config_decorators:
            inserted_by_value = [deco.decorator_name] + (deco.inserted_by or [])

            if isinstance(deco, StepMutator):
                debug.userconf_exec(
                    "Evaluating step level decorator %s for %s (mutate)"
                    % (deco.__class__.__name__, step.name)
                )
                deco.mutate(
                    MutableStep(
                        cls,
                        step,
                        pre_mutate=False,
                        statically_defined=deco.statically_defined,
                        inserted_by=inserted_by_value,
                    )
                )
            else:
                raise MetaflowInternalError(
                    "A non StepMutator found in step custom decorators"
                )

        if step.config_decorators:
            # We remove all mention of the custom step decorator
            setattr(cls, step.name, step)

    cls._init_graph()
    graph = flow._graph

    for step in flow:
        for deco in step.decorators:
            if _should_skip_decorator_for_spin(
                deco, is_spin, skip_decorators, logger, "Step decorator"
            ):
                continue
            deco.step_init(
                flow,
                graph,
                step.__name__,
                step.decorators,
                environment,
                flow_datastore,
                logger,
            )


FlowSpecDerived = TypeVar("FlowSpecDerived", bound=FlowSpec)

# The StepFlag is a "fake" input item to be able to distinguish
# callables and those that have had a `@step` decorator on them. This enables us
# to check the ordering of decorators (ie: put @step first) with the type
# system. There should be a better way to do this with a more flexible type
# system but this is what works for now with the Python type system
StepFlag = NewType("StepFlag", bool)


@overload
def step(
    f: Callable[[FlowSpecDerived], None],
) -> Callable[[FlowSpecDerived, StepFlag], None]: ...


@overload
def step(
    f: Callable[[FlowSpecDerived, Any], None],
) -> Callable[[FlowSpecDerived, Any, StepFlag], None]: ...


def step(
    f: Union[Callable[[FlowSpecDerived], None], Callable[[FlowSpecDerived, Any], None]],
):
    """
    Marks a method in a FlowSpec as a Metaflow Step. Note that this
    decorator needs to be placed as close to the method as possible (ie:
    before other decorators).

    In other words, this is valid:
    ```
    @batch
    @step
    def foo(self):
        pass
    ```

    whereas this is not:
    ```
    @step
    @batch
    def foo(self):
        pass
    ```

    Parameters
    ----------
    f : Union[Callable[[FlowSpecDerived], None], Callable[[FlowSpecDerived, Any], None]]
        Function to make into a Metaflow Step

    Returns
    -------
    Union[Callable[[FlowSpecDerived, StepFlag], None], Callable[[FlowSpecDerived, Any, StepFlag], None]]
        Function that is a Metaflow Step
    """
    f.is_step = True
    f.decorators = []
    f.config_decorators = []
    f.wrappers = []
    f.name = f.__name__
    return f


def _import_plugin_decorators(globals_dict):
    """
    Auto-generate a decorator function for every decorator
    defined in plugins.STEP_DECORATORS and plugins.FLOW_DECORATORS.
    """
    from .plugins import STEP_DECORATORS, FLOW_DECORATORS

    # Q: Why not use StepDecorators directly as decorators?
    # A: Getting an object behave as a decorator that can work
    #    both with and without arguments is surprisingly hard.
    #    It is easier to make plain function decorators work in
    #    the dual mode - see _base_step_decorator above.
    for decotype in STEP_DECORATORS:
        globals_dict[decotype.name] = partial(_base_step_decorator, decotype)

    # add flow-level decorators
    for decotype in FLOW_DECORATORS:
        globals_dict[decotype.name] = partial(_base_flow_decorator, decotype)
