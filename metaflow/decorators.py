from functools import partial
import json
import re

from typing import Any, Callable, NewType, TypeVar, Union, overload

from .flowspec import FlowSpec
from .exception import (
    MetaflowInternalError,
    MetaflowException,
    InvalidDecoratorAttribute,
)

from .parameters import current_flow

from metaflow._vendor import click

try:
    unicode
except NameError:
    unicode = str
    basestring = str


class BadStepDecoratorException(MetaflowException):
    headline = "Syntax error"

    def __init__(self, deco, func):
        msg = (
            "You tried to apply decorator '{deco}' on '{func}' which is "
            "not declared as a @step. Make sure you apply this decorator "
            "on a function which has @step on the line just before the "
            "function name and @{deco} is above @step.".format(
                deco=deco, func=func.__name__
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
        from .plugins import STEP_DECORATORS

        decos = ", ".join(
            t.name for t in STEP_DECORATORS if not t.name.endswith("_internal")
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
        from .plugins import FLOW_DECORATORS

        decos = ", ".join(t.name for t in FLOW_DECORATORS)
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

    def __init__(self, attributes=None, statically_defined=False):
        self.attributes = self.defaults.copy()
        self.statically_defined = statically_defined

        if attributes:
            for k, v in attributes.items():
                if k in self.defaults:
                    self.attributes[k] = v
                else:
                    raise InvalidDecoratorAttribute(self.name, k, self.defaults)

    @classmethod
    def _parse_decorator_spec(cls, deco_spec):
        if len(deco_spec) == 0:
            return cls()

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
        return cls(attributes=attrs)

    def make_decorator_spec(self):
        attrs = {k: v for k, v in self.attributes.items() if v is not None}
        if attrs:
            attr_list = []
            # We dump simple types directly as string to get around the nightmare quote
            # escaping but for more complex types (typically dictionaries or lists),
            # we dump using JSON.
            for k, v in attrs.items():
                if isinstance(v, (int, float, unicode, basestring)):
                    attr_list.append("%s=%s" % (k, str(v)))
                else:
                    attr_list.append("%s=%s" % (k, json.dumps(v).replace('"', '\\"')))

            attrstr = ",".join(attr_list)
            return "%s:%s" % (self.name, attrstr)
        else:
            return self.name

    def __str__(self):
        mode = "static" if self.statically_defined else "dynamic"
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
    seen = {}
    flow_cls = getattr(current_flow, "flow_cls", None)
    if flow_cls is None:
        return cmd
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
            else:
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
        Called to add custom packages needed for a decorator. This hook will be
        called in the `MetaflowPackage` class where metaflow compiles the code package
        tarball. This hook is invoked in the `MetaflowPackage`'s `path_tuples`
        function. The `path_tuples` function is a generator that yields a tuple of
        `(file_path, arcname)`.`file_path` is the path of the file in the local file system;
        the `arcname` is the path of the file in the constructed tarball or the path of the file
        after decompressing the tarball.

        Returns a list of tuples where each tuple represents (file_path, arcname)
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


def _get_all_step_decos():
    global _all_step_decos
    if _all_step_decos is None:
        from .plugins import STEP_DECORATORS

        _all_step_decos = {decotype.name: decotype for decotype in STEP_DECORATORS}
    return _all_step_decos


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

    decos = _get_all_step_decos()

    for decospec in decospecs:
        splits = decospec.split(":", 1)
        deconame = splits[0]
        if deconame not in decos:
            raise UnknownStepDecoratorException(deconame)
        # Attach the decorator to step if it doesn't have the decorator
        # already. This means that statically defined decorators are always
        # preferred over runtime decorators.
        if (
            deconame not in [deco.name for deco in step.decorators]
            or decos[deconame].allow_multiple
        ):
            # if the decorator is present in a step and is of type allow_multiple
            # then add the decorator to the step
            deco = decos[deconame]._parse_decorator_spec(
                splits[1] if len(splits) > 1 else ""
            )
            step.decorators.append(deco)


def _init_flow_decorators(
    flow, graph, environment, flow_datastore, metadata, logger, echo, deco_options
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
            deco_flow_init_options = {
                option: deco_options[option.replace("-", "_")]
                for option in deco.options
            }
        for deco in decorators:
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


def _init_step_decorators(flow, graph, environment, flow_datastore, logger):
    for step in flow:
        for deco in step.decorators:
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
    f: Callable[[FlowSpecDerived], None]
) -> Callable[[FlowSpecDerived, StepFlag], None]:
    ...


@overload
def step(
    f: Callable[[FlowSpecDerived, Any], None],
) -> Callable[[FlowSpecDerived, Any, StepFlag], None]:
    ...


def step(
    f: Union[Callable[[FlowSpecDerived], None], Callable[[FlowSpecDerived, Any], None]]
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
    try:
        # python 3
        f.name = f.__name__
    except:
        # python 2
        f.name = f.__func__.func_name
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
