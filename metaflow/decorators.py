import traceback
from functools import partial
import re

from .flowspec import FlowSpec
from .exception import MetaflowException, InvalidDecoratorAttribute


class BadStepDecoratorException(MetaflowException):
    headline = "Syntax error"

    def __init__(self, deco, func):
        msg =\
            "You tried to apply decorator '{deco}' on '{func}' which is "\
            "not declared as a @step. Make sure you apply this decorator "\
            "on a function which has @step on the line just before the "\
            "function name and @{deco} is above @step.".format(deco=deco,
                                                               func=func.__name__)
        super(BadStepDecoratorException, self).__init__(msg)


class BadFlowDecoratorException(MetaflowException):
    headline = "Syntax error"

    def __init__(self, deconame):
        msg =\
            "Decorator '%s' can be applied only to FlowSpecs. Make sure "\
            "the decorator is above a class definition." % deconame
        super(BadFlowDecoratorException, self).__init__(msg)


class UnknownStepDecoratorException(MetaflowException):
    headline = "Unknown step decorator"

    def __init__(self, deconame):
        from .plugins import STEP_DECORATORS
        decos = ','.join(t.name for t in STEP_DECORATORS)
        msg = "Unknown step decorator *{deconame}*. The following decorators are "\
              "supported: *{decos}*".format(deconame=deconame, decos=decos)
        super(UnknownStepDecoratorException, self).__init__(msg)


class DuplicateStepDecoratorException(MetaflowException):
    headline = "Duplicate decorators"

    def __init__(self, deco, func):
        msg = "Step '{step}' already has a decorator '{deco}'. "\
              "You can specify each decorator only once."\
              .format(step=func.__name__, deco=deco)
        super(DuplicateStepDecoratorException, self).__init__(msg)


class UnknownFlowDecoratorException(MetaflowException):
    headline = "Unknown flow decorator"

    def __init__(self, deconame):
        from .plugins import FLOW_DECORATORS
        decos = ','.join(t.name for t in FLOW_DECORATORS)
        msg = "Unknown flow decorator *{deconame}*. The following decorators are "\
              "supported: *{decos}*".format(deconame=deconame, decos=decos)
        super(UnknownFlowDecoratorException, self).__init__(msg)


class DuplicateFlowDecoratorException(MetaflowException):
    headline = "Duplicate decorators"

    def __init__(self, deco):
        msg = "Flow already has a decorator '{deco}'. "\
              "You can specify each decorator only once."\
              .format(deco=deco)
        super(DuplicateFlowDecoratorException, self).__init__(msg)


class Decorator(object):
    """
    Base class for all decorators.
    """

    name = 'NONAME'
    defaults = {}

    def __init__(self,
                 attributes=None,
                 statically_defined=False):
        self.attributes = self.defaults.copy()
        self.statically_defined = statically_defined

        if attributes:
            for k, v in attributes.items():
                if k in self.defaults:
                    self.attributes[k] = v
                else:
                    raise InvalidDecoratorAttribute(
                        self.name, k, self.defaults)

    @classmethod
    def _parse_decorator_spec(cls, deco_spec):
        top = deco_spec.split(':', 1)
        if len(top) == 1:
            return cls()
        else:
            name, attrspec = top
            attrs = dict(map(lambda x: x.strip(), a.split('=')) 
                for a in re.split(''',(?=[\s\w]+=)''', attrspec.strip('"\'')))
            return cls(attributes=attrs)

    def make_decorator_spec(self):
        attrs = {k: v for k, v in self.attributes.items() if v is not None}
        if attrs:
            attrstr = ','.join('%s=%s' % x for x in attrs.items())
            return '%s:%s' % (self.name, attrstr)
        else:
            return self.name

    def __str__(self):
        mode = 'decorated' if self.statically_defined else 'cli'
        attrs = ' '.join('%s=%s' % x for x in self.attributes.items())
        if attrs:
            attrs = ' ' + attrs
        fmt = '%s<%s%s>' % (self.name, mode, attrs)
        return fmt


class FlowDecorator(Decorator):

    def flow_init(self, flow, graph,  environment, datastore, logger):
        """
        Called when all decorators have been created for this flow.
        """
        pass


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

    def step_init(self, flow, graph, step_name, decorators, environment, datastore, logger):
        """
        Called when all decorators have been created for this step
        """
        pass

    def package_init(self, flow, step_name, environment):
        """
        Called to determine package components
        """
        pass

    def step_task_retry_count(self):
        """
        Called to determine the number of times this task should be retried.
        Returns a tuple of (user_code_retries, error_retries). Error retries
        are attempts to run the process after the user code has failed all
        its retries.
        """
        return 0, 0

    def runtime_init(self, flow, graph, package, run_id):
        """
        Top-level initialization before anything gets run in the runtime
        context.
        """
        pass

    def runtime_task_created(self,
                             datastore,
                             task_id,
                             split_index,
                             input_paths,
                             is_cloned):
        """
        Called when the runtime has created a task related to this step.
        """
        pass

    def runtime_finished(self, exception):
        """
        Called when the runtime created task finishes or encounters an interrupt/exception.
        """
        pass

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries):
        """
        Access the command line for a step execution in the runtime context.
        """
        pass

    def task_pre_step(self,
                      step_name,
                      datastore,
                      metadata,
                      run_id,
                      task_id,
                      flow,
                      graph,
                      retry_count,
                      max_user_code_retries):
        """
        Run before the step function in the task context.
        """
        pass

    def task_decorate(self,
                      step_func,
                      flow,
                      graph,
                      retry_count,
                      max_user_code_retries):
        return step_func

    def task_post_step(self,
                       step_name,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):
        """
        Run after the step function has finished successfully in the task
        context.
        """
        pass

    def task_exception(self,
                       exception,
                       step_name,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):
        """
        Run if the step function raised an exception in the task context.

        If this method returns True, it is assumed that the exception has
        been taken care of and the flow may continue.
        """
        pass

    def task_finished(self,
                      step_name,
                      flow,
                      graph,
                      is_task_ok,
                      retry_count,
                      max_user_code_retries):
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
            # _flow_decorators.
            if decofunc.name in cls._flow_decorators:
                raise DuplicateFlowDecoratorException(decofunc.name)
            else:
                cls._flow_decorators[decofunc.name] = decofunc(attributes=kwargs,
                                                               statically_defined=True)
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
        if not hasattr(func, 'is_step'):
            raise BadStepDecoratorException(decotype.name, func)

        # Only the first decorator applies
        if decotype.name in [deco.name for deco in func.decorators]:
            raise DuplicateStepDecoratorException(decotype.name, func)
        else:
            func.decorators.append(decotype(attributes=kwargs,
                                            statically_defined=True))

        return func
    else:
        # Keyword arguments specified, e.g. @foobar(a=1, b=2).
        # Return a decorator function that will get the actual
        # function to be decorated as the first argument.
        def wrap(f):
            return _base_step_decorator(decotype, f, **kwargs)
        return wrap


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
    from .plugins import STEP_DECORATORS
    decos = {decotype.name: decotype for decotype in STEP_DECORATORS}
    for decospec in decospecs:
        deconame = decospec.split(':')[0]
        if deconame not in decos:
            raise UnknownStepDecoratorException(deconame)
        # Attach the decorator to step if it doesn't have the decorator
        # already. This means that statically defined decorators are always
        # preferred over runtime decorators.
        if deconame not in [deco.name for deco in step.decorators]:
            deco = decos[deconame]._parse_decorator_spec(decospec)
            step.decorators.append(deco)

def _init_decorators(flow, graph, environment, datastore, logger):
    for deco in flow._flow_decorators.values():
        deco.flow_init(flow, graph,  environment, datastore, logger)
    for step in flow:
        for deco in step.decorators:
            deco.step_init(flow, graph, step.__name__,
                           step.decorators, environment, datastore, logger)

def step(f):
    """
    The step decorator. Makes a method a step in the workflow.
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
