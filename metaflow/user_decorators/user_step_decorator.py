import inspect
import json
import types

from functools import partial
from typing import Callable, Any, Optional, TYPE_CHECKING, Union

from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import resolve_delayed_evaluator

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_step


class _StepDecorator:

    _step_field = None
    _allowed_args = False
    decorator_name = None

    def __init__(self, *args, **kwargs):
        if self.decorator_name is None:
            self.decorator_name = "%s.%s" % (self.__module__, self.__class__.__name__)
        arg = None
        if args:
            if isinstance(args[0], _StepDecorator):
                arg = args[0]._my_step
            else:
                arg = args[0]
        if arg and callable(arg) and hasattr(arg, "is_step"):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the step
            self._set_my_step(arg)
        else:
            if not self._allowed_args:
                raise MetaflowException(
                    "Step decorator '%s' does not allow arguments" % str(self)
                )
            elif isinstance(self._allowed_args, list) and (
                len(args) != 0 or any(a not in self._allowed_args for a in kwargs)
            ):
                raise MetaflowException(
                    "Step decorator '%s' only allows the following keyword arguments: %s"
                    % (self.decorator_name, str(self._allowed_args))
                )

            # Store the args so we can use them when we also get the step we are applied
            # to in the __call__ method.
            self._args = args
            self._kwargs = kwargs

    def __get__(self, instance, owner):
        # Required so that we "present" as a step when the step decorator is
        # of the form
        # @MyStepDecorator
        # @step
        # def my_step(self):
        #     pass
        #
        # In that case, if we don't have __get__, the object is a _StepDecorator
        # and not a step. Other parts of the code rely on steps having is_step. There are
        # other ways to solve this but this allowed for minimal changes going forward.

        # Note, there are two cases:
        #  - we want to get the function of the class (not through an instance)
        #  - we are accessing it through an instance in which case we need to return
        #    a *bound* method.
        f = self()
        if instance:
            # We are accessing the step decorator through an instance of the class so
            # we will bind it to make it a method
            return f.__get__(instance, owner)
        return f

    def __call__(
        self,
        step: Optional[
            Union[
                Callable[["metaflow.decorators.FlowSpecDerived"], None],
                Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
            ]
        ] = None,
    ) -> Union[
        Callable[["metaflow.decorators.FlowSpecDerived"], None],
        Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
    ]:
        if step:
            if isinstance(step, _StepDecorator):
                step = step._my_step
            # You can use config values in the arguments to a _StepDecorator
            # so we resolve those as well
            new_args = [resolve_delayed_evaluator(arg) for arg in self._args]
            new_kwargs = {
                k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
            }
            if new_args or new_kwargs:
                self.init(*new_args, **new_kwargs)
                if hasattr(self, "_empty_init"):
                    raise MetaflowException(
                        "Step decorator '%s' is used with arguments "
                        "but does not implement init" % self.decorator_name
                    )
            return self._set_my_step(step)
        elif not self._my_step:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non step
            raise MetaflowException(
                "Step decorator '%s' can only be applied to a step function"
                % self.decorator_name
            )
        return self._my_step

    def _set_my_step(
        self,
        step: Union[
            Callable[["metaflow.decorators.FlowSpecDerived"], None],
            Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
        ],
    ) -> Union[
        Callable[["metaflow.decorators.FlowSpecDerived"], None],
        Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
    ]:

        self._my_step = step
        if self._step_field is None:
            raise RuntimeError(
                "_StepDecorator is not properly overloaded; missing _step_field. "
                "This is a Metaflow bug, please contact support."
            )
        getattr(self._my_step, self._step_field).append(self)
        return self._my_step

    def init(self, *args, **kwargs):
        # Allows us to catch issues where the user passes arguments to a decorator
        # but doesn't tell us how to handle them.
        self._empty_init = True


class StepDecorator(_StepDecorator):
    _step_field = "wrappers"
    _allowed_args = True
    decorator_name: Optional[str] = None
    """
    Set this to the name of the decorator. If not specified, the fully qualified name
    of the class will be used.
    """

    def init(self, **kwargs):
        """
        This method is called when the step decorator is initialized. This allows you to
        process any arguments to it.

        As an example:
        ```
        @StepDecorator
        class MyDecorator(StepDecoratorClass):
            def init(self, arg1, arg2):
                self.arg1 = arg1
                self.arg2 = arg2
                # Do something with the arguments
        ```

        can then be used as
        ```
        @MyDecorator(arg1=42, arg2=conf_expr("config.my_arg2"))
        @step
        def start(self):
            pass
        ```
        """
        self._empty_init = True

    def pre_step(
        self, step_name: str, flow: "metaflow.flowspec.FlowSpec"
    ) -> Optional[Callable[["metaflow.flowspec.FlowSpec", Optional[Any]], Any]]:
        """
        This method is called prior to the execution of the step.

        It should return either None to execute anything wrapped by this step decorator
        as usual or a callable that will be called instead.

        Parameters
        ----------
        step_name: str
            The name of the step being decorated.
        flow: FlowSpec
            The flow object to which the step belongs.

        Returns
        -------
        Optional[Callable[FlowSpec, Optional[Any]]]
            An optional function to use instead of the wrapped step. Note that the function
            returned should match the signature of the step being wrapped (join steps
            take an additional "inputs" argument).
        """
        return None

    def post_step(
        self,
        step_name: str,
        flow: "metaflow.flowspec.FlowSpec",
        exception: Optional[Exception] = None,
    ):
        """
        This method is called after the execution of the step. In the case of an
        exception, the exception will also be passed to this method. If the exception
        is not re-raised, the step will be considered successful.

        Parameters
        ----------
        step_name: str
            The name of the step being decorated.
        flow: FlowSpec
            The flow object to which the step belongs.
        exception: Optional[Exception]
            The exception raised during the step execution, if any.
        """
        if exception:
            raise exception

    @property
    def skip_step(self):
        """
        Returns True if this step decorator causes the entire step code (or rather,
        anything this step decorator applies to) to be skipped and not executed.
        """
        return getattr(self, "_skip_step", False)

    @skip_step.setter
    def skip_step(self, value):
        """
        Set the skip_step property to True or False. This is used internally to indicate
        whether the step decorator causes the entire step code (or rather, anything this
        step decorator applies to) to be skipped and not executed.
        """
        self._skip_step = value

    def make_decorator_spec(self):
        attrs = {i: v for i, v in enumerate(self._args) if v is not None}
        attrs.update({k: v for k, v in self._kwargs.items() if v is not None})
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
            return "%s:%s" % (self.decorator_name, attrstr)
        else:
            return self.decorator_name


def step_decorator(*args, **kwargs):
    """
    Use this decorator to transform a generator function into a step decorator.

    As an example:

    ```
    @step_decorator
    def timing(step_name, flow):
        start_time = time.time()
        yield
        end_time = time.time()
        flow.artifact_total_time = end_time - start_time
        print(f"Step {step_name} took {flow.artifact_total_time} seconds")
    ```
    which can then be used as:

    ```
    @timing
    @step
    def start(self):
        print("Hello, world!")
    ```

    You can also optionally specify a name for the decorator
    ```
    @step_decorator(name="timing")
    ...
    ```

    If not specified, the name will be the fully qualified name of the function.

    The function's yield can also yield a callable that will replace whatever is
    being wrapped.

    You can also catch exceptions around the yield statement. Catching and not re-raising
    the exception will make the step successful.

    For more complex use cases, you can use the `StepDecorator` class directly which
    allows more control.

    """
    if args:
        # If we have args, we either had @step_decorator with no argument or we had
        # @step_decorator(name="foo") and transformed it into @step_decorator(step, name="foo")
        obj = args[0]
        name = kwargs.get("name", "%s.%s" % (obj.__module__, obj.__name__))

        if not isinstance(obj, types.FunctionType) or not inspect.isgeneratorfunction(
            obj
        ):
            raise MetaflowException(
                "@step_decorator can only decorate generator functions."
            )

        class WrapClass(StepDecorator):
            _allowed_args = ["name"]
            _step_field = "wrappers"
            decorator_name = name

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._generator = obj

            def pre_step(self, step_name, flow):
                self._generator = self._generator(step_name, flow)
                return self._generator.send(None)

            def post_step(self, step_name, flow, exception=None):
                try:
                    if exception:
                        self._generator.throw(exception)
                    else:
                        self._generator.send(None)
                except StopIteration:
                    pass
                else:
                    raise MetaflowException(
                        "Step decorator '%s' should only yield once"
                        % self.decorator_name
                    )

        return WrapClass
    else:

        def wrap(f):
            return step_decorator(f, **kwargs)

        return wrap


class StepMutator(_StepDecorator):
    _step_field = "config_decorators"
    _allowed_args = True

    def init(self, *args, **kwargs):
        """
        This method is intended to be optionally overridden if you need to
        have an initializer.
        """
        self._empty_init = True

    def mutate(
        self, mutable_step: "metaflow.user_decorators.mutable_step.MutableStep"
    ) -> None:
        raise NotImplementedError()
