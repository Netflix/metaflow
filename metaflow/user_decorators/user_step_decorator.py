import inspect
import json
import re
import types

from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Union

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import (
    resolve_delayed_evaluator,
    unpack_delayed_evaluator,
)

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_step


class UserStepDecoratorBase:

    _step_field = None
    _allowed_args = False
    decorator_name = None

    def __init__(self, *args, **kwargs):
        if self.decorator_name is None:
            self.decorator_name = f"{self.__module__}.{self.__class__.__name__}"
        arg = None
        self._args = []
        self._kwargs = {}
        self.statically_defined = False

        if args:
            if isinstance(args[0], UserStepDecoratorBase):
                arg = args[0]._my_step
            else:
                arg = args[0]
        if arg and callable(arg) and hasattr(arg, "is_step"):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the step
            self._set_my_step(arg)
            return
        if kwargs:
            if not self._allowed_args:
                raise MetaflowException(
                    "Decorator '%s' does not allow arguments" % str(self)
                )
            elif isinstance(self._allowed_args, list) and (
                len(args) != 0 or any(a not in self._allowed_args for a in kwargs)
            ):
                raise MetaflowException(
                    "Decorator '%s' only allows the following keyword arguments: %s"
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
        # This is *not* called for something like:
        # @MyStepDecorator()
        # @step
        # def my_step(self):
        #     pass
        # because in that case, we will have called __call__ below and that already
        # returns a function and that __get__ function will be called.

        return self().__get__(instance, owner)

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
            if isinstance(step, UserStepDecoratorBase):
                step = step._my_step
            # You can use config values in the arguments to a _StepDecorator
            # so we resolve those as well
            self._args = [resolve_delayed_evaluator(arg) for arg in self._args]
            self._kwargs, _ = unpack_delayed_evaluator(self._kwargs)
            self._kwargs = {
                k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
            }
            if self._args or self._kwargs:
                self.init(*self._args, **self._kwargs)
                if hasattr(self, "_empty_init"):
                    raise MetaflowException(
                        "Decorator '%s' is used with arguments "
                        "but does not implement init" % self.decorator_name
                    )
            return self._set_my_step(step)
        elif not self._my_step:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non step
            raise MetaflowException(
                "Decorator '%s' can only be applied to a step function"
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
                "UserStepDecorator is not properly overloaded; missing _step_field. "
                "This is a Metaflow bug, please contact support."
            )
        getattr(self._my_step, self._step_field).append(self)
        return self._my_step

    @classmethod
    def parse_decorator_spec(cls, deco_spec: str) -> Optional["UserStepDecoratorBase"]:
        if len(deco_spec) == 0:
            return cls()

        args = []
        kwargs = {}
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
            try:
                pos = int(name)
            except ValueError:
                kwargs[name.strip()] = val_parsed
            else:
                # Extend args list if needed to accommodate position
                while len(args) <= pos:
                    args.append(None)
                args[pos] = val_parsed
        debug.userconf_exec(
            "Parsed decorator spec for %s: %s"
            % (cls.decorator_name, str((args, kwargs)))
        )
        return cls(*args, **kwargs)

    def make_decorator_spec(self):
        attrs = {}
        if self._args:
            attrs.update({i: v for i, v in enumerate(self._args) if v is not None})
        if self._kwargs:
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

    def init(self, *args, **kwargs):
        # Allows us to catch issues where the user passes arguments to a decorator
        # but doesn't tell us how to handle them.
        self._empty_init = True


class UserStepDecorator(UserStepDecoratorBase):
    _step_field = "wrappers"
    _allowed_args = True
    decorator_name: Optional[str] = None

    def init(self, **kwargs):
        """
        Implement this method if your UserStepDecorator takes arguments. It replaces the
        __init__ method in traditional Python classes.

        As an example:
        ```
        class MyDecorator(UserStepDecorator):
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
        Implement this method to perform any action prior to the execution of a step.

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
        Implement this method to perform any action after the execution of a step.

        If the step (or any code being wrapped by this decorator) raises an exception,
        it will be passed here and can either be caught (in which case the step will
        be considered as successful) or re-raised (in which case the entire step
        will be considered a failure unless another decorator catches the execption).

        Note that this method executes *before* artifacts are stored in the datastore
        so it is able to modify, add or remove artifacts from `flow`.

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
    def skip_step(self) -> Union[bool, Dict[str, Any]]:
        """
        Returns whether or not the step (or rather anything wrapped by this decorator)
        should be skipped

        Returns
        -------
        Union[bool, Dict[str, Any]]
            False if the step should not be skipped. True if it should be skipped and
            a dictionary if it should be skipped and the values passed in used as
            the arguments to the self.next call.
        """
        return getattr(self, "_skip_step", False)

    @skip_step.setter
    def skip_step(self, value: Union[bool, Dict[str, Any]]):
        """
        Set the skip_step property. You can set it to:
          - True to skip the step
          - False to not skip the step (default)
          - A dictionary with the keys valid in the `self.next` call.

        Parameters
        ----------
        value: Union[bool, Dict[str, Any]]
            True/False or a dictionary with the keys valid in the `self.next` call.
        """
        self._skip_step = value


def user_step_decorator(*args, **kwargs):
    """
    Use this decorator to transform a generator function into a user step decorator.

    As an example:

    ```
    @user_step_decorator
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

    Your generator should:
      - yield once and only once
      - yield either None or a callable that will replace whatever is being wrapped (it
        should have the same signature as the wrapped function)

    You are able to catch exceptions thrown by the yield statement (ie: coming from the
    wrapped code). Catching and not re-raising the exception will make the step
    successful.

    Note that you are able to modify the step's artifact after the yield.

    For more complex use cases, you can use the `UserStepDecorator` class directly which
    allows more control.
    """
    if args:
        # If we have args, we either had @step_decorator with no argument or we had
        # @step_decorator(arg="foo") and transformed it into @step_decorator(step, arg="foo")
        obj = args[0]
        name = f"{obj.__module__}.{obj.__name__}"

        if not isinstance(obj, types.FunctionType) or not inspect.isgeneratorfunction(
            obj
        ):
            raise MetaflowException(
                "@user_step_decorator can only decorate generator functions."
            )

        class WrapClass(UserStepDecorator):
            _allowed_args = False
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
                        "User step decorator '%s' should only yield once"
                        % self.decorator_name
                    )

        return WrapClass
    else:
        raise MetaflowException("@user_step_decorator does not take any argument")


class StepMutator(UserStepDecoratorBase):
    """
    Derive from this class to implement a step mutator.

    A step mutator allows you to introspect a step and add decorators to it. You can
    use values available through configurations to determine how to mutate the step.

    There are two main methods provided:
      - pre_mutate: called as early as possible right after configuration values are read.
      - mutate: called right after all the command line is parsed but before any
        Metaflow decorators are applied.
    """

    _step_field = "config_decorators"
    _allowed_args = True

    def init(self, *args, **kwargs):
        """
        Implement this method if you wish for your StepMutator to take in arguments.

        Your step-mutator can then look like:

        @MyMutator(arg1, arg2)
        @step
        def my_step(self):
            pass

        It is an error to use your mutator with arguments but not implement this method.

        When implementing, you should not call super().init().
        """
        self._empty_init = True

    def pre_mutate(
        self, mutable_step: "metaflow.user_decorators.mutable_step.MutableStep"
    ) -> None:
        """
        Method called right after all configuration values are read.

        Parameters
        ----------
        mutable_step : metaflow.user_decorators.mutable_step.MutableStep
            A representation of this step
        """
        return None

    def mutate(
        self, mutable_step: "metaflow.user_decorators.mutable_step.MutableStep"
    ) -> None:
        """
        Method called right before the first Metaflow decorator is applied. This
        means that the command line, including all `--with` options has been parsed.

        Parameters
        ----------
        mutable_step : metaflow.user_decorators.mutable_step.MutableStep
            A representation of this step
        """
        return None
