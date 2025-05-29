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
    import metaflow.datastore.inputs
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_step


class UserStepDecoratorMeta(type):
    def __str__(cls):
        return "%s(%s)" % (cls.__name__, cls.decorator_name)


class UserStepDecoratorBase(metaclass=UserStepDecoratorMeta):

    _step_field = None
    _allowed_args = False
    _allowed_kwargs = False
    decorator_name = None

    @classmethod
    def add_or_raise(
        cls,
        step: Union[
            Callable[["metaflow.decorators.FlowSpecDerived"], None],
            Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
        ],
        statically_defined: bool,
        inserted_by: Optional[str] = None,
    ) -> bool:
        if cls.decorator_name not in [
            deco.decorator_name for deco in getattr(step, cls._step_field)
        ]:
            debug.userconf_exec(
                "Adding decorator %s to step %s from %s"
                % (cls, step.__name__, inserted_by)
            )
            cls(step, _statically_defined=statically_defined, _inserted_by=inserted_by)
        else:
            if statically_defined:
                # Prevent circular dep
                from metaflow.decorators import DuplicateStepDecoratorException

                raise DuplicateStepDecoratorException(cls, step)

            # Else we ignore

    def __init__(self, *args, **kwargs):
        if self.decorator_name is None:
            self.decorator_name = f"{self.__module__}.{self.__class__.__name__}"
        arg = None
        self._args = []
        self._kwargs = {}
        # If nothing is set, the user statically defined the decorator
        self._special_kwargs = {"_statically_defined": True, "_inserted_by": None}
        for k, v in kwargs.items():
            if k in ("_statically_defined", "_inserted_by"):
                # These are special arguments that we do not want to pass to the step
                # decorator
                self._special_kwargs[k] = v
            else:
                self._kwargs[k] = v

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
        if args and not self._allowed_args:
            raise MetaflowException("%s does not allow arguments" % str(self))
        if self._kwargs:
            if not self._allowed_kwargs:
                raise MetaflowException("%s does not allow keyword arguments" % self)
            elif isinstance(self._allowed_kwargs, list) and any(
                a not in self._allowed_kwargs for a in self._kwargs
            ):
                raise MetaflowException(
                    "%s only allows the following keyword arguments: %s"
                    % (self, str(self._allowed_args))
                )

        # Store the args so we can use them when we also get the step we are applied
        # to in the __call__ method.
        self._args = args

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
        **kwargs,
    ) -> Union[
        Callable[["metaflow.decorators.FlowSpecDerived"], None],
        Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
    ]:
        # The only kwargs here are just special kwargs (not user facing since those
        # are passed in the constructor)
        self._special_kwargs.update(kwargs)
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
                        "%s is used with arguments "
                        "but does not implement init" % self
                    )
            return self._set_my_step(step)
        elif not self._my_step:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non step
            raise MetaflowException("%s can only be applied to a step function" % self)
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
        # When we set the step, we can now determine if we are statically defined or
        # not. We can't do it much earlier because the decorator itself may be defined
        # (ie: @user_step_decorator is statically defined) but it will only be a static
        # decorator when the user applies it to a step function.
        self.statically_defined = self._special_kwargs["_statically_defined"]
        self.inserted_by = self._special_kwargs["_inserted_by"]

        getattr(self._my_step, self._step_field).append(self)
        return self._my_step

    def __str__(self):
        return str(self.__class__)

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
    _allowed_args = False
    _allowed_kwargs = True
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
        self,
        step_name: str,
        flow: "metaflow.flowspec.FlowSpec",
        inputs: Optional["metaflow.datastore.inputs.Inputs"] = None,
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
        inputs: Optional[List[FlowSpec]]
            The inputs to the step being decorated. This is only provided for join steps
            and is None for all other steps.

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
    def timing(step_name, flow, inputs):
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
        sig = inspect.signature(obj)
        arg_count = len(sig.parameters)
        if kwargs:
            if arg_count != 4:
                raise MetaflowException(
                    "@user_step_decorator(<kwargs>) can only decorate generator "
                    "functions with 4 arguments (step_name, flow, inputs, attributes)"
                )
        elif arg_count not in (3, 4):
            raise MetaflowException(
                "@user_step_decorator can only decorator generator functions with 3 or "
                "4 arguments (step_name, flow, inputs [, attributes])."
            )

        class WrapClass(UserStepDecorator):
            _allowed_args = False
            _allowed_kwargs = True
            _step_field = "wrappers"
            decorator_name = name

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._generator = obj

            def init(self, **kwargs):
                pass

            def pre_step(self, step_name, flow, inputs):
                if arg_count == 4:
                    self._generator = self._generator(
                        step_name, flow, inputs, self._kwargs
                    )
                else:
                    self._generator = self._generator(step_name, flow, inputs)
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
                    raise MetaflowException(" %s should only yield once" % self)

        return WrapClass
    else:
        # Capture arguments passed to user_step_decorator
        def wrap(f):
            return user_step_decorator(f, **kwargs)

        return wrap


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
    _allowed_kwargs = True

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
