import inspect
import json
import sys
import re
import types

from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

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


class _TrieNode:
    def __init__(self):
        self.children = {}  # type: Dict[str, "_TrieNode"]
        self.total_children = 0
        self.value = None
        self.end_value = None

    def traverse(self, value: type):
        if self.total_children == 0:
            self.end_value = value
        else:
            self.end_value = None
        self.total_children += 1

    def remove_child(self, child_name: str) -> bool:
        if child_name in self.children:
            del self.children[child_name]
            self.total_children -= 1
            return True
        return False


class _ClassPath_Trie:
    def __init__(self):
        self.root = _TrieNode()
        self.inited = False

    def init(self, initial_nodes: Optional[List[Tuple[str, type]]] = None):
        # We need to do this so we can delay import of STEP_DECORATORS
        self.inited = True
        for classpath_name, value in initial_nodes or []:
            self.insert(classpath_name, value)

    def insert(self, classpath_name: str, value: type):
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            node = node.children.setdefault(c, _TrieNode())
            node.traverse(value)
        node.value = value

    def search(self, classpath_name: str) -> Optional[type]:
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            if c not in node.children:
                return None
            node = node.children[c]
        return node.value

    def remove(self, classpath_name: str):
        components = list(reversed(classpath_name.split(".")))

        def _remove(node: _TrieNode, components, depth):
            if depth == len(components):
                if node.value is not None:
                    node.value = None
                    return len(node.children) == 0
                return False
            c = components[depth]
            if c not in node.children:
                return False
            did_delete_child = _remove(node.children[c], components, depth + 1)
            if did_delete_child:
                node.remove_child(c)
                if node.total_children == 1:
                    # If we have one total child left, we have at least one
                    # child and that one has an end_value
                    for child in node.children.values():
                        assert (
                            child.end_value
                        ), "Node with one child must have an end_value"
                        node.end_value = child.end_value
                return node.total_children == 0
            return False

        _remove(self.root, components, 0)

    def unique_prefix_value(self, classpath_name: str) -> Optional[type]:
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            if c not in node.children:
                return None
            node = node.children[c]
        # If we reach here, it means the classpath_name is a prefix.
        # We check if it has only one path forward (end_value will be non None)
        # If value is not None, we also consider this to be a unique "prefix"
        # This happens since this trie is also filled with metaflow default decorators
        return node.end_value or node.value

    def get_unique_prefixes(self) -> Dict[str, type]:
        """
        Get all unique prefixes in the trie.

        Returns
        -------
        List[str]
            A list of unique prefixes.
        """
        to_return = {}

        def _collect(node, current_prefix):
            if node.end_value is not None:
                to_return[current_prefix] = node.end_value
                # We stop there and don't look further since we found the unique prefix
                return
            if node.value is not None:
                to_return[current_prefix] = node.value
                # We continue to look for more unique prefixes
            for child_name, child_node in node.children.items():
                _collect(
                    child_node,
                    f"{current_prefix}.{child_name}" if current_prefix else child_name,
                )

        _collect(self.root, "")
        return {".".join(reversed(k.split("."))): v for k, v in to_return.items()}


class UserStepDecoratorMeta(type):
    _all_registered_decorators = _ClassPath_Trie()
    _do_not_register = set()

    def __new__(mcs, name, bases, namespace, **_kwargs):
        cls = super().__new__(mcs, name, bases, namespace)
        cls.decorator_name = getattr(
            cls, "_decorator_name", f"{cls.__module__}.{cls.__name__}"
        )
        # We inject `METAFLOW_PACKAGE` in the module so that this gets packaged
        effective_module = getattr(cls, "_original_module", cls.__module__)
        if not effective_module.startswith(
            "metaflow."
        ) and not effective_module.startswith("metaflow_extensions."):
            setattr(sys.modules[effective_module], "METAFLOW_PACKAGE", 1)

        if cls.decorator_name in mcs._do_not_register:
            return cls

        # We inject a __init_subclass__ method so we can figure out if there
        # are subclasses. We want to register as decorators only the ones that do
        # not have a subclass. The logic is that everything is registered and if
        # a subclass shows up, we will unregister the parent class leaving only those
        # classes that do not have any subclasses registered.
        @classmethod
        def do_unregister(cls_, **_kwargs):
            for base in cls_.__bases__:
                if isinstance(base, UserStepDecoratorMeta):
                    # If the base is a UserStepDecoratorMeta, we unregister it
                    # so that we don't have any decorators that are not the
                    # most derived one.
                    mcs._all_registered_decorators.remove(base.decorator_name)
                    # Also make sure we don't register again
                    mcs._do_not_register.add(base.decorator_name)

        cls.__init_subclass__ = do_unregister
        mcs._all_registered_decorators.insert(cls.decorator_name, cls)
        return cls

    def __str__(cls):
        return "%s(%s)" % (
            cls.__name__ if cls.__name__ != "WrapClass" else "UserStepDecorator",
            getattr(cls, "decorator_name", None),
        )

    @classmethod
    def all_decorators(mcs) -> Dict[str, "UserStepDecoratorMeta"]:
        """
        Get all registered decorators using the minimally unique classpath name

        Returns
        -------
        Dict[str, UserStepDecoratorBase]
            A dictionary mapping decorator names to their classes.
        """
        mcs._check_init()
        return mcs._all_registered_decorators.get_unique_prefixes()

    @classmethod
    def get_decorator_by_name(
        mcs, decorator_name: str
    ) -> Optional["UserStepDecoratorBase"]:
        """
        Get a decorator by its name.

        Parameters
        ----------
        decorator_name: str
            The name of the decorator to retrieve.

        Returns
        -------
        Optional[UserStepDecoratorBase]
            The decorator class if found, None otherwise.
        """
        mcs._check_init()
        return mcs._all_registered_decorators.unique_prefix_value(decorator_name)

    @classmethod
    def _check_init(mcs):
        # Delay importing STEP_DECORATORS until we actually need it
        if not mcs._all_registered_decorators.inited:
            from metaflow.plugins import STEP_DECORATORS

            mcs._all_registered_decorators.init(
                [
                    (t.name, t)
                    for t in STEP_DECORATORS
                    if not t.name.endswith("_internal")
                ]
            )


class UserStepDecoratorBase(metaclass=UserStepDecoratorMeta):
    _step_field = None
    _allowed_args = False
    _allowed_kwargs = False

    def __init__(self, *args, **kwargs):
        arg = None
        self._args = args
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

        if self._args:
            if isinstance(self._args[0], UserStepDecoratorBase):
                arg = self._args[0]._my_step
            else:
                arg = self._args[0]

        if arg and callable(arg) and hasattr(arg, "is_step"):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the step
            self._set_my_step(arg)
            self._args = args[1:]  # The rest of the args are the decorator args

        if self._args and not self._allowed_args:
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

            return self._set_my_step(step)
        elif not self._my_step:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non step
            raise MetaflowException("%s can only be applied to a step function" % self)
        return self._my_step

    def add_or_raise(
        self,
        step: Union[
            Callable[["metaflow.decorators.FlowSpecDerived"], None],
            Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
        ],
        statically_defined: bool,
        inserted_by: Optional[str] = None,
    ) -> bool:
        if self.decorator_name not in [
            deco.decorator_name for deco in getattr(step, self._step_field)
        ]:
            debug.userconf_exec(
                "Adding decorator %s to step %s from %s"
                % (self, step.__name__, inserted_by)
            )
            self(step, _statically_defined=statically_defined, _inserted_by=inserted_by)
        else:
            if statically_defined:
                # Prevent circular dep
                from metaflow.decorators import DuplicateStepDecoratorException

                raise DuplicateStepDecoratorException(self.__class__, step)

            # Else we ignore

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
        self.init()
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

    @property
    def args(self) -> List[Any]:
        """
        Returns the arguments passed to the decorator.
        """
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        """
        Returns the keyword arguments passed to the decorator.
        """
        return self._kwargs

    def init(self):
        # You can use config values in the arguments to a UserStepDecoratorBase
        # so we resolve those as well
        self._args = [resolve_delayed_evaluator(arg) for arg in self._args]
        self._kwargs, _ = unpack_delayed_evaluator(self._kwargs)
        self._kwargs = {
            k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
        }
        if self._args or self._kwargs:
            if "init" not in self.__class__.__dict__:
                raise MetaflowException(
                    "%s is used with arguments but does not implement init" % self
                )


class UserStepDecorator(UserStepDecoratorBase):
    _step_field = "wrappers"
    _allowed_args = False
    _allowed_kwargs = True

    def init(self):
        """
        Implement this method if your UserStepDecorator takes arguments. It replaces the
        __init__ method in traditional Python classes.

        Note that you need to use self.args and self.kwargs to access the arguments.
        You should also use super().init() as the first line of your method.

        As an example:
        ```
        class MyDecorator(UserStepDecorator):
            def init(self):
                self.arg1 = self.kwargs.get("arg1", None)
                self.arg2 = self.kwargs.get("arg2", None)
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
        super().init()

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
        # If we have args, we either had @user_step_decorator with no argument or we had
        # @user_step_decorator(arg="foo") and transformed it into
        # @user_step_decorator(step, arg="foo")
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
            _decorator_name = name
            _original_module = obj.__module__

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._generator = obj

            def init(self):
                super().init()
                if self.args:
                    raise MetaflowException(
                        "%s does not allow arguments, only keyword arguments"
                        % str(self)
                    )

            def pre_step(self, step_name, flow, inputs):
                if arg_count == 4:
                    self._generator = self._generator(
                        step_name, flow, inputs, self.kwargs
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

    def init(self):
        """
        Implement this method if you wish for your StepMutator to take in arguments.

        Note that you need to use self.args and self.kwargs to access the arguments.
        You should also use super().init() as the first line of your method.

        Your step-mutator can then look like:

        @MyMutator(arg1, arg2)
        @step
        def my_step(self):
            pass

        It is an error to use your mutator with arguments but not implement this method.
        """
        super().init()

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
