from functools import partial
from typing import Any, Callable, Generator, Optional, TYPE_CHECKING, Tuple, Union

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import (
    ConfigValue,
    resolve_delayed_evaluator,
)

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.parameters


class MutableStep:
    """
    A MutableStep is a wrapper passed to the `CustomStepDecorator`'s `evaluate` method
    to allow the decorator to interact with the step and providing easy methods to
    modify the behavior of the step.
    """

    def __init__(
        self,
        flow_spec: "metaflow.flowspec.FlowSpec",
        step: Union[
            Callable[["metaflow.decorators.FlowSpecDerived"], None],
            Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
        ],
    ):
        self._mutable_container = MutableFlow(flow_spec)
        self._my_step = step

    @property
    def flow(self) -> "MutableFlow":
        """
        The flow that contains this step

        Returns
        -------
        MutableFlow
            The flow that contains this step
        """
        return self._mutable_container

    @property
    def decorators(self) -> Generator["metaflow.decorators.StepDecorator", None, None]:
        """
        Iterate over all the decorators of this step. Note that the same type of decorator
        may be present multiple times and no order is guaranteed.

        Yields
        ------
        metaflow.decorators.StepDecorator
            A decorator of the step
        """
        for deco in self._my_step.decorators:
            yield deco

    def add_decorator(self, deco_type: partial, **kwargs) -> None:
        """
        Add a Metaflow decorator to a step.

        Parameters
        ----------
        deco_type : partial
            The decorator class to add to this step
        """
        # Prevent circular import
        from metaflow.decorators import DuplicateStepDecoratorException, StepDecorator

        # Validate deco_type
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], StepDecorator)
        ):
            raise TypeError("add_decorator takes a StepDecorator")

        deco_type = deco_type.args[0]
        if (
            deco_type.name in [deco.name for deco in self._my_step.decorators]
            and not deco_type.allow_multiple
        ):
            raise DuplicateStepDecoratorException(deco_type.name, self._my_step)

        debug.userconf_exec(
            "Mutable decorator adding step decorator %s to step %s"
            % (deco_type.name, self._my_step.name)
        )
        self._my_step.decorators.append(
            deco_type(attributes=kwargs, statically_defined=True)
        )

    def remove_decorator(self, deco_name: str, all: bool = True, **kwargs) -> bool:
        """
        Remove one or more Metaflow decorators from a step.

        Some decorators can be applied multiple times to a step. This method allows you
        to choose which decorator to remove or just remove all of them or one of them.

        Parameters
        ----------
        deco_name : str
            Name of the decorator to remove
        all : bool, default True
            If True, remove all instances of the decorator that match the filters
            passed using kwargs (or all the instances of the decorator if no filters are
            passed). If False, removes only the first found instance of the decorator.

        Returns
        -------
        bool
            Returns True if at least one decorator was removed.
        """
        new_deco_list = []
        did_remove = False
        for deco in self._my_step.decorators:
            if deco.name == deco_name:
                # Evaluate all the configuration values if any
                deco.init()

                # Check filters
                match_ok = True
                if kwargs:
                    for k, v in kwargs.items():
                        match_ok = k in deco.attributes and deco.attributes[k] == v
                        if match_ok is False:
                            break
                if match_ok:
                    did_remove = True
                    debug.userconf_exec(
                        "Mutable decorator removing step decorator %s from step %s"
                        % (deco.name, self._my_step.name)
                    )
                else:
                    new_deco_list.append(deco)
            else:
                new_deco_list.append(deco)
            if did_remove and not all:
                break

        self._my_step.decorators = new_deco_list
        return did_remove


class MutableFlow:
    def __init__(self, flow_spec: "metaflow.flowspec.FlowSpec"):
        self._flow_cls = flow_spec

    @property
    def decorators(self) -> Generator["metaflow.decorators.FlowDecorator", None, None]:
        """
        Iterate over all the decorators of this flow. Note that the same type of decorator
        may be present multiple times and no order is guaranteed.

        Yields
        ------
        metaflow.decorators.FlowDecorator
            A decorator of the flow
        """
        for decos in self._flow_cls._flow_decorators.values():
            for deco in decos:
                yield deco

    @property
    def configs(self) -> Generator[Tuple[str, ConfigValue], None, None]:
        """
        Iterate over all user configurations in this flow

        Use this to parameterize your flow based on configuration. As an example, the
        `evaluate` method of your `CustomFlowDecorator` can use this to add an
        environment decorator.
        ```
        class MyDecorator(CustomFlowDecorator):
            def evaluate(flow: MutableFlow):
                val = next(flow.configs)[1].steps.start.cpu
                flow.start.add_decorator(environment, vars={'mycpu': val})
                return flow

        @MyDecorator()
        class TestFlow(FlowSpec):
            config = Config('myconfig.json')

            @step
            def start(self):
                pass
        ```
        can be used to add an environment decorator to the `start` step.

        Yields
        ------
        Tuple[str, ConfigValue]
            Iterates over the configurations of the flow
        """
        from metaflow.flowspec import _FlowState

        # When configs are parsed, they are loaded in _flow_state[_FlowState.CONFIGS]
        for name, value in self._flow_cls._flow_state.get(
            _FlowState.CONFIGS, {}
        ).items():
            yield name, ConfigValue(value)

    @property
    def parameters(self) -> Generator[Tuple[str, Any], None, None]:
        for var, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            yield var, param

    @property
    def steps(self) -> Generator[Tuple[str, MutableStep], None, None]:
        """
        Iterate over all the steps in this flow. The order of the steps
        returned is not guaranteed.

        Yields
        ------
        Tuple[str, MutableStep]
            A tuple with the step name and the step proxy
        """
        for var in dir(self._flow_cls):
            potential_step = getattr(self._flow_cls, var)
            if callable(potential_step) and hasattr(potential_step, "is_step"):
                yield var, MutableStep(self._flow_cls, potential_step)

    def add_parameter(
        self, name: str, value: "metaflow.parameters.Parameter", overwrite: bool = False
    ) -> None:
        from metaflow.parameters import Parameter

        if hasattr(self._flow_cls, name) and not overwrite:
            raise MetaflowException(
                "Flow '%s' already has a class member '%s' -- "
                "set overwrite=True in add_parameter to overwrite it."
                % (self._flow_cls.__name__, name)
            )
        if not isinstance(value, Parameter) or value.IS_CONFIG_PARAMETER:
            raise MetaflowException(
                "Only a Parameter or an IncludeFile can be added using `add_parameter`"
                "; got %s" % type(value)
            )
        debug.userconf_exec("Mutable flow decorator adding parameter %s to flow" % name)
        setattr(self._flow_cls, name, value)

    def remove_parameter(self, parameter_name: str) -> bool:
        """
        Remove a parameter from the flow.

        The name given should match the name of the parameter (can be different
        from the name of the parameter in the flow. You can not remove config parameters.

        Parameters
        ----------
        parameter_name : str
            Name of the parameter

        Returns
        -------
        bool
            Returns True if the parameter was removed
        """
        from metaflow.flowspec import _FlowState

        for var, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            if param.name == parameter_name:
                delattr(self._flow_cls, var)
                debug.userconf_exec(
                    "Mutable flow decorator removing parameter %s from flow" % var
                )
                # Reset so that we don't list it again
                del self._flow_cls._flow_state[_FlowState.CACHED_PARAMETERS]
                return True
        return False

    def add_decorator(self, deco_type: partial, **kwargs) -> None:
        """
        Add a Metaflow decorator to a flow.

        Parameters
        ----------
        deco_type : partial
            The decorator class to add to this flow
        """
        # Prevent circular import
        from metaflow.decorators import DuplicateFlowDecoratorException, FlowDecorator

        # Validate deco_type
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], FlowDecorator)
        ):
            raise TypeError("add_decorator takes a FlowDecorator")

        deco_type = deco_type.args[0]
        if (
            deco_type.name in self._flow_cls._flow_decorators
            and not deco_type.allow_multiple
        ):
            raise DuplicateFlowDecoratorException(deco_type.name)

        self._flow_cls._flow_decorators.setdefault(deco_type.name, []).append(
            deco_type(attributes=kwargs, statically_defined=True)
        )
        debug.userconf_exec(
            "Mutable flow decorator adding decorator %s to flow" % deco_type.name
        )

    def remove_decorator(self, deco_name: str, all: bool = True, **kwargs) -> bool:
        """
        Remove one or more Metaflow decorators from a flow.

        Some decorators can be applied multiple times to a flow. This method allows you
        to choose which decorator to remove or just remove all of them or one of them.

        Parameters
        ----------
        deco_name : str
            Name of the decorator to remove
        all : bool, default True
            If True, remove all instances of the decorator that match the filters
            passed using kwargs (or all the instances of the decorator if no filters are
            passed). If False, removes only the first found instance of the decorator.

        Returns
        -------
        bool
            Returns True if at least one decorator was removed.
        """
        new_deco_list = []
        old_deco_list = self._flow_cls._flow_decorators.get(deco_name)
        if old_deco_list is None:
            return False

        did_remove = False
        for deco in old_deco_list:
            # Evaluate all the configuration values if any
            deco.init()

            # Check filters
            match_ok = True
            if kwargs:
                for k, v in kwargs.items():
                    match_ok = k in deco.attributes and deco.attributes[k] == v
                    if match_ok is False:
                        break
            if match_ok:
                did_remove = True
                debug.userconf_exec(
                    "Mutable flow decorator removing decorator %s from flow" % deco.name
                )
            else:
                new_deco_list.append(deco)
            if did_remove and not all:
                break

        if new_deco_list:
            self._flow_cls._flow_decorators[deco_name] = new_deco_list
        else:
            del self._flow_cls._flow_decorators[deco_name]
        return did_remove

    def __getattr__(self, name):
        # We allow direct access to the steps, configs and parameters but nothing else
        from metaflow.parameters import Parameter

        attr = getattr(self._flow_cls, name)
        if attr:
            # Steps
            if callable(attr) and hasattr(attr, "is_step"):
                return MutableStep(self._flow_cls, attr)
            if name[0] == "_" or name in self._flow_cls._NON_PARAMETERS:
                raise AttributeError(self, name)
            if isinstance(attr, (Parameter, ConfigValue)):
                return attr
        raise AttributeError(self, name)


class CustomFlowDecorator:
    def __init__(self, *args, **kwargs):
        from ..flowspec import FlowSpecMeta

        if args and isinstance(args[0], (CustomFlowDecorator, FlowSpecMeta)):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the FlowSpec or another decorator (they
            # can be stacked)

            # If we have a init function, we call it with no arguments -- this can
            # happen if the user defines a function with default parameters for example
            try:
                self.init()
            except NotImplementedError:
                pass

            # Now set the flow class we apply to
            if isinstance(args[0], FlowSpecMeta):
                self._set_flow_cls(args[0])
            else:
                self._set_flow_cls(args[0]._flow_cls)
        else:
            # The arguments are actually passed to the init function for this decorator
            self._args = args
            self._kwargs = kwargs

    def __get__(self, instance, owner):
        # Required so that we "present" as a FlowSpec when the flow decorator is
        # of the form
        # @MyFlowDecorator
        # class MyFlow(FlowSpec):
        #     pass
        #
        # In that case, if we don't have __get__, the object is a CustomFlowDecorator
        # and not a FlowSpec. This is more critical for steps (and CustomStepDecorator)
        # because other parts of the code rely on steps having is_step. There are
        # other ways to solve this but this allowed for minimal changes going forward.
        return self()

    def __call__(
        self, flow_spec: Optional["metaflow.flowspec.FlowSpecMeta"] = None
    ) -> "metaflow.flowspec.FlowSpecMeta":
        if flow_spec:
            # This is the case of a decorator @MyDecorator(foo=1, bar=2) and so
            # we already called __init__ and saved foo and bar in self._args and
            # self._kwargs and are now calling this on the flow itself.

            # You can use config values in the arguments to a CustomFlowDecorator
            # so we resolve those as well
            new_args = [resolve_delayed_evaluator(arg) for arg in self._args]
            new_kwargs = {
                k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
            }
            self.init(*new_args, **new_kwargs)
            if hasattr(self, "_empty_init"):
                raise MetaflowException(
                    "CustomFlowDecorator '%s' is used with arguments "
                    "but does not implement init" % str(self.__class__)
                )

            return self._set_flow_cls(flow_spec)
        elif not self._flow_cls:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non flow
            raise MetaflowException(
                "A CustomFlowDecorator can only be applied to a FlowSpec"
            )
        return self._flow_cls()

    def _set_flow_cls(
        self, flow_spec: "metaflow.flowspec.FlowSpecMeta"
    ) -> "metaflow.flowspec.FlowSpecMeta":
        from ..flowspec import _FlowState

        flow_spec._flow_state.setdefault(_FlowState.CONFIG_DECORATORS, []).append(self)
        self._flow_cls = flow_spec
        return flow_spec

    def init(self, *args, **kwargs):
        """
        This method is intended to be optionally overridden if you need to
        have an initializer.
        """
        self._empty_init = True

    def evaluate(self, mutable_flow: MutableFlow) -> None:
        """
        Implement this method to act on the flow and modify it as needed.

        Parameters
        ----------
        mutable_flow : MutableFlow
            Flow

        Raises
        ------
        NotImplementedError
            _description_
        """
        raise NotImplementedError()


class CustomStepDecorator:
    def __init__(self, *args, **kwargs):
        arg = None
        if args:
            if isinstance(args[0], CustomStepDecorator):
                arg = args[0]._my_step
            else:
                arg = args[0]
        if arg and callable(arg) and hasattr(arg, "is_step"):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the step
            self._set_my_step(arg)
        else:
            # The arguments are actually passed to the init function for this decorator
            self._args = args
            self._kwargs = kwargs

    def __get__(self, instance, owner):
        # See explanation in CustomFlowDecorator.__get__
        return self()

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
            # You can use config values in the arguments to a CustomFlowDecorator
            # so we resolve those as well
            new_args = [resolve_delayed_evaluator(arg) for arg in self._args]
            new_kwargs = {
                k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
            }
            self.init(*new_args, **new_kwargs)
            if hasattr(self, "_empty_init"):
                raise MetaflowException(
                    "CustomStepDecorator '%s' is used with arguments "
                    "but does not implement init" % str(self.__class__)
                )
            return self._set_my_step(step)
        elif not self._my_step:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non step
            raise MetaflowException(
                "A CustomStepDecorator can only be applied to a step function"
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
        self._my_step.config_decorators.append(self)
        return self._my_step

    def init(self, *args, **kwargs):
        """
        This method is intended to be optionally overridden if you need to
        have an initializer.
        """
        self._empty_init = True

    def evaluate(self, mutable_step: MutableStep) -> None:
        raise NotImplementedError()
