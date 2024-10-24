from functools import partial
from typing import Any, Callable, Generator, Optional, TYPE_CHECKING, Tuple, Union

from metaflow.exception import MetaflowException
from metaflow.parameters import Parameter
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    from metaflow.flowspec import _FlowSpecMeta
    from metaflow.decorators import FlowSpecDerived


class MutableStep:
    """
    A MutableStep is a wrapper passed to the `CustomStepDecorator`'s `evaluate` method
    to allow the decorator to interact with the step and providing easy methods to
    modify the behavior of the step.
    """

    def __init__(
        self,
        step: Union[
            Callable[["FlowSpecDerived"], None],
            Callable[["FlowSpecDerived", Any], None],
        ],
    ):
        self._my_step = step

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
                # Check filters
                match_ok = True
                if kwargs:
                    for k, v in kwargs.items():
                        match_ok = k in deco.attributes and deco.attributes[k] == v
                        if match_ok is False:
                            break
                if match_ok:
                    did_remove = True
                else:
                    new_deco_list.append(deco)
            else:
                new_deco_list.append(deco)
            if did_remove and not all:
                break

        self._my_step.decorators = new_deco_list
        return did_remove

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

        self._my_step.decorators.append(
            deco_type(attributes=kwargs, statically_defined=True)
        )


class MutableFlow:
    def __init__(self, flow_spec: "FlowSpec"):
        self._flow_cls = flow_spec

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
    def steps(self) -> Generator[Tuple[str, MutableStep], None, None]:
        """
        Iterate over all the steps in this flow

        Yields
        ------
        Tuple[str, MutableStep]
            A tuple with the step name and the step proxy
        """
        for var in dir(self._flow_cls):
            potential_step = getattr(self._flow_cls, var)
            if callable(potential_step) and hasattr(potential_step, "is_step"):
                yield var, MutableStep(potential_step)

    def __getattr__(self, name):
        # We allow direct access to the steps, configs and parameters but nothing else
        attr = getattr(self._flow_cls, name)
        if attr:
            # Steps
            if callable(attr) and hasattr(attr, "is_step"):
                return MutableStep(attr)
            if name[0] == "_" or name in self._flow_cls._NON_PARAMETERS:
                raise AttributeError(self, name)
            return attr
        raise AttributeError(self, name)


class CustomFlowDecorator:
    def __init__(self, *args, **kwargs):
        from ..flowspec import FlowSpec, _FlowSpecMeta

        if args and isinstance(args[0], (CustomFlowDecorator, _FlowSpecMeta)):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the FlowSpec or another decorator (they
            # can be stacked)
            if isinstance(args[0], _FlowSpecMeta):
                self._set_flow_cls(args[0])
            else:
                self._set_flow_cls(args[0]._flow_cls)
        else:
            # The arguments are actually passed to the init function for this decorator
            self._args = args
            self._kwargs = kwargs

    def __call__(self, flow_spec: Optional["_FlowSpecMeta"] = None) -> "_FlowSpecMeta":
        if flow_spec:
            # This is the case of a decorator @MyDecorator(foo=1, bar=2) and so
            # we already called __init__ and saved foo and bar and are now calling
            # this on the flow itself.
            self.init(*self._args, **self._kwargs)
            return self._set_flow_cls(flow_spec)
        elif not self._flow_cls:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non flow
            raise MetaflowException(
                "A CustomFlowDecorator can only be applied to a FlowSpec"
            )
        return self._flow_cls()

    def _set_flow_cls(self, flow_spec: "_FlowSpecMeta") -> "_FlowSpecMeta":
        from ..flowspec import _FlowState

        flow_spec._flow_state.setdefault(_FlowState.CONFIG_DECORATORS, []).append(self)
        self._flow_cls = flow_spec
        return flow_spec

    def init(self, *args, **kwargs):
        """
        This method is intended to be optionally overridden if you need to
        have an initializer.

        Raises
        ------
            NotImplementedError: If the method is not overridden in a subclass.
        """
        raise NotImplementedError()

    def evaluate(self, mutable_flow: MutableFlow) -> None:
        raise NotImplementedError()


class CustomStepDecorator:
    def __init__(self, *args, **kwargs):
        if args and (
            isinstance(args[0], CustomStepDecorator)
            or callable(args[0])
            and hasattr(args[0], "is_step")
        ):
            # This means the decorator is bare like @MyDecorator
            # and the first argument is the step or another decorator (they
            # can be stacked)
            if isinstance(args[0], CustomStepDecorator):
                self._set_my_step(args[0]._my_step)
            else:
                self._set_my_step(args[0])
        else:
            # The arguments are actually passed to the init function for this decorator
            self._args = args
            self._kwargs = kwargs

    def __call__(
        self,
        step: Optional[
            Union[
                Callable[["FlowSpecDerived"], None],
                Callable[["FlowSpecDerived", Any], None],
            ]
        ] = None,
    ) -> Union[
        Callable[["FlowSpecDerived"], None],
        Callable[["FlowSpecDerived", Any], None],
    ]:
        if step:
            # This is the case of a decorator @MyDecorator(foo=1, bar=2) and so
            # we already called __init__ and saved foo and bar and are now calling
            # this on the step itself.
            self.init(*self._args, **self._kwargs)
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
            Callable[["FlowSpecDerived"], None],
            Callable[["FlowSpecDerived", Any], None],
        ],
    ) -> Union[
        Callable[["FlowSpecDerived"], None],
        Callable[["FlowSpecDerived", Any], None],
    ]:
        from ..flowspec import _FlowState

        self._my_step = step
        # Get the flow
        flow_spec = step.__globals__[step.__qualname__.rsplit(".", 1)[0]]
        flow_spec._flow_state.setdefault(_FlowState.CONFIG_DECORATORS, []).append(self)

        self._flow_cls = flow_spec

    def init(self, *args, **kwargs):
        """
        This method is intended to be optionally overridden if you need to
        have an initializer.

        Raises
        ------
            NotImplementedError: If the method is not overridden in a subclass.
        """
        raise NotImplementedError()

    def evaluate(self, mutable_step: MutableStep) -> None:
        raise NotImplementedError()
