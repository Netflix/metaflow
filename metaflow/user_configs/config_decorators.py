from functools import partial
from typing import Any, Callable, Generator, TYPE_CHECKING, Tuple, Union

from metaflow.exception import MetaflowException
from metaflow.parameters import Parameter
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    from metaflow.flowspec import FlowSpec
    from metaflow.decorators import FlowSpecDerived, StepDecorator


class StepProxy:
    """
    A StepProxy is a wrapper passed to the `StepConfigDecorator`'s `evaluate` method
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


class FlowSpecProxy:
    def __init__(self, flow_spec: "FlowSpec"):
        self._flow_cls = flow_spec

    @property
    def configs(self) -> Generator[Tuple[str, ConfigValue], None, None]:
        """
        Iterate over all user configurations in this flow

        Use this to parameterize your flow based on configuration. As an example, the
        `evaluate` method of your `FlowConfigDecorator` can use this to add an
        environment decorator.
        ```
        class MyDecorator(FlowConfigDecorator):
            def evaluate(flow: FlowSpecProxy):
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
    def steps(self) -> Generator[Tuple[str, StepProxy], None, None]:
        """
        Iterate over all the steps in this flow

        Yields
        ------
        Tuple[str, StepProxy]
            A tuple with the step name and the step proxy
        """
        for var in dir(self._flow_cls):
            potential_step = getattr(self._flow_cls, var)
            if callable(potential_step) and hasattr(potential_step, "is_step"):
                yield var, StepProxy(potential_step)

    def __getattr__(self, name):
        # We allow direct access to the steps, configs and parameters but nothing else
        attr = getattr(self._flow_cls, name)
        if attr:
            # Steps
            if callable(attr) and hasattr(attr, "is_step"):
                return StepProxy(attr)
            if name[0] == "_" or name in self._flow_cls._NON_PARAMETERS:
                raise AttributeError(self, name)
            return attr
        raise AttributeError(self, name)


class FlowConfigDecorator:
    def __call__(self, flow_spec: "FlowSpec") -> "FlowSpec":
        from ..flowspec import _FlowState

        flow_spec._flow_state.setdefault(_FlowState.CONFIG_DECORATORS, []).append(self)
        self._flow_cls = flow_spec
        return flow_spec

    def evaluate(self, flow_proxy: FlowSpecProxy) -> None:
        raise NotImplementedError()


class StepConfigDecorator:
    def __call__(
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

        if not hasattr(step, "is_step"):
            raise MetaflowException(
                "StepConfigDecorators must be applied to a step function"
            )
        self._my_step = step
        # Get the flow
        flow_spec = step.__globals__[step.__qualname__.rsplit(".", 1)[0]]
        flow_spec._flow_state.setdefault(_FlowState.CONFIG_DECORATORS, []).append(self)

        self._flow_cls = flow_spec

        return step

    def evaluate(self, step_proxy: StepProxy) -> None:
        raise NotImplementedError()
