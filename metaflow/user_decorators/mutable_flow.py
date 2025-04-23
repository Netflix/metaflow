from functools import partial
from typing import Any, Generator, Tuple, TYPE_CHECKING

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_step


class MutableFlow:
    def __init__(self, flow_spec: "metaflow.flowspec.FlowSpec"):
        self._flow_cls = flow_spec

    @property
    def decorator_specs(self) -> Generator[str, None, None]:
        """
        Iterate over all the decorator specifications of this flow. Note that the same
        type of decorator may be present multiple times and no order is guaranteed.

        Yields
        ------
        str
            A string containing the specification of the decorator (its type and
            arguments)
        """
        for decos in self._flow_cls._flow_decorators.values():
            for deco in decos:
                yield deco.make_decorator_spec()

    @property
    def configs(self) -> Generator[Tuple[str, ConfigValue], None, None]:
        """
        Iterate over all user configurations in this flow

        Use this to parameterize your flow based on configuration. As an example, the
        `mutate` method of your `FlowMutator` can use this to add an
        environment decorator.
        ```
        class MyDecorator(FlowMutator):
            def mutate(flow: MutableFlow):
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
    def steps(
        self,
    ) -> Generator[
        Tuple[str, "metaflow.user_decorators.mutable_step.MutableStep"], None, None
    ]:
        """
        Iterate over all the steps in this flow. The order of the steps
        returned is not guaranteed.

        Yields
        ------
        Tuple[str, MutableStep]
            A tuple with the step name and the step proxy
        """
        from .mutable_step import MutableStep

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
            The decorator class to add to this flow. This decorator can be a Metaflow
            flow decorator.
        TODO: There is no user-defined flow decorator yet. I don't know if we want to
        add one. It has deeper implications to run something pre and post flow and it
        seems easily accomplishable using just the start and end steps so I would lean
        towards not complicating things.
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
            "Flow mutator adding decorator '%s' to flow" % deco_type.name
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
                    "Flow mutator removing decorator '%s' from flow" % deco.name
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

        from .mutable_step import MutableStep

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
