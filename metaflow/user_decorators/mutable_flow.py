from functools import partial
from typing import Generator, Optional, Tuple, TYPE_CHECKING, Union

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.parameters
    import metaflow.user_decorators.mutable_step


class MutableFlow:
    def __init__(
        self,
        flow_spec: "metaflow.flowspec.FlowSpec",
        pre_mutate: bool = False,
        statically_defined: bool = False,
        inserted_by: Optional[str] = None,
    ):
        self._flow_cls = flow_spec
        self._pre_mutate = pre_mutate
        self._statically_defined = statically_defined
        self._inserted_by = inserted_by
        if self._inserted_by is None:
            # This is an error because MutableSteps should only be created by
            # StepMutators or FlowMutators. We need to catch it now because otherwise
            # we may put stuff on the command line (with --with) that would get added
            # twice and weird behavior may ensue.
            raise MetaflowException(
                "MutableFlow should only be created by StepMutators or FlowMutators. "
                "This is an internal error."
            )

    @property
    def decorator_specs(self) -> Generator[str, None, None]:
        """
        Iterate over all the decorator specifications of this flow. Note that the same
        type of decorator may be present multiple times and no order is guaranteed.
        A decorator specification has the following format:
        `<decorator_name>:<arg1>=<value1>,<arg2>=<value2>,...`.

        You can use the decorator specification to remove a decorator from the flow
        for example.

        Yields
        ------
        str
            A string representing the decorator.
        """
        for decos in self._flow_cls._flow_decorators.values():
            for deco in decos:
                r = deco.make_decorator_spec()
                debug.userconf_exec(
                    "Mutable flow yielding flow decorator specification: %s" % r
                )
                yield r

    @property
    def configs(self) -> Generator[Tuple[str, ConfigValue], None, None]:
        """
        Iterate over all user configurations in this flow

        Use this to parameterize your flow based on configuration. As an example, the
        `pre_mutate`/`mutate` methods can add decorators to steps in the flow that
        depend on values in the configuration.

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
            r = name, ConfigValue(value)
            debug.userconf_exec("Mutable flow yielding config: %s" % str(r))
            yield r

    @property
    def parameters(
        self,
    ) -> Generator[Tuple[str, "metaflow.parameters.Parameter"], None, None]:
        """
        Iterate over all the parameters in this flow.

        Yields
        ------
        Tuple[str, Parameter]
            Name of the parameter and parameter in the flow
        """
        for var, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            debug.userconf_exec(
                "Mutable flow yielding parameter: %s" % str((var, param))
            )
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
                debug.userconf_exec("Mutable flow yielding step: %s" % var)
                yield var, MutableStep(
                    self._flow_cls,
                    potential_step,
                    pre_mutate=self._pre_mutate,
                    statically_defined=self._statically_defined,
                    inserted_by=self._inserted_by,
                )

    def add_parameter(
        self, name: str, value: "metaflow.parameters.Parameter", overwrite: bool = False
    ) -> None:
        """
        Add a parameter to the flow. You can only add parameters in the `pre_mutate`
        method.

        Parameters
        ----------
        name : str
            Name of the parameter
        value : Parameter
            Parameter to add to the flow
        overwrite : bool, default False
            If True, overwrite the parameter if it already exists
        """
        if not self._pre_mutate:
            raise MetaflowException(
                "Adding parameter '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method" % (name, self._inserted_by)
            )
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
        debug.userconf_exec("Mutable flow adding parameter %s to flow" % name)
        setattr(self._flow_cls, name, value)

    def remove_parameter(self, parameter_name: str) -> bool:
        """
        Remove a parameter from the flow.

        The name given should match the name of the parameter (can be different
        from the name of the parameter in the flow. You can not remove config parameters.
        You can only remove parameters in the `pre_mutate` method.

        Parameters
        ----------
        parameter_name : str
            Name of the parameter

        Returns
        -------
        bool
            Returns True if the parameter was removed
        """
        if not self._pre_mutate:
            raise MetaflowException(
                "Removing parameter '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method"
                % (parameter_name, self._inserted_by)
            )
        from metaflow.flowspec import _FlowState

        for var, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            if param.name == parameter_name:
                delattr(self._flow_cls, var)
                debug.userconf_exec(
                    "Mutable flow removing parameter %s from flow" % var
                )
                # Reset so that we don't list it again
                del self._flow_cls._flow_state[_FlowState.CACHED_PARAMETERS]
                return True
        debug.userconf_exec(
            "Mutable flow failed to remove parameter %s from flow" % parameter_name
        )
        return False

    def add_decorator(self, deco_type: Union[partial, str], **kwargs) -> None:
        """
        Add a Metaflow flow-decorator to a flow. You can only add decorators in the
        `pre_mutate` method.

        You can add either a Metaflow decorator directly or its decorator
        specification for it (the same you would get back from decorator_specs).
        As an example:
        ```
        from metaflow import project

        ...
        my_flow.add_decorator(project, name="my_project")
        ```

        is equivalent to:
        ```
        my_flow.add_decorator("project:name=my_project")
        ```

        Note in the later case, there is no need to import the flow decorator.

        The latter syntax is useful to, for example, allow decorators to be stored as
        strings in a configuration file.

        Parameters
        ----------
        deco_type : Union[partial, str]
            The decorator class to add to this flow. If using a string, you cannot specify
            additional arguments as all argument will be parsed from the decorator
            specification.
        """
        if not self._pre_mutate:
            raise MetaflowException(
                "Adding flow-decorator '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method"
                % (
                    deco_type if isinstance(deco_type, str) else deco_type.name,
                    self._inserted_by,
                )
            )
        # Prevent circular import
        from metaflow.decorators import (
            DuplicateFlowDecoratorException,
            FlowDecorator,
            extract_flow_decorator_from_decospec,
        )

        # If deco_type is a string, we want to parse it to a decospec
        if isinstance(deco_type, str):
            if kwargs:
                raise MetaflowException(
                    "Cannot specify additional arguments when adding a flow decorator "
                    "using a decospec"
                )
            flow_deco = extract_flow_decorator_from_decospec(deco_type)
            flow_deco.statically_defined = self._statically_defined
            flow_deco.inserted_by = self._inserted_by
            # Check duplicates
            if (
                flow_deco.name in self._flow_cls._flow_decorators
                and not flow_deco.allow_multiple
            ):
                raise DuplicateFlowDecoratorException(flow_deco.name)
            debug.userconf_exec(
                "Mutable flow adding decorator '%s' to flow (from str)" % flow_deco.name
            )
            self._flow_cls._flow_decorators.setdefault(flow_deco.name, []).append(
                flow_deco
            )
            return

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
            deco_type(
                attributes=kwargs,
                statically_defined=self._statically_defined,
                inserted_by=self._inserted_by,
            )
        )
        debug.userconf_exec(
            "Mutable flow adding decorator '%s' to flow" % deco_type.name
        )

    def remove_decorator(self, deco_spec: str) -> bool:
        """
        Remove a flow-level decorator. To remove a decorator, you can pass the decorator
        specification (obtained from `decorator_specs` for example).
        Note that if multiple decorators share the same decorator specification
        (very rare), they will all be removed.

        You can only remove decorators in the `pre_mutate` method.

        Parameters
        ----------
        deco_spec : str
            Decorator specification of the decorator to remove.

        Returns
        -------
        bool
            Returns True if a decorator was removed.
        """

        if not self._pre_mutate:
            raise MetaflowException(
                "Removing flow-decorator '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method" % (deco_spec, self._inserted_by)
            )

        splits = deco_spec.split(":", 1)
        deconame = splits[0]
        new_deco_list = []
        old_deco_list = self._flow_cls._flow_decorators.get(deconame)
        if old_deco_list is None:
            debug.userconf_exec(
                "Mutable flow failed to remove decorator '%s' from flow (non present)"
                % deco_spec
            )
            return False

        did_remove = False
        for deco in old_deco_list:
            # Get the decospec and compare with the one passed in
            to_compare_spec = deco.make_decorator_spec()
            if to_compare_spec == deco_spec:
                did_remove = True
                debug.userconf_exec(
                    "Mutable flow removing decorator '%s' from flow" % deco.name
                )
            else:
                new_deco_list.append(deco)
        debug.userconf_exec(
            "Mutable flow removed %d decorators from flow"
            % (len(old_deco_list) - len(new_deco_list))
        )
        if new_deco_list:
            self._flow_cls._flow_decorators[deconame] = new_deco_list
        else:
            del self._flow_cls._flow_decorators[deconame]
        return did_remove

    def __getattr__(self, name):
        # We allow direct access to the steps, configs and parameters but nothing else
        from metaflow.parameters import Parameter

        from .mutable_step import MutableStep

        attr = getattr(self._flow_cls, name)
        if attr:
            # Steps
            if callable(attr) and hasattr(attr, "is_step"):
                return MutableStep(
                    self._flow_cls,
                    attr,
                    pre_mutate=self._pre_mutate,
                    statically_defined=self._statically_defined,
                    inserted_by=self._inserted_by,
                )
            if name[0] == "_" or name in self._flow_cls._NON_PARAMETERS:
                raise AttributeError(self, name)
            if isinstance(attr, (Parameter, ConfigValue)):
                return attr
        raise AttributeError(self, name)
