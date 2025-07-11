from functools import partial
from typing import Any, Dict, Generator, List, Optional, Tuple, TYPE_CHECKING, Union

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.parameters
    import metaflow.user_decorators.mutable_step


class MutableFlow:
    IGNORE = 1
    ERROR = 2
    OVERRIDE = 3

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
    def decorator_specs(
        self,
    ) -> Generator[Tuple[str, str, List[Any], Dict[str, Any]], None, None]:
        """
        Iterate over all the decorator specifications of this flow. Note that the same
        type of decorator may be present multiple times and no order is guaranteed.

        The returned tuple contains:
        - The decorator's name (shortest possible)
        - The decorator's fully qualified name (in the case of Metaflow decorators, this
          will indicate which extension the decorator comes from)
        - A list of positional arguments
        - A dictionary of keyword arguments

        You can use the decorator specification to remove a decorator from the flow
        for example.

        Yields
        ------
        str, str, List[Any], Dict[str, Any]
            A tuple containing the decorator name, it's fully qualified name,
            a list of positional arguments, and a dictionary of keyword arguments.
        """
        for decos in self._flow_cls._flow_decorators.values():
            for deco in decos:
                # 3.7 does not support yield foo, *bar syntax so we
                # work around

                r = [
                    deco.name,
                    "%s.%s"
                    % (
                        deco.__class__.__module__,
                        deco.__class__.__name__,
                    ),
                ]
                r.extend(deco.get_args_kwargs())
                yield tuple(r)

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
                % (parameter_name, " from ".join(self._inserted_by))
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

    def add_decorator(
        self,
        deco_type: Union[partial, str],
        deco_args: Optional[List[Any]] = None,
        deco_kwargs: Optional[Dict[str, Any]] = None,
        duplicates: int = IGNORE,
    ) -> None:
        """
        Add a Metaflow flow-decorator to a flow. You can only add decorators in the
        `pre_mutate` method.

        You can either add the decorator itself or its decorator specification for it
        (the same you would get back from decorator_specs). You can also mix and match
        but you cannot provide arguments both through the string and the
        deco_args/deco_kwargs.

        As an example:
        ```
        from metaflow import project

        ...
        my_flow.add_decorator(project, deco_kwargs={"name":"my_project"})
        ```

        is equivalent to:
        ```
        my_flow.add_decorator("project:name=my_project")
        ```

        Note in the later case, there is no need to import the flow decorator.

        The latter syntax is useful to, for example, allow decorators to be stored as
        strings in a configuration file.

        In terms of precedence for decorators:
          - if a decorator can be applied multiple times all decorators
            added are kept (this is rare for flow-decorators).
          - if `duplicates` is set to `MutableFlow.IGNORE`, then the decorator
            being added is ignored (in other words, the existing decorator has precedence).
          - if `duplicates` is set to `MutableFlow.OVERRIDE`, then the *existing*
            decorator is removed and this newly added one replaces it (in other
            words, the newly added decorator has precedence).
          - if `duplicates` is set to `MutableFlow.ERROR`, then an error is raised but only
            if the newly added decorator is *static* (ie: defined directly in the code).
            If not, it is ignored.

        Parameters
        ----------
        deco_type : Union[partial, str]
            The decorator class to add to this flow. If using a string, you cannot specify
            additional arguments as all argument will be parsed from the decorator
            specification.
        deco_args : List[Any], optional, default None
            Positional arguments to pass to the decorator.
        deco_kwargs : Dict[str, Any], optional, default None
            Keyword arguments to pass to the decorator.
        duplicates : int, default MutableFlow.IGNORE
            Instruction on how to handle duplicates. It can be one of:
            - `MutableFlow.IGNORE`: Ignore the decorator if it already exists.
            - `MutableFlow.ERROR`: Raise an error if the decorator already exists.
            - `MutableFlow.OVERRIDE`: Remove the existing decorator and add this one.

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

        deco_args = deco_args or []
        deco_kwargs = deco_kwargs or {}

        def _add_flow_decorator(flow_deco):
            if deco_args:
                raise MetaflowException(
                    "Flow decorators do not take additional positional arguments"
                )
            # Update kwargs:
            flow_deco.attributes.update(deco_kwargs)

            # Check duplicates
            def _do_add():
                flow_deco.statically_defined = self._statically_defined
                flow_deco.inserted_by = self._inserted_by
                self._flow_cls._flow_decorators.setdefault(flow_deco.name, []).append(
                    flow_deco
                )
                debug.userconf_exec(
                    "Mutable flow adding flow decorator '%s'" % deco_type
                )

            existing_deco = [
                d for d in self._flow_cls._flow_decorators if d.name == flow_deco.name
            ]

            if flow_deco.allow_multiple or not existing_deco:
                _do_add()
            elif duplicates == MutableFlow.IGNORE:
                # If we ignore, we do not add the decorator
                debug.userconf_exec(
                    "Mutable flow ignoring flow decorator '%s'"
                    "(already exists and duplicates are ignored)" % flow_deco.name
                )
            elif duplicates == MutableFlow.OVERRIDE:
                # If we override, we remove the existing decorator and add this one
                debug.userconf_exec(
                    "Mutable flow overriding flow decorator '%s' "
                    "(removing existing decorator and adding new one)" % flow_deco.name
                )
                self._flow_cls._flow_decorators = [
                    d
                    for d in self._flow_cls._flow_decorators
                    if d.name != flow_deco.name
                ]
                _do_add()
            elif duplicates == MutableFlow.ERROR:
                # If we error, we raise an exception
                if self._statically_defined:
                    raise DuplicateFlowDecoratorException(flow_deco.name)
                else:
                    debug.userconf_exec(
                        "Mutable flow ignoring flow decorator '%s' "
                        "(already exists and non statically defined)" % flow_deco.name
                    )
            else:
                raise ValueError("Invalid duplicates value: %s" % duplicates)

        # If deco_type is a string, we want to parse it to a decospec
        if isinstance(deco_type, str):
            flow_deco, has_args_kwargs = extract_flow_decorator_from_decospec(deco_type)
            if (deco_args or deco_kwargs) and has_args_kwargs:
                raise MetaflowException(
                    "Cannot specify additional arguments when adding a flow decorator "
                    "using a decospec that already contains arguments"
                )
            _add_flow_decorator(flow_deco)
            return

        # Validate deco_type
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], FlowDecorator)
        ):
            raise TypeError("add_decorator takes a FlowDecorator")

        deco_type = deco_type.args[0]
        _add_flow_decorator(
            deco_type(
                attributes=deco_kwargs,
                statically_defined=self._statically_defined,
                inserted_by=self._inserted_by,
            )
        )

    def remove_decorator(
        self,
        deco_name: str,
        deco_args: Optional[List[Any]] = None,
        deco_kwargs: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Remove a flow-level decorator. To remove a decorator, you can pass the decorator
        specification (obtained from `decorator_specs` for example).
        Note that if multiple decorators share the same decorator specification
        (very rare), they will all be removed.

        You can only remove decorators in the `pre_mutate` method.

        Parameters
        ----------
        deco_name : str
            Decorator specification of the decorator to remove. If nothing else is
            specified, all decorators matching that name will be removed.
        deco_args : List[Any], optional, default None
            Positional arguments to match the decorator specification.
        deco_kwargs : Dict[str, Any], optional, default None
            Keyword arguments to match the decorator specification.

        Returns
        -------
        bool
            Returns True if a decorator was removed.
        """

        if not self._pre_mutate:
            raise MetaflowException(
                "Removing flow-decorator '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method" % (deco_name, self._inserted_by)
            )

        do_all = deco_args is None and deco_kwargs is None
        did_remove = False
        if do_all and deco_name in self._flow_cls._flow_decorators:
            del self._flow_cls._flow_decorators[deco_name]
            return True
        old_deco_list = self._flow_cls._flow_decorators.get(deco_name)
        if not old_deco_list:
            debug.userconf_exec(
                "Mutable flow failed to remove decorator '%s' from flow (non present)"
                % deco_name
            )
            return False
        new_deco_list = []
        for deco in old_deco_list:
            if deco.get_args_kwargs() == (deco_args or [], deco_kwargs or {}):
                did_remove = True
            else:
                new_deco_list.append(deco)
        debug.userconf_exec(
            "Mutable flow removed %d decorators from flow"
            % (len(old_deco_list) - len(new_deco_list))
        )
        if new_deco_list:
            self._flow_cls._flow_decorators[deconame] = new_deco_list
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
