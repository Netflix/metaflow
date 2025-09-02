from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Union,
)

from metaflow.debug import debug
from metaflow.exception import MetaflowException

from .user_step_decorator import StepMutator, UserStepDecoratorBase

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec


class MutableStep:
    IGNORE = 1
    ERROR = 2
    OVERRIDE = 3

    def __init__(
        self,
        flow_spec: "metaflow.flowspec.FlowSpec",
        step: Union[
            Callable[["metaflow.decorators.FlowSpecDerived"], None],
            Callable[["metaflow.decorators.FlowSpecDerived", Any], None],
        ],
        pre_mutate: bool = False,
        statically_defined: bool = False,
        inserted_by: Optional[str] = None,
    ):
        from .mutable_flow import MutableFlow

        self._mutable_container = MutableFlow(
            flow_spec,
            pre_mutate=pre_mutate,
            statically_defined=statically_defined,
            inserted_by=inserted_by,
        )
        self._flow_cls = flow_spec.__class__
        self._my_step = step
        self._pre_mutate = pre_mutate
        self._statically_defined = statically_defined
        self._inserted_by = inserted_by
        if self._inserted_by is None:
            # This is an error because MutableSteps should only be created by
            # StepMutators or FlowMutators. We need to catch it now because otherwise
            # we may put stuff on the command line (with --with) that would get added
            # twice and weird behavior may ensue.
            raise MetaflowException(
                "MutableStep should only be created by StepMutators or FlowMutators. "
                "This is an internal error."
            )

    @property
    def flow(self) -> "metaflow.user_decorator.mutable_flow.MutableFlow":
        """
        The flow that contains this step

        Returns
        -------
        MutableFlow
            The flow that contains this step
        """
        return self._mutable_container

    @property
    def decorator_specs(
        self,
    ) -> Generator[Tuple[str, str, List[Any], Dict[str, Any]], None, None]:
        """
        Iterate over all the decorator specifications of this step. Note that the same
        type of decorator may be present multiple times and no order is guaranteed.

        The returned tuple contains:
        - The decorator's name (shortest possible)
        - The decorator's fully qualified name (in the case of Metaflow decorators, this
          will indicate which extension the decorator comes from)
        - A list of positional arguments
        - A dictionary of keyword arguments

        You can use the resulting tuple to remove a decorator for example

        Yields
        ------
        str, str, List[Any], Dict[str, Any]
            A tuple containing the decorator name, it's fully qualified name,
            a list of positional arguments, and a dictionary of keyword arguments.
        """
        for deco in self._my_step.decorators:
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

        for deco in self._my_step.wrappers:
            r = [
                UserStepDecoratorBase.get_decorator_name(deco.__class__),
                deco.decorator_name,
            ]
            r.extend(deco.get_args_kwargs())
            yield tuple(r)

        for deco in self._my_step.config_decorators:
            r = [
                UserStepDecoratorBase.get_decorator_name(deco.__class__),
                deco.decorator_name,
            ]
            r.extend(deco.get_args_kwargs())
            yield tuple(r)

    def add_decorator(
        self,
        deco_type: Union[partial, UserStepDecoratorBase, str],
        deco_args: Optional[List[Any]] = None,
        deco_kwargs: Optional[Dict[str, Any]] = None,
        duplicates: int = IGNORE,
    ) -> None:
        """
        Add a Metaflow step-decorator or a user step-decorator to a step.

        You can either add the decorator itself or its decorator specification for it
        (the same you would get back from decorator_specs). You can also mix and match
        but you cannot provide arguments both through the string and the
        deco_args/deco_kwargs.

        As an example:
        ```
        from metaflow import environment
        ...
        my_step.add_decorator(environment, deco_kwargs={"vars": {"foo": 42})}
        ```

        is equivalent to:
        ```
        my_step.add_decorator('environment:vars={"foo": 42}')
        ```

        is equivalent to:
        ```
        my_step.add_decorator('environment', deco_kwargs={"vars":{"foo": 42}})
        ```

        but this is not allowed:
        ```
        my_step.add_decorator('environment:vars={"bar" 43}', deco_kwargs={"vars":{"foo": 42}})
        ```

        Note in the case where you specify a
        string for the decorator, there is no need to import the decorator.

        The string syntax is useful to, for example, allow decorators to be stored as
        strings in a configuration file.

        You can only add StepMutators in the pre_mutate stage.

        In terms of precedence for decorators:
          - if a decorator can be applied multiple times (like `@card`) all decorators
            added are kept.
          - if `duplicates` is set to `MutableStep.IGNORE`, then the decorator
            being added is ignored (in other words, the existing decorator has precedence).
          - if `duplicates` is set to `MutableStep.OVERRIDE`, then the *existing*
            decorator is removed and this newly added one replaces it (in other
            words, the newly added decorator has precedence).
          - if `duplicates` is set to `MutableStep.ERROR`, then an error is raised but only
            if the newly added decorator is *static* (ie: defined directly in the code).
            If not, it is ignored.

        Parameters
        ----------
        deco_type : Union[partial, UserStepDecoratorBase, str]
            The decorator class to add to this step.
        deco_args : List[Any], optional, default None
            Positional arguments to pass to the decorator.
        deco_kwargs : Dict[str, Any], optional, default None
            Keyword arguments to pass to the decorator.
        duplicates : int, default MutableStep.IGNORE
            Instruction on how to handle duplicates. It can be one of:
            - `MutableStep.IGNORE`: Ignore the decorator if it already exists.
            - `MutableStep.ERROR`: Raise an error if the decorator already exists.
            - `MutableStep.OVERRIDE`: Remove the existing decorator and add this one.
        """
        # Prevent circular import
        from metaflow.decorators import (
            DuplicateStepDecoratorException,
            StepDecorator,
            extract_step_decorator_from_decospec,
        )

        deco_args = deco_args or []
        deco_kwargs = deco_kwargs or {}

        def _add_step_decorator(step_deco):
            if deco_args:
                raise MetaflowException(
                    "Step decorators do not take additional positional arguments"
                )
            # Update kwargs:
            step_deco.attributes.update(deco_kwargs)

            # Check duplicates
            def _do_add():
                step_deco.statically_defined = self._statically_defined
                step_deco.inserted_by = self._inserted_by
                self._my_step.decorators.append(step_deco)
                debug.userconf_exec(
                    "Mutable step adding step decorator '%s' to step '%s'"
                    % (deco_type, self._my_step.name)
                )

            existing_deco = [
                d for d in self._my_step.decorators if d.name == step_deco.name
            ]

            if step_deco.allow_multiple or not existing_deco:
                _do_add()
            elif duplicates == MutableStep.IGNORE:
                # If we ignore, we do not add the decorator
                debug.userconf_exec(
                    "Mutable step ignoring step decorator '%s' on step '%s' "
                    "(already exists and duplicates are ignored)"
                    % (step_deco.name, self._my_step.name)
                )
            elif duplicates == MutableStep.OVERRIDE:
                # If we override, we remove the existing decorator and add this one
                debug.userconf_exec(
                    "Mutable step overriding step decorator '%s' on step '%s' "
                    "(removing existing decorator and adding new one)"
                    % (step_deco.name, self._my_step.name)
                )
                self._my_step.decorators = [
                    d for d in self._my_step.decorators if d.name != step_deco.name
                ]
                _do_add()
            elif duplicates == MutableStep.ERROR:
                # If we error, we raise an exception
                if self._statically_defined:
                    raise DuplicateStepDecoratorException(step_deco.name, self._my_step)
                else:
                    debug.userconf_exec(
                        "Mutable step ignoring step decorator '%s' on step '%s' "
                        "(already exists and non statically defined)"
                        % (step_deco.name, self._my_step.name)
                    )
            else:
                raise ValueError("Invalid duplicates value: %s" % duplicates)

        if isinstance(deco_type, str):
            step_deco, has_args_kwargs = extract_step_decorator_from_decospec(deco_type)
            if (deco_args or deco_kwargs) and has_args_kwargs:
                raise MetaflowException(
                    "Cannot specify additional arguments when adding a user step "
                    "decorator using a decospec that already has arguments"
                )

            if isinstance(step_deco, StepDecorator):
                _add_step_decorator(step_deco)
            else:
                # User defined decorator.
                if not self._pre_mutate and isinstance(step_deco, StepMutator):
                    raise MetaflowException(
                        "Adding step mutator '%s' from %s is only allowed in the "
                        "`pre_mutate` method and not the `mutate` method"
                        % (step_deco.decorator_name, self._inserted_by)
                    )

                if deco_args or deco_kwargs:
                    # We need to recreate the object if there were args or kwargs
                    # since they were not in the string
                    step_deco = step_deco.__class__(*deco_args, **deco_kwargs)

                step_deco.add_or_raise(
                    self._my_step,
                    self._statically_defined,
                    duplicates,
                    self._inserted_by,
                )
            return

        if isinstance(deco_type, type) and issubclass(deco_type, UserStepDecoratorBase):
            # We can only add step mutators in the pre mutate stage.
            if not self._pre_mutate and issubclass(deco_type, StepMutator):
                raise MetaflowException(
                    "Adding step mutator '%s' from %s is only allowed in the "
                    "`pre_mutate` method and not the `mutate` method"
                    % (step_deco.decorator_name, self._inserted_by)
                )
            debug.userconf_exec(
                "Mutable step adding decorator %s to step %s"
                % (deco_type, self._my_step.name)
            )

            d = deco_type(*deco_args, **deco_kwargs)
            # add_or_raise properly registers the decorator
            d.add_or_raise(
                self._my_step, self._statically_defined, duplicates, self._inserted_by
            )
            return

        # At this point, it should be a regular Metaflow step decorator
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], StepDecorator)
        ):
            raise TypeError(
                "add_decorator takes a metaflow decorator or user StepDecorator"
            )

        deco_type = deco_type.args[0]
        _add_step_decorator(
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
        Remove a step-level decorator. To remove a decorator, you can pass the decorator
        specification (obtained from `decorator_specs` for example).
        Note that if multiple decorators share the same decorator specification
        (very rare), they will all be removed.

        You can only remove StepMutators in the `pre_mutate` method.

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

        do_all = deco_args is None and deco_kwargs is None
        did_remove = False
        canonical_deco_type = UserStepDecoratorBase.get_decorator_by_name(deco_name)
        if issubclass(canonical_deco_type, UserStepDecoratorBase):
            for attr in ["config_decorators", "wrappers"]:
                new_deco_list = []
                for deco in getattr(self._my_step, attr):
                    if deco.decorator_name == canonical_deco_type.decorator_name:
                        if do_all:
                            continue  # We remove all decorators with this name
                        if deco.get_args_kwargs() == (
                            deco_args or [],
                            deco_kwargs or {},
                        ):
                            if not self._pre_mutate and isinstance(deco, StepMutator):
                                raise MetaflowException(
                                    "Removing step mutator '%s' from %s is only allowed in the "
                                    "`pre_mutate` method and not the `mutate` method"
                                    % (deco.decorator_name, self._inserted_by)
                                )
                            did_remove = True
                            debug.userconf_exec(
                                "Mutable step removing user step decorator '%s' from step '%s'"
                                % (deco.decorator_name, self._my_step.name)
                            )
                        else:
                            new_deco_list.append(deco)
                    else:
                        new_deco_list.append(deco)
                setattr(self._my_step, attr, new_deco_list)

        if did_remove:
            return True
        new_deco_list = []
        for deco in self._my_step.decorators:
            if deco.name == deco_name:
                if do_all:
                    continue  # We remove all decorators with this name
                # Check if the decorator specification matches
                if deco.get_args_kwargs() == (deco_args, deco_kwargs):
                    did_remove = True
                    debug.userconf_exec(
                        "Mutable step removing step decorator '%s' from step '%s'"
                        % (deco.name, self._my_step.name)
                    )
                else:
                    new_deco_list.append(deco)
            else:
                new_deco_list.append(deco)

        self._my_step.decorators = new_deco_list

        if did_remove:
            return True

        debug.userconf_exec(
            "Mutable step did not find decorator '%s' to remove from step '%s'"
            % (deco_name, self._my_step.name)
        )
        return False
