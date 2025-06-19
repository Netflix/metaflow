from functools import partial
from typing import Any, Callable, Generator, Optional, TYPE_CHECKING, Union

from metaflow.debug import debug
from metaflow.exception import MetaflowException

from .user_step_decorator import StepMutator, UserStepDecoratorBase

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec


class MutableStep:
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
    def decorator_specs(self) -> Generator[str, None, None]:
        """
        Iterate over all the decorator specifications of this step. Note that the same
        type of decorator may be present multiple times and no order is guaranteed.
        A decorator specification has the following format:
        `<decorator_name>:<arg1>=<value1>,<arg2>=<value2>,...`.

        You can use the decorator specification to remove a decorator from the step
        for example.

        Yields
        ------
        str
            A string representing the decorator.
        """
        for deco in self._my_step.decorators:
            yield deco.make_decorator_spec()

        for deco in self._my_step.wrappers:
            yield deco.make_decorator_spec()

        for deco in self._my_step.config_decorators:
            yield deco.make_decorator_spec()

    def add_decorator(
        self, deco_type: Union[partial, UserStepDecoratorBase, str], **kwargs
    ) -> None:
        """
        Add a Metaflow step-decorator or a user step-decorator to a step.

        You can either add the decorator itself or its decorator specification for it
        (the same you would get back from decorator_specs).

        As an example:
        ```
        from metaflow import environment
        ...
        my_step.add_decorator(environment, vars={"foo": 42})
        ```

        is equivalent to:
        ```
        my_step.add_decorator('environment:vars={"foo": 42}')
        ```

        Note in the later case, there is no need to import the step decorator.

        The latter syntax is useful to, for example, allow decorators to be stored as
        strings in a configuration file.

        You can only add StepMutators in the pre_mutate stage.

        Parameters
        ----------
        deco_type : Union[partial, UserStepDecoratorBase, str]
            The decorator class to add to this step. If using a string, you cannot specify
            additional arguments as all argument will be parsed from the decorator
            specification.
        """
        # Prevent circular import
        from metaflow.decorators import (
            DuplicateStepDecoratorException,
            StepDecorator,
            extract_step_decorator_from_decospec,
        )

        if isinstance(deco_type, str):
            if kwargs:
                raise MetaflowException(
                    "Cannot specify additional arguments when adding a user step "
                    "decorator using a decospec"
                )
            step_deco = extract_step_decorator_from_decospec(
                deco_type, self._flow_cls._global_dicts
            )
            step_deco.statically_defined = self._statically_defined
            step_deco.inserted_by = self._inserted_by
            if isinstance(step_deco, StepDecorator):
                # Check duplicates
                if (
                    step_deco.name in [deco.name for deco in self._my_step.decorators]
                    and not step_deco.allow_multiple
                ):
                    if self._statically_defined:
                        raise DuplicateStepDecoratorException(
                            step_deco.name, self._my_step
                        )
                    else:
                        return  # Else we ignore
                debug.userconf_exec(
                    "Mutable step adding step decorator '%s' to step '%s' (from str)"
                    % (deco_type, self._my_step.name)
                )
                self._my_step.decorators.append(step_deco)
            else:
                # User defined decorator.
                if not self._pre_mutate and isinstance(step_deco, StepMutator):
                    raise MetaflowException(
                        "Adding step mutator '%s' from %s is only allowed in the "
                        "`pre_mutate` method and not the `mutate` method"
                        % (step_deco.decorator_name, self._inserted_by)
                    )
                debug.userconf_exec(
                    "Mutable step adding decorator %s to step %s (from str)"
                    % (deco_type, self._my_step.name)
                )
                step_deco.add_or_raise(
                    self._my_step, self._statically_defined, self._inserted_by
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

            # In __call__, the decorator will append itself to the right field to
            # self._my_step
            d = deco_type(**kwargs)
            d.add_or_raise(self._my_step, self._statically_defined, self._inserted_by)
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
        if (
            deco_type.name in [deco.name for deco in self._my_step.decorators]
            and not deco_type.allow_multiple
        ):
            if self._statically_defined:
                raise DuplicateStepDecoratorException(deco_type.name, self._my_step)
            else:
                return  # Else we ignore

        debug.userconf_exec(
            "Mutable step adding step decorator '%s' to step %s with attributes %s"
            % (deco_type.name, self._my_step.name, str(kwargs))
        )
        self._my_step.decorators.append(
            deco_type(
                attributes=kwargs,
                statically_defined=self._statically_defined,
                inserted_by=self._inserted_by,
            )
        )

    def remove_decorator(self, deco_spec: str) -> bool:
        """
        Remove a step-level decorator. To remove a decorator, you can pass the decorator
        specification (obtained from `decorator_specs` for example).
        Note that if multiple decorators share the same decorator specification
        (very rare), they will all be removed.

        You can only remove StepMutators in the `pre_mutate` method.

        Parameters
        ----------
        deco_spec : str
            Decorator specification of the decorator to remove.

        Returns
        -------
        bool
            Returns True if a decorator was removed.
        """

        splits = deco_spec.split(":", 1)
        deconame = splits[0]
        new_deco_list = []
        did_remove = False
        for deco in self._my_step.decorators:
            if deco.name == deconame:
                # Check if the decorator specification matches
                if deco.make_decorator_spec() == deco_spec:
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

        # If we did not remove a decorator yet, we need to check the user step
        # decorators as well
        for attr in ["config_decorators", "wrappers"]:
            new_deco_list = []

            for deco in getattr(self._my_step, attr):
                if deco.decorator_name == deconame:
                    if deco.make_decorator_spec() == deco_spec:
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

        debug.userconf_exec(
            "Mutable step did not find decorator '%s' to remove from step '%s'"
            % (deconame, self._my_step.name)
        )
        return False
