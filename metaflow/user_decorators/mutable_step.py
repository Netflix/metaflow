from functools import partial
from typing import Any, Callable, Generator, TYPE_CHECKING, Union

from metaflow.debug import debug

from .user_step_decorator import StepDecorator

if TYPE_CHECKING:
    import metaflow.decorators
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_flow


class MutableStep:
    """
    A MutableStep is a wrapper passed to the `StepMutator`'s `mutate` method
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
        from .mutable_flow import MutableFlow

        self._mutable_container = MutableFlow(flow_spec)
        self._my_step = step

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

        Yields
        ------
        str
            A string containing the specification of the decorator (its type and
            arguments -- identical to the one you would pass with --with)

        TODO: Should we add statically_defined. I feel that distinction may not be as
        significant for the user so we may want to rethink it.
        """
        for deco in self._my_step.decorators:
            yield deco.make_decorator_spec()

        for deco in self._my_step.wrappers:
            yield deco.make_decorator_spec()

    def add_decorator(self, deco_type: Union[partial, StepDecorator], **kwargs) -> None:
        """
        Add a Metaflow decorator or user decorator to a step.

        Parameters
        ----------
        deco_type : Union[partial, StepDecorator]
            The decorator class to add to this step. This decorator can be a Metaflow
            decorator as well as a user-defined step-decorator (defined using
            `@step_decorator` or a derived class of StepDecorator)
        """
        # Prevent circular import
        from metaflow.decorators import DuplicateStepDecoratorException
        from metaflow.decorators import StepDecorator as MFStepDecorator

        if isinstance(deco_type, StepDecorator):
            debug.userconf_exec(
                "Step mutator adding user decorator %s to step %s"
                % (deco_type.decorator_name, self._my_step.name)
            )
            deco_type(**kwargs)(self._my_step)
            return
        # Validate deco_type
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], MFStepDecorator)
        ):
            raise TypeError(
                "add_decorator takes a metaflow decorator or user StepDecorator"
            )

        deco_type = deco_type.args[0]
        if (
            deco_type.name in [deco.name for deco in self._my_step.decorators]
            and not deco_type.allow_multiple
        ):
            raise DuplicateStepDecoratorException(deco_type.name, self._my_step)

        debug.userconf_exec(
            "Step mutator adding step decorator '%s' to step '%s'"
            % (deco_type.name, self._my_step.name)
        )
        self._my_step.decorators.append(
            deco_type(attributes=kwargs, statically_defined=True)
        )

    def remove_decorator(self, deco_name: str, all: bool = True, **kwargs) -> bool:
        """
        Removes decorators with the name `deco_name` from the step.

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
                        "Step mutator removing step decorator '%s' from step '%s"
                        % (deco.name, self._my_step.name)
                    )
                else:
                    new_deco_list.append(deco)
            else:
                new_deco_list.append(deco)
            if did_remove and not all:
                break

        self._my_step.decorators = new_deco_list

        new_deco_list = []
        did_remove = False
        for deco in self._my_step.wrappers:
            if deco.decorator_name == deco_name:
                # Check filters
                match_ok = True
                if kwargs:
                    for k, v in kwargs.items():
                        match_ok = k in deco._kwargs and deco._kwargs[k] == v
                        if match_ok is False:
                            break
                if match_ok:
                    did_remove = True
                    debug.userconf_exec(
                        "Step mutator removing user step decorator '%s' from step '%s"
                        % (deco.decorator_name, self._my_step.name)
                    )
                else:
                    new_deco_list.append(deco)
            else:
                new_deco_list.append(deco)
            if did_remove and not all:
                break
        self._my_step.wrappers = new_deco_list
        return did_remove
