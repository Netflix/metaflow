from typing import Optional, TYPE_CHECKING

from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import resolve_delayed_evaluator

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_flow


class FlowMutator:
    def __init__(self, *args, **kwargs):
        from ..flowspec import FlowSpecMeta

        self._flow_cls = None

        if args and isinstance(args[0], (FlowMutator, FlowSpecMeta)):
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

    def __call__(
        self, flow_spec: Optional["metaflow.flowspec.FlowSpecMeta"] = None
    ) -> "metaflow.flowspec.FlowSpecMeta":
        if flow_spec:
            # This is the case of a decorator @MyDecorator(foo=1, bar=2) and so
            # we already called __init__ and saved foo and bar in self._args and
            # self._kwargs and are now calling this on the flow itself.

            # You can use config values in the arguments to a FlowMutator
            # so we resolve those as well
            new_args = [resolve_delayed_evaluator(arg) for arg in self._args]
            new_kwargs = {
                k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
            }
            if new_args or new_kwargs:
                self.init(*new_args, **new_kwargs)
                if hasattr(self, "_empty_init"):
                    raise MetaflowException(
                        "FlowMutator '%s' is used with arguments "
                        "but does not implement init" % str(self.__class__)
                    )
            else:
                # If there are no actual arguments so @MyDecorator(), we behave like
                # @MyDecorator with one *crucial* difference: the return value will
                # be the flow_cls and NOT an object of type MyDecorator. This is
                # useful to add flow level decorators to children classes of FlowSpec
                # and effectively give default decorators to all instances of those
                # children classes.
                try:
                    self.init()
                except NotImplementedError:
                    pass

            return self._set_flow_cls(flow_spec)
        elif not self._flow_cls:
            # This means that somehow the initialization did not happen properly
            # so this may have been applied to a non flow
            raise MetaflowException("A FlowMutator can only be applied to a FlowSpec")
        # NOTA: This returns self._flow_cls() because the object in the case of
        # @FlowDecorator
        # class MyFlow(FlowSpec):
        #     pass
        # the object is a FlowDecorator and when the main function calls it, we end up
        # here and need to actually call the FlowSpec. This is not the case when using
        # a decorator with arguments because in the line above, we will have returned a
        # FlowSpec object. Previous solution was to use __get__ but this does not seem
        # to work properly.
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

    def mutate(
        self, mutable_flow: "metaflow.user_decorators.mutable_flow.MutableFlow"
    ) -> None:
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
