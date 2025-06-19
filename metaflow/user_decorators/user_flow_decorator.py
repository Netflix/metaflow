from typing import Optional, TYPE_CHECKING

from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import (
    resolve_delayed_evaluator,
    unpack_delayed_evaluator,
)

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_flow


class FlowMutatorMeta(type):
    def __str__(cls):
        return "FlowMutator(%s)" % cls.__name__


class FlowMutator(metaclass=FlowMutatorMeta):
    """
    Derive from this class to implement a flow mutator.

    A flow mutator allows you to introspect a flow and its included steps. You can
    then add parameters, configurations and decorators to the flow as well as modify
    any of its steps.
    use values available through configurations to determine how to mutate the flow.

    There are two main methods provided:
      - pre_mutate: called as early as possible right after configuration values are read.
      - mutate: called right after all the command line is parsed but before any
        Metaflow decorators are applied.

    The `mutate` method does not allow you to modify the flow itself but you can still
    modify the steps.
    """

    def __init__(self, *args, **kwargs):
        from ..flowspec import FlowSpecMeta

        self._flow_cls = None
        # Flow mutators are always statically defined (no way of passing them
        # on the command line or adding them via configs)
        self.statically_defined = True
        self.inserted_by = None

        # The arguments are actually passed to the init function for this decorator
        # and used in _graph_info
        self._args = args
        self._kwargs = kwargs
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

    def __mro_entries__(self, bases):
        # This is called in the following case:
        # @MyMutator
        # class MyBaseFlowSpec(FlowSpec):
        #   pass
        #
        # class MyFlow(MyBaseFlowSpec):
        #   pass
        #
        # MyBaseFlowSpec will be an object of type MyMutator which is not
        # great when inheriting from it. With this method, we ensure that the
        # base class will actually be MyBaseFlowSpec
        return (self._flow_cls,)

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
            new_kwargs, _ = unpack_delayed_evaluator(self._kwargs)
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

    def __str__(self):
        return str(self.__class__)

    def init(self, *args, **kwargs):
        """
        Implement this method if you wish for your FlowMutator to take in arguments.

        Your flow-mutator can then look like:

        @MyMutator(arg1, arg2)
        class MyFlow(FlowSpec):
            pass

        It is an error to use your mutator with arguments but not implement this method.

        When implementing, you should not call super().init().
        """
        self._empty_init = True

    def pre_mutate(
        self, mutable_flow: "metaflow.user_decorators.mutable_flow.MutableFlow"
    ) -> None:
        """
        Method called right after all configuration values are read.

        Parameters
        ----------
        mutable_flow : metaflow.user_decorators.mutable_flow.MutableFlow
            A representation of this flow
        """
        return None

    def mutate(
        self, mutable_flow: "metaflow.user_decorators.mutable_flow.MutableFlow"
    ) -> None:
        """
        Method called right before the first Metaflow step decorator is applied. This
        means that the command line, including all `--with` options has been parsed.

        Given how late this function is called, there are a few restrictions on what
        you can do; the following methods on MutableFlow are not allowed and calling
        them will result in an error:
          - add_parameter/remove_parameter
          - add_decorator/remove_decorator

        To call these methods, use the `pre_mutate` method instead.

        Parameters
        ----------
        mutable_flow : metaflow.user_decorators.mutable_flow.MutableFlow
            A representation of this flow
        """
        return None
