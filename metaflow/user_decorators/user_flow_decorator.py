from typing import Dict, Optional, Union, TYPE_CHECKING

from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import (
    resolve_delayed_evaluator,
    unpack_delayed_evaluator,
)

from .common import ClassPath_Trie

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.user_decorators.mutable_flow


class FlowMutatorMeta(type):
    _all_registered_decorators = ClassPath_Trie()
    _do_not_register = set()
    _import_modules = set()

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls.decorator_name = getattr(
            cls, "_decorator_name", f"{cls.__module__}.{cls.__name__}"
        )
        if not cls.__module__.startswith("metaflow.") and not cls.__module__.startswith(
            "metaflow_extensions."
        ):
            mcs._import_modules.add(cls.__module__)

        if name == "FlowMutator" or cls.decorator_name in mcs._do_not_register:
            return cls

        # We inject a __init_subclass__ method so we can figure out if there
        # are subclasses. We want to register as decorators only the ones that do
        # not have a subclass. The logic is that everything is registered and if
        # a subclass shows up, we will unregister the parent class leaving only those
        # classes that do not have any subclasses registered.
        @classmethod
        def do_unregister(cls_, **_kwargs):
            for base in cls_.__bases__:
                if isinstance(base, FlowMutatorMeta):
                    # If the base is a FlowMutatorMeta, we unregister it
                    # so that we don't have any decorators that are not the
                    # most derived one.
                    mcs._all_registered_decorators.remove(base.decorator_name)
                    # Also make sure we don't register again
                    mcs._do_not_register.add(base.decorator_name)

        cls.__init_subclass__ = do_unregister
        mcs._all_registered_decorators.insert(cls.decorator_name, cls)
        return cls

    @classmethod
    def all_decorators(mcs) -> Dict[str, "FlowMutatorMeta"]:
        mcs._check_init()
        return mcs._all_registered_decorators.get_unique_prefixes()

    def __str__(cls):
        return "FlowMutator(%s)" % cls.decorator_name

    @classmethod
    def get_decorator_by_name(
        mcs, decorator_name: str
    ) -> Optional[Union["FlowDecoratorMeta", "metaflow.decorators.Decorator"]]:
        """
        Get a decorator by its name.

        Parameters
        ----------
        decorator_name: str
            The name of the decorator to retrieve.

        Returns
        -------
        Optional[FlowDecoratorMeta]
            The decorator class if found, None otherwise.
        """
        mcs._check_init()
        return mcs._all_registered_decorators.unique_prefix_value(decorator_name)

    @classmethod
    def get_decorator_name(mcs, decorator_type: type) -> Optional[str]:
        """
        Get the minimally unique classpath name for a decorator type.

        Parameters
        ----------
        decorator_type: type
            The type of the decorator to retrieve the name for.

        Returns
        -------
        Optional[str]
            The minimally unique classpath name if found, None otherwise.
        """
        mcs._check_init()
        return mcs._all_registered_decorators.unique_prefix_for_type(decorator_type)

    @classmethod
    def _check_init(mcs):
        # Delay importing STEP_DECORATORS until we actually need it
        if not mcs._all_registered_decorators.inited:
            from metaflow.plugins import FLOW_DECORATORS

            mcs._all_registered_decorators.init([(t.name, t) for t in FLOW_DECORATORS])


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

            # Now set the flow class we apply to
            if isinstance(args[0], FlowSpecMeta):
                self._set_flow_cls(args[0])
            else:
                self._set_flow_cls(args[0]._flow_cls)
            self._args = self._args[1:]  # Remove the first argument

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
            if isinstance(flow_spec, FlowMutator):
                flow_spec = flow_spec._flow_cls
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

        flow_spec._flow_state.setdefault(_FlowState.FLOW_MUTATORS, []).append(self)
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

        """
        pass

    def external_init(self):
        # You can use config values in the arguments to a FlowMutator
        # so we resolve those as well
        self._args = [resolve_delayed_evaluator(arg) for arg in self._args]
        self._kwargs, _ = unpack_delayed_evaluator(self._kwargs)
        self._kwargs = {
            k: resolve_delayed_evaluator(v) for k, v in self._kwargs.items()
        }
        if self._args or self._kwargs:
            if "init" not in self.__class__.__dict__:
                raise MetaflowException(
                    "%s is used with arguments but does not implement init" % self
                )
        if "init" in self.__class__.__dict__:
            self.init(*self._args, **self._kwargs)

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
