import array
import collections
import inspect
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from metaflow.debug import debug
from metaflow.exception import MetaflowException
from metaflow.user_configs.config_parameters import ConfigValue

if TYPE_CHECKING:
    import metaflow.flowspec
    import metaflow.parameters
    import metaflow.user_decorators.mutable_step
    import metaflow.user_decorators.user_flow_decorator
    import metaflow.decorators


# Stdlib mutable types that may legitimately appear as default-arg literals.
# Per UD-9: rejected at add_step decoration time with a None-sentinel hint.
# Third-party mutables (numpy.ndarray, pandas.DataFrame, etc.) are NOT
# detected by this denylist and are documented as user responsibility
# in docs/api/mutable_flow.md.
_MUTABLE_DEFAULT_TYPES = (
    list,
    dict,
    set,
    bytearray,
    collections.deque,
    collections.OrderedDict,
    collections.defaultdict,
    collections.Counter,
    array.array,
)


def _reject_mutable_defaults(func: Callable, sig: inspect.Signature) -> None:
    """Raise TypeError if any parameter has a mutable default literal."""
    for p in sig.parameters.values():
        if p.default is inspect.Parameter.empty:
            continue
        if isinstance(p.default, _MUTABLE_DEFAULT_TYPES):
            raise TypeError(
                "add_step: parameter %r on %r has mutable default %r. "
                "Use None sentinel + in-function check:\n"
                "  def f(%s=None):\n"
                "      if %s is None: %s = []  # or default literal\n"
                "      ..."
                % (
                    p.name,
                    getattr(func, "__qualname__", repr(func)),
                    p.default,
                    p.name,
                    p.name,
                    p.name,
                )
            )


def _is_advanced_mode_r8(sig: inspect.Signature) -> bool:
    """R8: single positional parameter named ``self``."""
    params = list(sig.parameters.values())
    if len(params) != 1:
        return False
    p = params[0]
    if p.name != "self":
        return False
    return p.kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


def _check_signature_rules(func: Callable, sig: inspect.Signature) -> str:
    """Apply rules R1..R13. Returns 'advanced_r8' or 'style_a'.

    Rejects unsupported patterns with TypeError at decoration time.
    R-EXTRA-EMBED-STYLE-A (embedded() in Style A funcs) is checked at
    lint time, not here.
    """
    # R5: decorated functions are ACCEPTED. inspect.signature already
    # follows __wrapped__ when functools.wraps is used; we do not call
    # inspect.unwrap here (signature alone is correct per the design memo).
    name = getattr(func, "__qualname__", repr(func))

    # R3: lambdas
    if getattr(func, "__name__", "") == "<lambda>":
        raise TypeError(
            "add_step: lambdas not supported (not packageable across runners). "
            "Define %s as a module-level `def`." % name
        )
    # R4: functools.partial
    if isinstance(func, partial):
        raise TypeError(
            "add_step: functools.partial is not supported in Phase 1; "
            "wrap the partial application in a module-level def."
        )
    # R11: class objects
    if inspect.isclass(func):
        raise TypeError(
            "add_step: class objects are not supported as steps; pass a "
            "function instead."
        )
    # R6: bound methods (excluding the R8 `self` advanced-mode signature)
    if inspect.ismethod(func) and getattr(func, "__self__", None) is not None:
        raise TypeError(
            "add_step: bound methods are not supported as steps; pass the "
            "underlying function instead."
        )
    # R9: async functions
    if inspect.iscoroutinefunction(func):
        raise TypeError("add_step: `async def` functions are not supported.")
    # R10: generator functions
    if inspect.isgeneratorfunction(func):
        raise TypeError("add_step: generator functions are not supported as steps.")
    # R12: callable instances (have __call__ but aren't function/method/class)
    if not (
        inspect.isfunction(func) or inspect.ismethod(func) or inspect.isbuiltin(func)
    ) and callable(func):
        raise TypeError(
            "add_step: callable instances (objects with __call__) are not "
            "supported. Pass the underlying function."
        )

    # Now look at signature parameters for R1, R2, R13 — and detect R8.
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                "add_step: *%s is not supported; declare positional "
                "parameters explicitly or use inputs={} for advanced wiring." % p.name
            )
        if p.kind is inspect.Parameter.VAR_KEYWORD:
            raise TypeError(
                "add_step: **%s is not supported; declare keyword "
                "parameters explicitly." % p.name
            )
        if p.kind is inspect.Parameter.KEYWORD_ONLY:
            raise TypeError(
                "add_step: keyword-only parameters (after *) are not "
                "supported. Promote %r to positional-or-keyword." % p.name
            )

    return "advanced_r8" if _is_advanced_mode_r8(sig) else "style_a"


def _resolve_inputs_map(
    sig: inspect.Signature,
    inputs: Optional[Dict[str, str]],
    mode: str,
) -> Dict[str, str]:
    """Build internal_name -> external_artifact_name map.

    Style A: signature drives the input list. Each positional parameter
    becomes both the internal name (caller perspective) and the default
    external name (producer perspective), unless overridden in ``inputs``.

    Advanced R8: the single ``self`` parameter is the proxy; inputs are
    declared explicitly via ``inputs={internal: external, ...}``.
    """
    inputs = dict(inputs or {})
    result: Dict[str, str] = {}
    if mode == "advanced_r8":
        # The user's body references self.<x>; the overlay maps each
        # declared internal name to its external in the producer.
        return inputs
    for p in sig.parameters.values():
        internal = p.name
        result[internal] = inputs.get(internal, internal)
    # Allow inputs= to introduce extra overlay entries beyond the
    # signature (e.g. for self.<x> references not captured by params).
    for k, v in inputs.items():
        if k not in result:
            result[k] = v
    return result


def _normalize_produces(
    produces: Union[str, Tuple[str, ...], List[str], None],
) -> Optional[Tuple[str, ...]]:
    if produces is None:
        return None
    if isinstance(produces, str):
        return (produces,)
    if isinstance(produces, (list, tuple)):
        return tuple(produces)
    raise TypeError(
        "add_step: produces= must be None, a str, or a tuple/list of str; "
        "got %r" % (produces,)
    )


def _spec_signature(
    func: Callable,
    produces: Optional[Tuple[str, ...]],
    after: Tuple[str, ...],
    decorators: Optional[List[Any]],
) -> Tuple[Any, ...]:
    """Stable tuple used to detect re-registration with matching spec."""
    return (
        getattr(func, "__qualname__", repr(func)),
        getattr(func, "__module__", None),
        produces,
        after,
        tuple(repr(d) for d in (decorators or [])),
    )


def _synthesize_wrapper(
    *,
    name: str,
    func: Callable,
    sig: inspect.Signature,
    mode: str,
    inputs_map: Dict[str, str],
    produces: Optional[Tuple[str, ...]],
    edges: Dict[str, Any],
) -> Callable:
    """Build the FlowSpec-callable wrapper around a user function.

    The returned wrapper has the FlowSpec calling convention
    ``f(self)`` and at runtime:
      1. Resolves declared inputs via overlay-aware getattr.
      2. Invokes ``func(...)``.
      3. Stores the return value(s) on ``self.<produces>``.
      4. Calls ``self.next(...)`` using the wrapper's ``_mf_edges["out"]``.

    The wrapper carries:
      - ``_mf_edges``: ``{"in": [...], "out": [...], "type": "linear"}``
        — a mutable dict shared with FlowGraph so post-graph-build
        rewiring updates take effect at runtime via the same reference.
      - ``_mf_dataflow``: introspection metadata about the input/output
        names (used by graph mutations and lint).
      - ``_mf_dataflow_entries``: list of registered data_flow entries
        to be emitted into ``_graph_info``.
      - ``_mf_defaults``: snapshotted defaults captured at decoration time.
      - ``is_step``, ``decorators``, ``wrappers``, ``config_decorators``:
        attributes the FlowSpec runtime expects on every step.
      - ``__wrapped__``: pointer at the original user func so
        ``inspect.unwrap`` recovers the source file for ``DAGNode``.
    """
    # Snapshot defaults at decoration time. Mutable defaults are already
    # rejected by _reject_mutable_defaults, so capture-by-reference is
    # safe by construction for documented stdlib types.
    defaults: Dict[str, Any] = {}
    param_names: List[str] = []
    for p_name, p in sig.parameters.items():
        param_names.append(p_name)
        if p.default is not inspect.Parameter.empty:
            defaults[p_name] = p.default

    produces_tuple = produces  # None or tuple[str, ...]

    # Sentinel — the actual successors are set by FlowGraph post-processing.
    # We refer to `edges` via closure so post-graph-build mutations are
    # visible to the wrapper at runtime through the same dict reference.

    if mode == "advanced_r8":
        # The user func takes its own `self` (a _NamespacedSelf proxy).
        # Inputs are NOT extracted via getattr in the wrapper — the body
        # reads them via the proxy at the point of use.
        from metaflow._namespaced_self import _NamespacedSelf

        def _wrapper(self):
            ns = _NamespacedSelf(self)
            result = func(ns)
            if produces_tuple is None:
                pass
            elif len(produces_tuple) == 1:
                setattr(self, produces_tuple[0], result)
            else:
                if not isinstance(result, tuple) or len(result) != len(produces_tuple):
                    raise RuntimeError(
                        "step %r: produces declared %d values but function "
                        "returned %r" % (name, len(produces_tuple), result)
                    )
                for slot_name, value in zip(produces_tuple, result):
                    setattr(self, slot_name, value)
            out_names = list(edges.get("out", []))
            if not out_names:
                # Unknown successors — defer to FlowSpec.next's own validation
                self.next()
            else:
                self.next(*[getattr(self, n) for n in out_names])

    else:
        # Style A: signature-driven inputs, with try/except AttributeError
        # fallback to closure-captured immutable defaults (UD-6).
        def _wrapper(self):
            kwargs: Dict[str, Any] = {}
            for p_name in param_names:
                try:
                    kwargs[p_name] = getattr(self, p_name)
                except AttributeError:
                    if p_name in defaults:
                        kwargs[p_name] = defaults[p_name]
                    else:
                        raise
            result = func(**kwargs)
            if produces_tuple is None:
                pass
            elif len(produces_tuple) == 1:
                setattr(self, produces_tuple[0], result)
            else:
                if not isinstance(result, tuple) or len(result) != len(produces_tuple):
                    raise RuntimeError(
                        "step %r: produces declared %d values but function "
                        "returned %r" % (name, len(produces_tuple), result)
                    )
                for slot_name, value in zip(produces_tuple, result):
                    setattr(self, slot_name, value)
            out_names = list(edges.get("out", []))
            if not out_names:
                self.next()
            else:
                self.next(*[getattr(self, n) for n in out_names])

    _wrapper.__name__ = name
    _wrapper.__qualname__ = getattr(func, "__qualname__", name)
    _wrapper.__module__ = getattr(func, "__module__", "metaflow.user_decorators")
    _wrapper.__doc__ = getattr(func, "__doc__", None)
    _wrapper.__wrapped__ = func
    _wrapper.is_step = True
    _wrapper.decorators = []
    _wrapper.wrappers = []
    _wrapper.config_decorators = []
    _wrapper._mf_edges = edges
    _wrapper._mf_dataflow = {
        "inputs": dict(inputs_map),
        "outputs": dict((n, n) for n in (produces_tuple or ())),
        "mode": mode,
    }
    _wrapper._mf_dataflow_entries = []
    if inputs_map:
        _wrapper._mf_dataflow_entries.append(
            {"kind": "explicit_inputs", "payload": {"inputs": dict(inputs_map)}}
        )
    if produces_tuple:
        _wrapper._mf_dataflow_entries.append(
            {
                "kind": "explicit_outputs",
                "payload": {"outputs": list(produces_tuple)},
            }
        )
    if mode == "advanced_r8":
        _wrapper._mf_dataflow_entries.append(
            {
                "kind": "embedded_callable",
                "payload": {
                    "callable_qualname": getattr(func, "__qualname__", repr(func))
                },
            }
        )
    _wrapper._mf_defaults = dict(defaults)
    _wrapper._mf_added_by_mutator = True
    return _wrapper


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
            A tuple containing the decorator name, its fully qualified name,
            a list of positional arguments, and a dictionary of keyword arguments.
        """
        from metaflow.flowspec import FlowStateItems
        from .user_flow_decorator import FlowMutator, FlowMutatorMeta

        flow_decos = self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS]
        for decos in flow_decos.values():
            for deco in decos:
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

        for deco in self._flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]:
            if isinstance(deco, FlowMutator):
                short_name = FlowMutatorMeta.get_decorator_name(deco.__class__)
                r = [
                    short_name or deco.__class__.__name__,
                    deco.decorator_name,
                    list(deco._args),
                    dict(deco._kwargs),
                ]
                yield tuple(r)

    def has_decorator(self, name: str) -> bool:
        """Check whether this flow has at least one decorator with the given name.

        Parameters
        ----------
        name : str
            The decorator name (short) or fully qualified name (contains a period).
        """
        for deco_name, deco_fq, _args, _kwargs in self.decorator_specs:
            if self._match_name(deco_name, deco_fq, name):
                return True
        return False

    def get_decorator_specs(
        self, name: str
    ) -> List[Tuple[str, str, List[Any], Dict[str, Any]]]:
        """Return all spec tuples matching the given name.

        Parameters
        ----------
        name : str
            The decorator name (short) or fully qualified name (contains a period).

        Returns
        -------
        List[Tuple[str, str, List[Any], Dict[str, Any]]]
            A list of (short_name, fq_name, args, kwargs) tuples. Empty list if
            no decorators match.
        """
        return [
            (deco_name, deco_fq, list(args), dict(kwargs))
            for deco_name, deco_fq, args, kwargs in self.decorator_specs
            if self._match_name(deco_name, deco_fq, name)
        ]

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

        If you want to access a particular configuration value, you can use the getattr
        method or simply <MutableFlow>.step_name.

        Yields
        ------
        Tuple[str, ConfigValue]
            Iterates over the configurations of the flow
        """
        from metaflow.flowspec import FlowStateItems

        # When configs are parsed, they are loaded in _flow_state[FlowStateItems.CONFIGS]
        for name, (value, plain_flag) in self._flow_cls._flow_state[
            FlowStateItems.CONFIGS
        ].items():
            r = name, value if plain_flag or value is None else ConfigValue(value)
            debug.userconf_exec("Mutable flow yielding config: %s" % str(r))
            yield r

    @property
    def parameters(
        self,
    ) -> Generator[Tuple[str, "metaflow.parameters.Parameter"], None, None]:
        """
        Iterate over all the parameters in this flow.

        If you want to access a particular parameter, you can use the getattr method or
        simply <MutableFlow>.step_name.

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

        If you want to access a particular step, you can use the getattr method or simply
        <MutableFlow>.step_name.

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
        from metaflow.flowspec import FlowStateItems

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
        self._flow_cls._flow_state[FlowStateItems.CACHED_PARAMETERS] = None

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
        from metaflow.flowspec import FlowStateItems

        for var, param in self._flow_cls._get_parameters():
            if param.IS_CONFIG_PARAMETER:
                continue
            if param.name == parameter_name:
                delattr(self._flow_cls, var)
                debug.userconf_exec(
                    "Mutable flow removing parameter %s from flow" % var
                )
                # Reset so that we don't list it again
                self._flow_cls._flow_state[FlowStateItems.CACHED_PARAMETERS] = None
                return True
        debug.userconf_exec(
            "Mutable flow failed to remove parameter %s from flow" % parameter_name
        )
        return False

    def add_step(
        self,
        name: str,
        after: Union[str, List[str]],
        func: Callable,
        *,
        produces: Union[str, Tuple[str, ...], List[str], None] = None,
        inputs: Optional[Dict[str, str]] = None,
        outputs: Optional[Dict[str, str]] = None,
        decorators: Optional[List[Any]] = None,
        next_steps: Union[str, List[str], None] = None,
        overwrite: bool = False,
    ) -> "metaflow.user_decorators.mutable_step.MutableStep":
        """Insert a new step into the flow graph between ``after`` and its
        original successor.

        Two calling styles:

        - **Style A (default — pure-function fast path):** pass ``func`` and
          ``produces``. Inputs are inferred from ``inspect.signature(func)``;
          the return value is assigned to ``self.<produces>``. ``produces``
          may be a string or a tuple/list of strings for multi-return.

        - **Style B (advanced — explicit mapping):** pass ``inputs={...}``
          and/or ``outputs={...}`` to override the inferred naming. The
          dict keys are the consumer-internal names; the values are the
          producer-external artifact names.

        - **R8 advanced (sandboxed self):** ``func`` may take a single
          positional argument named ``self``. The wrapper invokes it
          through a ``_NamespacedSelf`` proxy. Inside the body,
          ``embedded("name")`` may be used to call helpers. Lint rule
          ``L-NS-007`` rejects ``embedded()`` outside R8 mode.

        ``add_step`` may only be called inside the ``pre_mutate`` method
        of a ``FlowMutator`` — matching the timing of ``add_parameter``.

        Parameters
        ----------
        name : str
            Name of the new step.
        after : str or list[str]
            Predecessor step(s). The new step is inserted between
            ``after`` and ``after``'s original successor(s).
        func : Callable
            User function implementing the step body (see styles above).
        produces : str or tuple[str, ...], optional
            Name(s) of artifact(s) the step assigns to ``self.<produces>``.
        inputs : dict[str, str], optional
            Explicit ``internal_name -> external_artifact_name`` map.
        outputs : dict[str, str], optional
            Explicit ``internal_name -> external_artifact_name`` map.
        decorators : list, optional
            Step decorators to attach to the synthesized wrapper.
        next_steps : str or list[str], optional
            Override the inherited successor of the new step. If not
            given, the step takes ``after``'s original successor.
        overwrite : bool, default False
            If True and ``name`` already binds to a class attribute,
            replace it (equivalent to ``remove_step(name)`` then add).

        Returns
        -------
        MutableStep
            A handle to the newly added step.
        """
        from metaflow.flowspec import FlowStateItems
        from .mutable_step import MutableStep

        if not self._pre_mutate:
            raise MetaflowException(
                "add_step(%r) from %s is only allowed in `pre_mutate`, "
                "not `mutate`." % (name, self._inserted_by)
            )

        # Normalize predecessor list and next override.
        after_list = [after] if isinstance(after, str) else list(after or [])
        next_override: Optional[List[str]] = None
        if next_steps is not None:
            next_override = (
                [next_steps] if isinstance(next_steps, str) else list(next_steps)
            )

        # Signature inspection and rule check.
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError) as e:
            raise TypeError(
                "add_step: cannot introspect signature of %r: %s" % (func, e)
            )
        mode = _check_signature_rules(func, sig)

        # Mutable-default rejection (UD-9) runs only in Style A.
        if mode != "advanced_r8":
            _reject_mutable_defaults(func, sig)

        produces_tuple = _normalize_produces(produces)
        inputs_map = _resolve_inputs_map(sig, inputs, mode)

        # Idempotency / dedup guard (PM-004 fix): key on
        # (flow_cls_qualname, step_name) — NOT id(self) — so the guard
        # fires across pytest re-imports and importlib.reload. The
        # PROCESSED_BY check MUST run BEFORE the class-attribute
        # collision check so that a same-spec re-call sees its own
        # prior registration as idempotent (the class attribute is
        # exactly the wrapper we installed last time).
        processed_key = (self._flow_cls.__qualname__, name)
        processed = self._flow_cls._flow_state.setdefault(
            FlowStateItems.PROCESSED_BY, {}
        )
        spec = _spec_signature(func, produces_tuple, tuple(after_list), decorators)
        if processed_key in processed:
            prior = processed[processed_key]
            if prior == spec:
                # Idempotent re-registration: return the existing wrapper as
                # a MutableStep handle.
                existing = getattr(self._flow_cls, name, None)
                return MutableStep(
                    self._flow_cls,
                    existing,
                    pre_mutate=self._pre_mutate,
                    statically_defined=self._statically_defined,
                    inserted_by=self._inserted_by,
                )
            raise MetaflowException(
                "add_step(%r): conflicting prior registration on %r "
                "with a different spec. Use overwrite=True or pick a "
                "unique name." % (name, self._flow_cls.__name__)
            )
        if overwrite and hasattr(self._flow_cls, name):
            # The collision check already passed (overwrite=True). Treat
            # this as remove_step + add_step. Append the remove op first
            # so post-graph-build rewires fire in order.
            self.remove_step(name)

        # Class-attribute collision check — runs AFTER the PROCESSED_BY
        # idempotency check so a same-spec re-call short-circuits above
        # rather than tripping a spurious collision on the wrapper we
        # installed previously.
        if hasattr(self._flow_cls, name) and not overwrite:
            raise MetaflowException(
                "Flow %r already has a class member %r — set "
                "overwrite=True in add_step to replace it."
                % (self._flow_cls.__name__, name)
            )

        # Wrapper synthesis — share `edges` dict by reference with both
        # the wrapper closure and the `_mf_edges` attribute so post-
        # graph-build edge rewiring is visible at runtime.
        edges = {
            "in": list(after_list),
            "out": [],
            "type": "linear",
        }
        wrapper = _synthesize_wrapper(
            name=name,
            func=func,
            sig=sig,
            mode=mode,
            inputs_map=inputs_map,
            produces=produces_tuple,
            edges=edges,
        )
        if decorators:
            wrapper.decorators = list(decorators)

        # Record the source file of the user func so packaging picks it
        # up for remote runners.
        try:
            source_file = inspect.getfile(getattr(func, "__wrapped__", func))
        except (OSError, TypeError):
            source_file = None
        if source_file:
            packaged = self._flow_cls._flow_state.setdefault(
                FlowStateItems.PACKAGED_CALLABLES, []
            )
            packaged.append(source_file)

        # Install the wrapper on the flow class.
        setattr(self._flow_cls, name, wrapper)

        # Record the operation so FlowGraph._apply_graph_mutations runs
        # edge rewiring in declaration order across this and subsequent
        # add_step / remove_step calls.
        ops = self._flow_cls._flow_state.setdefault(FlowStateItems.GRAPH_MUTATIONS, [])
        ops.append(("add", name, list(after_list), next_override))
        processed[processed_key] = spec

        debug.userconf_exec(
            "Mutable flow add_step: %r after=%r produces=%r mode=%s"
            % (name, after_list, produces_tuple, mode)
        )
        return MutableStep(
            self._flow_cls,
            wrapper,
            pre_mutate=self._pre_mutate,
            statically_defined=self._statically_defined,
            inserted_by=self._inserted_by,
        )

    def remove_step(self, name: str) -> bool:
        """Remove a step from the flow graph by name.

        Edge rewiring: every node whose ``out_funcs`` referenced the
        removed step now references the removed step's successors
        (typically a single linear successor). Removing a start step
        or end step is rejected.

        Parameters
        ----------
        name : str
            Name of the step to remove.

        Returns
        -------
        bool
            ``True`` if a step was removed; ``False`` if no class
            attribute by that name was a step.
        """
        from metaflow.flowspec import FlowStateItems

        if not self._pre_mutate:
            raise MetaflowException(
                "remove_step(%r) from %s is only allowed in `pre_mutate`, "
                "not `mutate`." % (name, self._inserted_by)
            )

        attr = getattr(self._flow_cls, name, None)
        if attr is None or not getattr(attr, "is_step", False):
            debug.userconf_exec("Mutable flow remove_step: no step named %r" % name)
            return False
        if getattr(attr, "is_start_step", False):
            raise MetaflowException(
                "remove_step(%r): refusing to remove a start step." % name
            )
        if getattr(attr, "is_end_step", False):
            raise MetaflowException(
                "remove_step(%r): refusing to remove an end step." % name
            )

        # Delete the class attribute so graph._create_nodes does not
        # build a DAGNode for it. Edge rewiring happens in
        # FlowGraph._apply_graph_mutations using the recorded op.
        try:
            delattr(self._flow_cls, name)
        except AttributeError:
            # Inherited attribute — shadow with a sentinel so dir(cls)
            # discovery sees no step. Phase 1 only supports removing
            # steps defined on (or added to) the immediate class.
            raise MetaflowException(
                "remove_step(%r): cannot remove an inherited step in "
                "Phase 1. Define it directly on %r to remove it."
                % (name, self._flow_cls.__name__)
            )

        ops = self._flow_cls._flow_state.setdefault(FlowStateItems.GRAPH_MUTATIONS, [])
        ops.append(("remove", name))

        # Allow the same name to be re-added after removal — drop the
        # PROCESSED_BY entry so the dedup guard does not block it.
        processed = self._flow_cls._flow_state.setdefault(
            FlowStateItems.PROCESSED_BY, {}
        )
        processed.pop((self._flow_cls.__qualname__, name), None)

        debug.userconf_exec("Mutable flow remove_step: removed %r" % name)
        return True

    def add_decorator(
        self,
        deco_type: Union[
            partial, "metaflow.user_decorators.user_flow_decorator.FlowMutator", str
        ],
        deco_args: Optional[List[Any]] = None,
        deco_kwargs: Optional[Dict[str, Any]] = None,
        duplicates: int = IGNORE,
    ) -> Optional[
        Union[
            "metaflow.decorators.FlowDecorator",
            "metaflow.user_decorators.user_flow_decorator.FlowMutator",
        ]
    ]:
        """
        Add a Metaflow flow-decorator or a FlowMutator to a flow. You can only add
        decorators in the `pre_mutate` method.

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

        You can also add a FlowMutator class. The new FlowMutator will have its
        ``external_init()`` called immediately and its ``pre_mutate`` will be called after
        all previously existing FlowMutators have called pre-mutate.

        Parameters
        ----------
        deco_type : Union[partial, FlowMutator, str]
            The decorator class to add to this flow. Can be a FlowDecorator partial,
            a FlowMutator subclass, or a string decorator specification.
        deco_args : List[Any], optional, default None
            Positional arguments to pass to the decorator.
        deco_kwargs : Dict[str, Any], optional, default None
            Keyword arguments to pass to the decorator.
        duplicates : int, default MutableFlow.IGNORE
            Instruction on how to handle duplicates. It can be one of:
            - `MutableFlow.IGNORE`: Ignore the decorator if it already exists.
            - `MutableFlow.ERROR`: Raise an error if the decorator already exists.
            - `MutableFlow.OVERRIDE`: Remove the existing decorator and add this one.

        Returns
        -------
        Optional[Union[FlowDecorator, FlowMutator]]
            The decorator that was added or None if none was added due to duplicate handling.

        """
        if not self._pre_mutate:
            raise MetaflowException(
                "Adding flow-decorator '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method"
                % (
                    (
                        deco_type
                        if isinstance(deco_type, str)
                        else getattr(deco_type, "name", None)
                        or getattr(deco_type, "decorator_name", deco_type)
                    ),
                    self._inserted_by,
                )
            )
        # Prevent circular import
        from metaflow.decorators import (
            DuplicateFlowDecoratorException,
            FlowDecorator,
            extract_flow_decorator_from_decospec,
        )
        from metaflow.flowspec import FlowStateItems

        deco_args = deco_args or []
        deco_kwargs = deco_kwargs or {}

        def _add_flow_decorator(flow_deco):
            # NOTE: Here we operate not on self_data or inherited_data because mutators
            # are processed on the end flow anyways (they can come from any of the base
            # flow classes but they only execute on the flow actually being run). This makes
            # it easier particularly for the case of OVERRIDE where we need to override
            # a decorator that could be in either of the inherited or self dictionaries.
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
                flow_decos = self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS]

                flow_decos.setdefault(flow_deco.name, []).append(flow_deco)
                debug.userconf_exec(
                    "Mutable flow adding flow decorator '%s'" % deco_type
                )
                return flow_deco

            # self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS] is a  dictionary of form :
            # <deco_name> : [deco_instance, deco_instance, ...]
            flow_decos = self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS]
            existing_deco = [d for d in flow_decos if d == flow_deco.name]

            if flow_deco.allow_multiple or not existing_deco:
                return _do_add()
            elif duplicates == MutableFlow.IGNORE:
                # If we ignore, we do not add the decorator
                debug.userconf_exec(
                    "Mutable flow ignoring flow decorator '%s'"
                    "(already exists and duplicates are ignored)" % flow_deco.name
                )
                return None
            elif duplicates == MutableFlow.OVERRIDE:
                # If we override, we remove the existing decorator and add this one
                debug.userconf_exec(
                    "Mutable flow overriding flow decorator '%s' "
                    "(removing existing decorator and adding new one)" % flow_deco.name
                )
                flow_decos = self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS]
                self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS] = {
                    d: flow_decos[d] for d in flow_decos if d != flow_deco.name
                }
                return _do_add()
            elif duplicates == MutableFlow.ERROR:
                # If we error, we raise an exception
                if self._statically_defined:
                    raise DuplicateFlowDecoratorException(flow_deco.name)
                else:
                    debug.userconf_exec(
                        "Mutable flow ignoring flow decorator '%s' "
                        "(already exists and non statically defined)" % flow_deco.name
                    )
                return None
            else:
                raise ValueError("Invalid duplicates value: %s" % duplicates)

        def _add_flow_mutator(flow_mutator):
            flow_mutator.statically_defined = self._statically_defined
            flow_mutator.inserted_by = self._inserted_by
            flow_mutator._flow_cls = self._flow_cls

            def _do_add():
                # Call external_init BEFORE appending to lists so that if it
                # fails the mutator is not left half-registered.
                flow_mutator.external_init()
                # Append to the merged list (the one being iterated in the
                # pre_mutate loop) so the new mutator's pre_mutate is called
                # naturally by the ongoing iteration.
                merged_mutators = self._flow_cls._flow_state[
                    FlowStateItems.FLOW_MUTATORS
                ]
                merged_mutators.append(flow_mutator)
                # Also append to the underlying _self_data so a cache rebuild
                # includes this mutator. We use _self_data directly (not the
                # self_data property) to avoid clearing the merged cache.
                self._flow_cls._flow_state._self_data[
                    FlowStateItems.FLOW_MUTATORS
                ].append(flow_mutator)
                debug.userconf_exec(
                    "Mutable flow adding flow mutator '%s'"
                    % flow_mutator.decorator_name
                )
                return flow_mutator

            # Check for existing mutator with the same decorator_name
            existing_ids = {
                id(m)
                for m in self._flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]
                if hasattr(m, "decorator_name")
                and m.decorator_name == flow_mutator.decorator_name
            }

            if not existing_ids:
                return _do_add()
            elif duplicates == MutableFlow.IGNORE:
                debug.userconf_exec(
                    "Mutable flow ignoring flow mutator '%s' "
                    "(already exists and duplicates are ignored)"
                    % flow_mutator.decorator_name
                )
                return None
            elif duplicates == MutableFlow.OVERRIDE:
                debug.userconf_exec(
                    "Mutable flow overriding flow mutator '%s' "
                    "(removing existing mutator and adding new one)"
                    % flow_mutator.decorator_name
                )
                # Filter out existing entries from both merged and _self_data.
                # Use slice-assignment on merged to mutate in place (the outer
                # pre_mutate for-loop holds a reference to this list object).
                merged = self._flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]
                merged[:] = [m for m in merged if id(m) not in existing_ids]
                self_list = self._flow_cls._flow_state._self_data[
                    FlowStateItems.FLOW_MUTATORS
                ]
                self_list[:] = [m for m in self_list if id(m) not in existing_ids]
                return _do_add()
            elif duplicates == MutableFlow.ERROR:
                if self._statically_defined:
                    raise MetaflowException(
                        "Duplicate FlowMutator '%s' on flow"
                        % flow_mutator.decorator_name
                    )
                else:
                    debug.userconf_exec(
                        "Mutable flow ignoring flow mutator '%s' "
                        "(already exists and non statically defined)"
                        % flow_mutator.decorator_name
                    )
                return None
            else:
                raise ValueError("Invalid duplicates value: %s" % duplicates)

        # Check if this is a FlowMutator class or instance
        from .user_flow_decorator import FlowMutator

        # If deco_type is a string, we want to parse it to a decospec
        if isinstance(deco_type, str):
            flow_deco, has_args_kwargs = extract_flow_decorator_from_decospec(deco_type)
            if (deco_args or deco_kwargs) and has_args_kwargs:
                raise MetaflowException(
                    "Cannot specify additional arguments when adding a flow decorator "
                    "using a decospec that already contains arguments"
                )
            if isinstance(flow_deco, FlowMutator):
                # String resolved to a FlowMutator instance — route it through
                # the FlowMutator addition path
                return _add_flow_mutator(flow_deco)
            return _add_flow_decorator(flow_deco)

        if isinstance(deco_type, type) and issubclass(deco_type, FlowMutator):
            d = deco_type(*deco_args, **deco_kwargs)
            return _add_flow_mutator(d)

        # Validate deco_type
        if (
            not isinstance(deco_type, partial)
            or len(deco_type.args) != 1
            or not issubclass(deco_type.args[0], FlowDecorator)
        ):
            raise TypeError("add_decorator takes a FlowDecorator or FlowMutator")

        deco_type = deco_type.args[0]
        return _add_flow_decorator(
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

        FlowMutators cannot be removed because they are processed during iteration.
        Attempting to remove a FlowMutator will raise an error.

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

        # Prevent circular import
        from metaflow.flowspec import FlowStateItems
        from .user_flow_decorator import FlowMutator, FlowMutatorMeta

        if not self._pre_mutate:
            raise MetaflowException(
                "Removing flow-decorator '%s' from %s is only allowed in the `pre_mutate` "
                "method and not the `mutate` method" % (deco_name, self._inserted_by)
            )

        # Check if the name matches a FlowMutator — these cannot be removed
        # because the pre_mutate loop is iterating over them.
        for deco in self._flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]:
            if isinstance(deco, FlowMutator):
                if self._match_name(
                    FlowMutatorMeta.get_decorator_name(deco.__class__)
                    or deco.__class__.__name__,
                    deco.decorator_name,
                    deco_name,
                ):
                    raise MetaflowException(
                        "Cannot remove FlowMutator '%s'. FlowMutators are processed "
                        "during iteration and cannot be removed. Only FlowDecorators "
                        "(e.g. @project, @schedule) can be removed." % deco_name
                    )

        do_all = deco_args is None and deco_kwargs is None
        did_remove = False
        flow_decos = self._flow_cls._flow_state[FlowStateItems.FLOW_DECORATORS]

        if do_all and deco_name in flow_decos:
            del flow_decos[deco_name]
            return True
        old_deco_list = flow_decos.get(deco_name)
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
            flow_decos[deco_name] = new_deco_list
        else:
            del flow_decos[deco_name]
        return did_remove

    def _match_name(self, deco_name: str, deco_fq: str, query: str) -> bool:
        """Return True if *query* matches either the short or fully-qualified name.

        A query containing a period is compared against the fully-qualified name;
        otherwise it is compared against the short name.
        """
        if "." in query:
            return deco_fq == query
        return deco_name == query

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
