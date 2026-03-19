import os

from metaflow._vendor import click

from typing import Any, Dict, List, Optional, Union

# Sentinel for "no default provided" — distinct from None which is a valid default.
_NO_DEFAULT = object()

# Module-level store for resolved dynamic var values.
# Populated in the child subprocess from the pickle file passed via
# --dynamic-var-file.  Read by resolve_dynamic_vars_from_store()
_resolved_store: Optional[Dict[str, Any]] = None
_resolved_split_index: Optional[int] = None


def set_dynamic_var_store(values: Dict[str, Any], split_index: Optional[int] = None):
    """Set the module-level store of resolved dynamic var values."""
    global _resolved_store, _resolved_split_index
    _resolved_store = values
    _resolved_split_index = split_index


class DynamicVarFileInput(click.Path):
    """Click type that reads resolved dynamic var values from a pickle file.

    This is kept in dynamic_var.py (rather than cli.py) so that class identity
    is stable even when cli.py is reloaded by the Runner subprocess machinery.
    """

    name = "DynamicVarFileInput"

    def convert(self, value, param, ctx):
        import pickle

        v = super().convert(value, param, ctx)
        with open(v, "rb") as f:
            payload = pickle.load(f)

        values, split_index = payload

        set_dynamic_var_store(values, split_index)
        try:
            os.remove(v)
        except OSError:
            pass
        return v


def resolve_dynamic_vars_from_store(attributes: Dict[str, Any]) -> Dict[str, Any]:
    """Replace DynamicVar markers in *attributes* using the global store.

    If the store has not been populated (outermost process), this is a no-op and
    DynamicVar will remain
    """
    if _resolved_store is None:
        return attributes
    return _resolve_val(attributes, _resolved_store, _resolved_split_index)


class DynamicVar:
    """Marker for decorator attributes resolved from parent step artifacts at runtime.

    A ``DynamicVar`` is created by calling :func:`var` in a decorator parameter.
    It tells the orchestrator to resolve the value at runtime from an artifact
    set in the parent step. You can use `var` in:
     - decorator attributes provided the decorator supports it for that attribute; an
       error will be raised if it does not.
     - in any UserStepDecorator's constructor. Given the way decorators are initialized,
       this is supported in a transparent way for all UserStepDecorators.
    You cannot, however, use `var` in FlowMutators or StepMutators because the value
    of the variable is not known when the mutators need to operate (and there is no useful
    mutator code that executes *after* the value becomes known).

    Backend orchestrators may also not support dynamic values even if the decorator supports
    it. An error will be raised if that is the case.

    For decorator writers, to accept `var` in decorator attributes, you must not need
    to know the value *before* task_pre_step (that is the first time in the decorator's
    lifecycle when the value is guaranteed to be available). If you need the value to
    be available before task_pre_step and cannot delay it until task_pre_step, you cannot
    accept a `var` value for that attribute.
    """

    def __init__(self, var_name, pertask=False, default=_NO_DEFAULT):
        self.var_name = var_name
        self.pertask = pertask
        self.default = default

    def __repr__(self):
        parts = ["var('%s'" % self.var_name]
        if self.pertask:
            parts.append("pertask=True")
        if self.default is not _NO_DEFAULT:
            parts.append("default=%r" % (self.default,))
        return ", ".join(parts) + ")"


def var(var_name, pertask=False, default=_NO_DEFAULT):
    """
    Mark a decorator parameter to be resolved dynamically from a parent step artifact.

    Use this in decorator parameters to indicate that the value should come from
    ``self.<var_name>`` available in the parent step. For example::

        @titus(cpu=var("num_cpu"))
        @step
        def compute(self):
            ...

    means "use the value of ``self.num_cpu`` from the parent step as the cpu parameter."

    When ``pertask=True``, the artifact is expected to be a list or dict. The runtime
    indexes into it using the split index of the current task (foreach index or
    parallel node index). For a list, ``collection[split_index]`` is used. For a
    dict, ``collection[split_index]`` is used (or ``collection.get(split_index, default)``
    if *default* is provided).

    Availability
    ------------
    ``var()`` is currently supported when executing locally (or with remote compute) only.
    Deploying a flow that uses ``var()`` to Argo Workflows, Airflow, or
    AWS Step Functions will raise an error at compile time.

    Parameters
    ----------
    var_name : str
        Name of the flow artifact to read from the parent step.
    pertask : bool, default False
        If True, the artifact is a collection (list or dict) and the runtime will
        index into it using the current task's split index.
    default : optional
        Default value when ``pertask=True`` and the split index is not found in a dict.
        Only meaningful when the artifact is a dict.

    Returns
    -------
    DynamicVar
        A marker object that will be resolved at runtime.
    """
    return DynamicVar(var_name, pertask=pertask, default=default)


def _contains_dynamic_var(val):
    """Check if val or any nested structure contains a DynamicVar."""
    if isinstance(val, DynamicVar):
        return True
    if isinstance(val, dict):
        return any(
            _contains_dynamic_var(k) or _contains_dynamic_var(v) for k, v in val.items()
        )
    if isinstance(val, (list, tuple, set)):
        return any(_contains_dynamic_var(x) for x in val)
    return False


def collect_dynamic_var_names(val):
    """Recursively collect all DynamicVar.var_name values from a nested structure."""
    if isinstance(val, DynamicVar):
        return {val.var_name}
    if isinstance(val, dict):
        result = set()
        for k, v in val.items():
            result |= collect_dynamic_var_names(k)
            result |= collect_dynamic_var_names(v)
        return result
    if isinstance(val, (list, tuple, set)):
        result = set()
        for x in val:
            result |= collect_dynamic_var_names(x)
        return result
    return set()


def resolve_dynamic_vars(
    attributes: Union[Dict[str, Any], List[Any]],
    source: Any,
    split_index: Optional[int] = None,
):
    """Recursively replace DynamicVar with resolved values.

    *source* can be a dict mapping artifact names to values, or a FlowSpec
    instance (for the user-step-decorator path that resolves via ``getattr``).

    *split_index* is required when any DynamicVar has ``pertask=True``.
    """
    return _resolve_val(attributes, source, split_index)


def _resolve_val(val, source, split_index=None):
    """Resolve *val* recursively.

    *source* can be either:
    - a ``dict`` mapping artifact names to values (parent-process / store path), or

    *split_index* is the foreach/parallel index for ``pertask=True`` vars.
    """
    if isinstance(val, DynamicVar):
        if isinstance(source, dict):
            collection = source[val.var_name]
        else:
            # source is a FlowSpec instance (UserStepDecorator path)
            collection = getattr(source, val.var_name)

        if not val.pertask:
            return collection

        # pertask=True: index into the collection using split_index.
        # When split_index is None the resolution is happening for a step
        # that is not part of a split (e.g., a join step whose child process
        # also initializes decorators of sibling steps).  In that case return
        # the raw collection — the decorator won't use it for this step.
        if split_index is None:
            return collection

        if isinstance(collection, list):
            return collection[split_index]
        elif isinstance(collection, dict):
            if val.default is not _NO_DEFAULT:
                return collection.get(split_index, val.default)
            return collection[split_index]
        else:
            from .exception import MetaflowException

            raise MetaflowException(
                "var('%s', pertask=True) expects the artifact to be a list or "
                "dict but got %s." % (val.var_name, type(collection).__name__)
            )

    if isinstance(val, dict):
        return {
            _resolve_val(k, source, split_index): _resolve_val(v, source, split_index)
            for k, v in val.items()
        }
    if isinstance(val, list):
        return [_resolve_val(x, source, split_index) for x in val]
    if isinstance(val, tuple):
        return tuple(_resolve_val(x, source, split_index) for x in val)
    if isinstance(val, set):
        return {_resolve_val(x, source, split_index) for x in val}
    return val


def get_dynamic_vars(attributes):
    """Returns {attr_key: DynamicVar} for top-level keys whose values contain DynamicVar.

    For backwards compatibility, when the top-level value is itself a DynamicVar
    the dict maps key -> DynamicVar directly.  When the DynamicVar is nested, the
    key is still included so callers know which attributes are dynamic.
    """
    return {k: v for k, v in attributes.items() if _contains_dynamic_var(v)}


def has_dynamic_vars(attributes: Union[Dict[str, Any], List[Any]]):
    """Returns True if any attribute value (recursively) contains a DynamicVar."""
    if isinstance(attributes, dict):
        return any(_contains_dynamic_var(v) for v in attributes.values())
    return any(_contains_dynamic_var(x) for x in attributes)


def check_no_dynamic_vars(graph, orchestrator_name):
    """Raise MetaflowException if any decorator in the graph uses var().

    Call this at the top of compile/create methods for orchestrators that
    do not support dynamic decorator values.
    """
    from metaflow.exception import MetaflowException

    for node in graph:
        for deco in node.decorators:
            if has_dynamic_vars(deco.attributes):
                # Collect all var names for a helpful error message
                var_names = set()
                for k, v in deco.attributes.items():
                    var_names |= collect_dynamic_var_names(v)
                attr_list = ", ".join("var('%s')" % name for name in sorted(var_names))
                raise MetaflowException(
                    "Step *%s* uses var() in @%s (%s). "
                    "Deploying flows with var() to %s is not supported. "
                    "var() is currently only supported with Maestro."
                    % (node.name, deco.name, attr_list, orchestrator_name)
                )
        for wrapper in getattr(node, "wrappers", []):
            args_vars = collect_dynamic_var_names(wrapper._args)
            kwargs_vars = collect_dynamic_var_names(wrapper._kwargs)
            var_names = args_vars | kwargs_vars
            if var_names:
                attr_list = ", ".join("var('%s')" % name for name in sorted(var_names))
                raise MetaflowException(
                    "Step *%s* uses var() in @%s (%s). "
                    "Deploying flows with var() to %s is not supported. "
                    "var() is currently only supported with Maestro."
                    % (node.name, type(wrapper).__name__, attr_list, orchestrator_name)
                )
