import re
from .exception import MetaflowException
from .util import all_equal


class LintWarn(MetaflowException):
    headline = "Validity checker found an issue"


class FlowLinter(object):
    def __init__(self):
        self.require_static_graph = True
        self.require_fundamentals = True
        self.require_acyclicity = True
        self.require_non_nested_foreach = False
        self._checks = []

    def _decorate(self, setting, f):
        f.attrs.append(setting)
        return f

    def ensure_static_graph(self, f):
        return self._decorate("require_static_graph", f)

    def ensure_fundamentals(self, f):
        return self._decorate("require_fundamentals", f)

    def ensure_acyclicity(self, f):
        return self._decorate("require_acyclicity", f)

    def ensure_non_nested_foreach(self, f):
        return self._decorate("require_non_nested_foreach", f)

    def check(self, f):
        self._checks.append(f)
        f.attrs = []
        return f

    def run_checks(self, graph, **kwargs):
        for check in self._checks:
            if any(getattr(self, attr) or kwargs.get(attr) for attr in check.attrs):
                check(graph)


linter = FlowLinter()


@linter.ensure_fundamentals
@linter.check
def check_reserved_words(graph):
    RESERVED = {"name", "next", "input", "index", "cmd"}
    msg = "Step name *%s* is a reserved word. Choose another name for the " "step."
    for node in graph:
        if node.name in RESERVED:
            raise LintWarn(msg % node.name, node.func_lineno, node.source_file)


@linter.ensure_fundamentals
@linter.check
def check_basic_steps(graph):
    if graph.start_step is None:
        annotated = [name for name, node in graph.nodes.items() if node.is_start_step]
        if len(annotated) > 1:
            raise LintWarn(
                "Multiple steps annotated with @step(start=True): %s. "
                "Exactly one is allowed." % ", ".join(sorted(annotated))
            )
        raise LintWarn(
            "Your flow must have exactly one start step. Either name a step "
            "'start' or use @step(start=True)."
        )
    if graph.end_step is None:
        annotated = [name for name, node in graph.nodes.items() if node.is_end_step]
        if len(annotated) > 1:
            raise LintWarn(
                "Multiple steps annotated with @step(end=True): %s. "
                "Exactly one is allowed." % ", ".join(sorted(annotated))
            )
        raise LintWarn(
            "Your flow must have exactly one end step. Either name a step "
            "'end' or use @step(end=True)."
        )


@linter.ensure_fundamentals
@linter.check
def check_annotation_name_conflict(graph):
    """Detect conflict between @step(start/end=True) and legacy step names."""
    if (
        graph.start_step is not None
        and graph.start_step != "start"
        and "start" in graph.nodes
    ):
        raise LintWarn(
            "Ambiguous start step: step '%s' is annotated with @step(start=True) "
            "but a step named 'start' also exists. Remove the 'start' name or "
            "the @step(start=True) annotation." % graph.start_step
        )

    if graph.end_step is not None and graph.end_step != "end" and "end" in graph.nodes:
        raise LintWarn(
            "Ambiguous end step: step '%s' is annotated with @step(end=True) "
            "but a step named 'end' also exists. Remove the 'end' name or "
            "the @step(end=True) annotation." % graph.end_step
        )


@linter.ensure_static_graph
@linter.check
def check_start_end_degree(graph):
    """Validate that the start step has no incoming and the end step has no outgoing."""
    if graph.start_step is None or graph.end_step is None:
        return

    start_node = graph[graph.start_step]
    if start_node.in_funcs:
        raise LintWarn(
            "The start step *%s* has incoming transitions from %s. "
            "A start step must have no incoming transitions."
            % (graph.start_step, ", ".join(start_node.in_funcs)),
            start_node.func_lineno,
            start_node.source_file,
        )

    end_node = graph[graph.end_step]
    if end_node.out_funcs:
        raise LintWarn(
            "The end step *%s* has outgoing transitions. "
            "An end step must have no outgoing transitions (no self.next())."
            % graph.end_step,
            end_node.func_lineno,
            end_node.source_file,
        )


@linter.ensure_static_graph
@linter.check
def check_that_end_is_end(graph):
    if graph.end_step is None:
        return

    node = graph[graph.end_step]
    msg0 = (
        "The terminal step *%s* should not have a self.next() transition. "
        "Just remove it." % graph.end_step
    )
    msg1 = (
        "The terminal step *%s* should not be a join step (it gets an extra "
        "argument). Add a join step before it." % graph.end_step
    )

    if node.has_tail_next or node.invalid_tail_next:
        raise LintWarn(msg0, node.tail_next_lineno, node.source_file)
    if node.num_args > 1:
        raise LintWarn(msg1, node.tail_next_lineno, node.source_file)


@linter.ensure_fundamentals
@linter.check
def check_step_names(graph):
    msg = (
        "Step *{0.name}* has an invalid name. Only lowercase ascii "
        "characters, underscores, and digits are allowed."
    )
    for node in graph:
        if re.search("[^a-z0-9_]", node.name) or node.name[0] == "_":
            raise LintWarn(msg.format(node), node.func_lineno, node.source_file)


@linter.ensure_fundamentals
@linter.check
def check_num_args(graph):
    msg0 = (
        "Step {0.name} has too many arguments. Normal steps take only "
        "'self' as an argument. Join steps take 'self' and 'inputs'."
    )
    msg1 = (
        "Step *{0.name}* is both a join step (it takes an extra argument) "
        "and a split step (it transitions to multiple steps). This is not "
        "allowed. Add a new step so that split and join become separate steps."
    )
    msg2 = "Step *{0.name}* is missing the 'self' argument."
    for node in graph:
        if node.num_args > 2:
            raise LintWarn(msg0.format(node), node.func_lineno, node.source_file)
        elif node.num_args == 2 and node.type != "join":
            raise LintWarn(msg1.format(node), node.func_lineno, node.source_file)
        elif node.num_args == 0:
            raise LintWarn(msg2.format(node), node.func_lineno, node.source_file)


@linter.ensure_static_graph
@linter.check
def check_static_transitions(graph):
    msg = (
        "Step *{0.name}* is missing a self.next() transition to "
        "the next step. Add a self.next() as the last line in the "
        "function."
    )
    for node in graph:
        if node.type != "end" and not node.has_tail_next:
            raise LintWarn(msg.format(node), node.func_lineno, node.source_file)


@linter.ensure_static_graph
@linter.check
def check_valid_transitions(graph):
    msg = (
        "Step *{0.name}* specifies an invalid self.next() transition. "
        "Make sure the self.next() expression matches with one of the "
        "supported transition types:\n"
        "  • Linear: self.next(self.step_name)\n"
        "  • Fan-out: self.next(self.step1, self.step2, ...)\n"
        "  • Foreach: self.next(self.step, foreach='variable')\n"
        "  • Switch: self.next({{\"key\": self.step, ...}}, condition='variable')\n\n"
        "For switch statements, keys must be string literals, numbers or config expressions "
        "(self.config.key_name), not variables."
    )
    for node in graph:
        if node.type != "end" and node.has_tail_next and node.invalid_tail_next:
            raise LintWarn(msg.format(node), node.tail_next_lineno, node.source_file)


@linter.ensure_static_graph
@linter.check
def check_unknown_transitions(graph):
    msg = (
        "Step *{0.name}* specifies a self.next() transition to "
        "an unknown step, *{step}*."
    )
    for node in graph:
        unknown = [n for n in node.out_funcs if n not in graph]
        if unknown:
            raise LintWarn(
                msg.format(node, step=unknown[0]),
                node.tail_next_lineno,
                node.source_file,
            )


@linter.ensure_acyclicity
@linter.ensure_static_graph
@linter.check
def check_for_acyclicity(graph):
    msg = (
        "There is a loop in your flow: *{0}*. Break the loop "
        "by fixing self.next() transitions."
    )

    def check_path(node, seen):
        for n in node.out_funcs:
            if node.type == "split-switch" and n == node.name:
                continue
            if n in seen:
                path = "->".join(seen + [n])
                raise LintWarn(
                    msg.format(path), node.tail_next_lineno, node.source_file
                )
            else:
                check_path(graph[n], seen + [n])

    for start in graph:
        check_path(start, [])


@linter.ensure_static_graph
@linter.check
def check_for_orphans(graph):
    if graph.start_step is None:
        return

    msg = (
        "Step *{0.name}* is unreachable from the entry step *%s*. Add "
        "self.next({0.name}) in another step or remove *{0.name}*." % graph.start_step
    )
    seen = set([graph.start_step])

    def traverse(node):
        for n in node.out_funcs:
            if n not in seen:
                seen.add(n)
                traverse(graph[n])

    traverse(graph[graph.start_step])
    nodeset = frozenset(n.name for n in graph)
    orphans = nodeset - seen
    if orphans:
        orphan = graph[list(orphans)[0]]
        raise LintWarn(msg.format(orphan), orphan.func_lineno, orphan.source_file)


@linter.ensure_static_graph
@linter.check
def check_split_join_balance(graph):
    if graph.start_step is None or graph.end_step is None:
        return

    msg0 = (
        "The terminal step *{end}* was reached before a split started at step(s) "
        "*{roots}* were joined. Add a join step before *{end}*."
    )
    msg1 = (
        "Step *{0.name}* seems like a join step (it takes an extra input "
        "argument) but an incorrect number of steps (*{paths}*) lead to "
        "it. This join was expecting {num_roots} incoming paths, starting "
        "from split step(s) *{roots}*."
    )
    msg2 = (
        "Step *{0.name}* seems like a join step (it takes an extra input "
        "argument) but it is not preceded by a split. Ensure that there is "
        "a matching split for every join."
    )
    msg3 = (
        "Step *{0.name}* joins steps from unrelated splits. Ensure that "
        "there is a matching join for every split."
    )

    def traverse(node, split_stack):
        if node.type in ("start", "linear"):
            new_stack = split_stack
        elif node.type in ("split", "foreach"):
            new_stack = split_stack + [("split", node.out_funcs)]
        elif node.type == "split-switch":
            # For a switch, continue traversal down each path with the same stack
            for n in node.out_funcs:
                if node.type == "split-switch" and n == node.name:
                    continue
                traverse(graph[n], split_stack)
            return
        elif node.type == "end":
            new_stack = split_stack
            if split_stack:
                _, split_roots = split_stack.pop()
                roots = ", ".join(split_roots)
                raise LintWarn(
                    msg0.format(roots=roots, end=graph.end_step),
                    node.func_lineno,
                    node.source_file,
                )
        elif node.type == "join":
            new_stack = split_stack
            if split_stack:
                _, split_roots = split_stack[-1]
                new_stack = split_stack[:-1]

                # Resolve each incoming function to its root branch from the split.
                resolved_branches = set(
                    graph[n].split_branches[-1] for n in node.in_funcs
                )

                # compares the set of resolved branches against the expected branches
                # from the split.
                if len(resolved_branches) != len(
                    split_roots
                ) or resolved_branches ^ set(split_roots):
                    paths = ", ".join(resolved_branches)
                    roots = ", ".join(split_roots)
                    raise LintWarn(
                        msg1.format(
                            node, paths=paths, num_roots=len(split_roots), roots=roots
                        ),
                        node.func_lineno,
                        node.source_file,
                    )
            else:
                raise LintWarn(msg2.format(node), node.func_lineno, node.source_file)

            # check that incoming steps come from the same lineage
            # (no cross joins)
            def parents(n):
                if graph[n].type == "join":
                    return tuple(graph[n].split_parents[:-1])
                else:
                    return tuple(graph[n].split_parents)

            if not all_equal(map(parents, node.in_funcs)):
                raise LintWarn(msg3.format(node), node.func_lineno, node.source_file)
        else:
            new_stack = split_stack

        for n in node.out_funcs:
            if node.type == "split-switch" and n == node.name:
                continue
            traverse(graph[n], new_stack)

    traverse(graph[graph.start_step], [])


@linter.ensure_static_graph
@linter.check
def check_switch_splits(graph):
    """Check conditional split constraints"""
    msg0 = (
        "Step *{0.name}* is a switch split but defines {num} transitions. "
        "Switch splits must define at least 2 transitions."
    )
    msg1 = "Step *{0.name}* is a switch split but has no condition variable."
    msg2 = "Step *{0.name}* is a switch split but has no switch cases defined."

    for node in graph:
        if node.type == "split-switch":
            # Check at least 2 outputs
            if len(node.out_funcs) < 2:
                raise LintWarn(
                    msg0.format(node, num=len(node.out_funcs)),
                    node.func_lineno,
                    node.source_file,
                )

            # Check condition exists
            if not node.condition:
                raise LintWarn(
                    msg1.format(node),
                    node.func_lineno,
                    node.source_file,
                )

            # Check switch cases exist
            if not node.switch_cases:
                raise LintWarn(
                    msg2.format(node),
                    node.func_lineno,
                    node.source_file,
                )


@linter.ensure_static_graph
@linter.check
def check_empty_foreaches(graph):
    msg = (
        "Step *{0.name}* is a foreach split that has no children: "
        "it is followed immediately by a join step, *{join}*. Add "
        "at least one step between the split and the join."
    )
    for node in graph:
        if node.type == "foreach":
            joins = [n for n in node.out_funcs if graph[n].type == "join"]
            if joins:
                raise LintWarn(
                    msg.format(node, join=joins[0]), node.func_lineno, node.source_file
                )


@linter.ensure_static_graph
@linter.check
def check_parallel_step_after_next(graph):
    msg = (
        "Step *{0.name}* is called as a parallel step with self.next(num_parallel=..) "
        "but does not have a @parallel decorator."
    )
    for node in graph:
        if node.parallel_foreach and not all(
            graph[out_node].parallel_step for out_node in node.out_funcs
        ):
            raise LintWarn(msg.format(node), node.func_lineno, node.source_file)


@linter.ensure_static_graph
@linter.check
def check_join_followed_by_parallel_step(graph):
    msg = (
        "An @parallel step should be followed by a join step. Step *{0}* is called "
        "after an @parallel step but is not a join step. Please add an extra `inputs` "
        "argument to the step."
    )
    for node in graph:
        if node.parallel_step and not graph[node.out_funcs[0]].type == "join":
            raise LintWarn(
                msg.format(node.out_funcs[0]), node.func_lineno, node.source_file
            )


@linter.ensure_static_graph
@linter.check
def check_parallel_foreach_calls_parallel_step(graph):
    msg = (
        "Step *{0.name}* has a @parallel decorator, but is not called "
        "with self.next(num_parallel=...) from step *{1.name}*."
    )
    for node in graph:
        if node.parallel_step:
            for node2 in graph:
                if node2.out_funcs and node.name in node2.out_funcs:
                    if not node2.parallel_foreach:
                        raise LintWarn(
                            msg.format(node, node2), node.func_lineno, node.source_file
                        )


@linter.ensure_non_nested_foreach
@linter.check
def check_nested_foreach(graph):
    msg = (
        "Nested foreaches are not allowed: Step *{0.name}* is a foreach "
        "split that is nested under another foreach split."
    )
    for node in graph:
        if node.type == "foreach":
            if any(graph[p].type == "foreach" for p in node.split_parents):
                raise LintWarn(msg.format(node), node.func_lineno, node.source_file)


@linter.ensure_static_graph
@linter.check
def check_ambiguous_joins(graph):
    for node in graph:
        if node.type == "join":
            problematic_parents = [
                p_name
                for p_name in node.in_funcs
                if graph[p_name].type == "split-switch"
            ]
            if problematic_parents:
                msg = (
                    "A conditional path cannot lead directly to a join step.\n"
                    "In your conditional step(s) {parents}, one or more of the possible paths transition directly to the join step {join_name}.\n"
                    "As a workaround, please introduce an intermediate, unconditional step on that specific path before joining."
                ).format(
                    parents=", ".join("*%s*" % p for p in problematic_parents),
                    join_name="*%s*" % node.name,
                )
                raise LintWarn(msg, node.func_lineno, node.source_file)


# ---------------------------------------------------------------------------
# Phase 1 graph mutation: L-NS-005..009 — namespace-overlay lint rules.
# These checks only fire for steps that carry _mf_dataflow (mutator-added
# wrappers); on flows that do not call add_step/remove_step they are
# zero-cost no-ops, preserving full backward compatibility.
# ---------------------------------------------------------------------------


def _iter_ancestors(graph, node):
    """Yield every ancestor of ``node`` (transitive predecessors).

    Walks ``in_funcs`` (populated by ``_traverse_graph`` after edge
    rewiring). Yields ancestor DAGNodes in BFS order.
    """
    seen = set()
    frontier = list(node.in_funcs)
    while frontier:
        name = frontier.pop(0)
        if name in seen or name not in graph.nodes:
            continue
        seen.add(name)
        parent = graph.nodes[name]
        yield parent
        frontier.extend(parent.in_funcs)


def _node_outputs(node):
    """Return the set of external artifact names a node produces.

    Mutator-added steps declare outputs via ``_mf_dataflow["outputs"]``
    (a map of internal_name -> external_name). Regular ``@step`` nodes
    have no declared output surface; their ``self.<x> = ...`` writes
    are not statically analyzed in Phase 1 and are conservatively
    treated as "produces anything" (i.e. they never trigger L-NS-005).
    """
    df = getattr(node, "_mf_dataflow", None)
    if df is None:
        return None  # opaque — do not flag
    outputs_map = df.get("outputs", {}) or {}
    # Output map values are the producer-external names.
    return set(outputs_map.values())


@linter.ensure_fundamentals
@linter.check
def check_l_ns_005_unknown_input(graph):
    """L-NS-005: a mutator-added step declares an input that no ancestor
    statically declares as an output. Catches typos in produces=/inputs=
    at graph-build time before the step body runs at runtime."""
    for node in graph:
        df = getattr(node, "_mf_dataflow", None)
        if not df:
            continue
        inputs_map = df.get("inputs", {}) or {}
        if not inputs_map:
            continue
        # The "external" value is what we expect to read from the parent.
        # Walk ancestors; collect every declared output. If any ancestor
        # is opaque (regular @step), we cannot statically prove the input
        # is unknown — skip the check defensively.
        external_inputs = set(inputs_map.values())
        produced = set()
        opaque = False
        for ancestor in _iter_ancestors(graph, node):
            outs = _node_outputs(ancestor)
            if outs is None:
                opaque = True
                break
            produced |= outs
        if opaque:
            continue
        missing = external_inputs - produced
        if missing:
            raise LintWarn(
                "Step *%s* declares input(s) %s that no ancestor produces. "
                "Add `produces=` on the producing step or correct the "
                "name on the consumer."
                % (node.name, ", ".join("*%s*" % m for m in sorted(missing))),
                node.func_lineno,
                node.source_file,
            )


def _l_ns_006_shadow_names(cls):
    """Names that ``task.py:_init_parameters`` will install as class-level
    read-only properties on ``cls``. Mirrors the filter at task.py:232-240
    verbatim (including ``var in all_vars`` exclusion + bare
    ``getattr(cls, var)``). Used by L-NS-006 to detect overlay names that
    would be shadowed by a class-level property at runtime.
    """
    from types import MethodType, FunctionType

    NP = getattr(cls, "_NON_PARAMETERS", frozenset())
    all_vars = set()
    for var, _ in cls._get_parameters():
        all_vars.add(var)
    shadowed = set(all_vars)  # Parameters themselves install class properties
    for var in dir(cls):
        if var.startswith("_") or var in NP or var in all_vars:
            continue
        val = getattr(cls, var)
        if isinstance(val, (MethodType, FunctionType, property, type)):
            continue
        shadowed.add(var)
    return shadowed


@linter.ensure_fundamentals
@linter.check
def check_l_ns_006_param_or_constant_shadow(graph):
    """L-NS-006: a mutator-added step declares an internal input name
    that collides with a flow Parameter or a class-level constant.
    The class-level read-only property installed by
    ``task.py:_init_parameters`` would shadow the overlay branch at
    runtime, so the step would silently read the Parameter / constant
    value instead of the producer's artifact."""
    flow_cls = getattr(graph, "flow_cls", None)
    if flow_cls is None:
        return
    shadowed = _l_ns_006_shadow_names(flow_cls)
    if not shadowed:
        return
    for node in graph:
        df = getattr(node, "_mf_dataflow", None)
        if not df:
            continue
        inputs_map = df.get("inputs", {}) or {}
        collisions = sorted(set(inputs_map.keys()) & shadowed)
        if collisions:
            raise LintWarn(
                "Step *%s* declares input name(s) %s that collide with a "
                "flow Parameter or class-level constant. The overlay would "
                "be shadowed by the class property installed by "
                "_init_parameters; rename the input."
                % (node.name, ", ".join("*%s*" % c for c in collisions)),
                node.func_lineno,
                node.source_file,
            )


def _find_embedded_calls(src, module_name=None):
    """Yield (line_no, qualified_name) for every call to ``embedded(...)``
    in ``src``. Handles direct identifier (``embedded(...)``), qualified
    (``metaflow.embedded(...)``), and aliased import (``from metaflow
    import embedded as e; e(...)``). Runtime alias rebinding (``e =
    embedded; e(...)``) and ``getattr``-style dynamic resolution are
    NOT detected — documented as known false-negatives in
    ``docs/api/mutable_flow.md``.
    """
    import ast as _ast

    try:
        tree = _ast.parse(src)
    except (SyntaxError, ValueError):
        return
    # Walk top-level imports to build an alias table.
    aliases = set()  # local-name strings that refer to the embedded sentinel
    aliases.add("embedded")
    for stmt in _ast.walk(tree):
        if isinstance(stmt, _ast.ImportFrom):
            mod = stmt.module or ""
            if mod == "metaflow" or mod.endswith("._namespaced_self"):
                for alias in stmt.names:
                    if alias.name == "embedded":
                        aliases.add(alias.asname or alias.name)
    # Find Call nodes whose function resolves to an alias.
    for sub in _ast.walk(tree):
        if not isinstance(sub, _ast.Call):
            continue
        fn = sub.func
        if isinstance(fn, _ast.Name) and fn.id in aliases:
            yield (getattr(sub, "lineno", 0), fn.id)
        elif isinstance(fn, _ast.Attribute) and fn.attr == "embedded":
            # metaflow.embedded(...) qualified access
            yield (getattr(sub, "lineno", 0), "embedded")


@linter.ensure_fundamentals
@linter.check
def check_l_ns_007_embedded_scope(graph):
    """L-NS-007: ``embedded(...)`` may ONLY be called inside callables
    registered via ``MutableFlow.add_step(...)`` (and only in R8
    advanced mode — when the callable takes a single positional
    ``self`` argument). Calls in regular ``@step`` bodies or in Style A
    user funcs are rejected statically here.
    """
    import inspect as _inspect

    # (a) Scan @step bodies on the flow class (skip mutator wrappers,
    #     which never have a literal embedded() call in their source —
    #     the call is in the user func they wrap).
    flow_cls = getattr(graph, "flow_cls", None)
    if flow_cls is not None:
        for attr_name in vars(flow_cls):
            attr = vars(flow_cls)[attr_name]
            if not getattr(attr, "is_step", False):
                continue
            if getattr(attr, "_mf_added_by_mutator", False):
                continue
            try:
                src = _inspect.getsource(attr)
            except (OSError, TypeError):
                continue
            for lineno, _ in _find_embedded_calls(src, attr.__module__):
                raise LintWarn(
                    "embedded(...) is not allowed in @step body *%s* "
                    "(line %d). Register the helper via "
                    "MutableFlow.add_step(func=...) instead." % (attr_name, lineno),
                    lineno,
                    _inspect.getfile(attr),
                )

    # (b) Scan source of each callable registered via add_step
    #     (PACKAGED_CALLABLES is a list of source file paths; the actual
    #     callables live as wrappers on the class — we walk back via
    #     __wrapped__ on wrapper nodes).
    if flow_cls is None:
        return
    for node in graph:
        df = getattr(node, "_mf_dataflow", None)
        if not df:
            continue
        mode = df.get("mode")
        wrapper = getattr(flow_cls, node.name, None)
        if wrapper is None:
            continue
        user_func = getattr(wrapper, "__wrapped__", None)
        if user_func is None:
            continue
        try:
            src = _inspect.getsource(user_func)
        except (OSError, TypeError):
            continue
        found = list(_find_embedded_calls(src, user_func.__module__))
        if not found:
            continue
        if mode != "advanced_r8":
            lineno = found[0][0]
            raise LintWarn(
                "embedded(...) is not allowed in Style A user func *%s* "
                "(no self arg). Refactor the body to take a single "
                "positional `self` parameter (advanced mode R8) or "
                "remove the embedded() call."
                % getattr(user_func, "__qualname__", node.name),
                lineno,
                _inspect.getfile(user_func),
            )


@linter.ensure_fundamentals
@linter.check
def check_l_ns_008_join_rename_ambiguity(graph):
    """L-NS-008: a mutator-added join step declares an input that two or
    more upstream branches produce under DIFFERENT external names.
    ``inputs[i].<internal>`` resolution would be ambiguous because the
    overlay maps one external name; the join silently reads the wrong
    upstream's value otherwise."""
    for node in graph:
        if node.type != "join":
            continue
        df = getattr(node, "_mf_dataflow", None)
        if not df:
            continue
        inputs_map = df.get("inputs", {}) or {}
        if not inputs_map:
            continue
        # For each declared internal name on the join, walk both branches
        # and verify all upstream producers agree on the external name.
        for internal, external in inputs_map.items():
            disagreeing = []
            for branch_root_name in node.in_funcs:
                branch_root = graph.nodes.get(branch_root_name)
                if branch_root is None:
                    continue
                # Walk the branch's ancestors looking for any node that
                # declares an output mapping under a different external
                # name for the same logical artifact.
                for ancestor in [branch_root] + list(
                    _iter_ancestors(graph, branch_root)
                ):
                    a_df = getattr(ancestor, "_mf_dataflow", None)
                    if not a_df:
                        continue
                    outputs_map = a_df.get("outputs", {}) or {}
                    if internal in outputs_map and outputs_map[internal] != external:
                        disagreeing.append((ancestor.name, outputs_map[internal]))
            if disagreeing:
                pairs = ", ".join("*%s*->*%s*" % (n, e) for n, e in disagreeing)
                raise LintWarn(
                    "Join *%s* declares input *%s* (external *%s*) but "
                    "upstream branches disagree on the external name: %s"
                    % (node.name, internal, external, pairs),
                    node.func_lineno,
                    node.source_file,
                )


@linter.ensure_fundamentals
@linter.check
def check_l_ns_009_dangling_foreach_removal(graph):
    """L-NS-009: ``remove_step`` would orphan a foreach split or its
    matching join. Detect by spotting foreach splits whose
    matching_join is missing OR joins whose split_parent is missing.
    """
    for node in graph:
        if node.type == "foreach":
            mj = getattr(node, "matching_join", None)
            if mj is None or mj not in graph.nodes:
                raise LintWarn(
                    "Foreach split *%s* has no matching join after "
                    "graph mutation. remove_step likely removed the "
                    "join; re-add a join step or restore the original." % node.name,
                    node.func_lineno,
                    node.source_file,
                )
        elif node.type == "join":
            split_parents = getattr(node, "split_parents", []) or []
            for sp in split_parents:
                if sp not in graph.nodes:
                    raise LintWarn(
                        "Join *%s* references split parent *%s* that "
                        "is no longer in the graph (likely removed via "
                        "remove_step)." % (node.name, sp),
                        node.func_lineno,
                        node.source_file,
                    )
