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
    msg = "Add %s *%s* step in your flow."
    for prefix, node in (("a", "start"), ("an", "end")):
        if node not in graph:
            raise LintWarn(msg % (prefix, node))


@linter.ensure_static_graph
@linter.check
def check_that_end_is_end(graph):
    msg0 = "The *end* step should not have a step.next() transition. " "Just remove it."
    msg1 = (
        "The *end* step should not be a join step (it gets an extra "
        "argument). Add a join step before it."
    )

    node = graph["end"]

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
    msg = (
        "Step *{0.name}* is unreachable from the start step. Add "
        "self.next({0.name}) in another step or remove *{0.name}*."
    )
    seen = set(["start"])

    def traverse(node):
        for n in node.out_funcs:
            if n not in seen:
                seen.add(n)
                traverse(graph[n])

    traverse(graph["start"])
    nodeset = frozenset(n.name for n in graph)
    orphans = nodeset - seen
    if orphans:
        orphan = graph[list(orphans)[0]]
        raise LintWarn(msg.format(orphan), orphan.func_lineno, orphan.source_file)


@linter.ensure_static_graph
@linter.check
def check_split_join_balance(graph):
    msg0 = (
        "Step *end* reached before a split started at step(s) *{roots}* "
        "were joined. Add a join step before *end*."
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
                    msg0.format(roots=roots), node.func_lineno, node.source_file
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

    traverse(graph["start"], [])


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
