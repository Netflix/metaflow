import re
from .exception import MetaflowException
from .util import all_equal

class LintWarn(MetaflowException):
    headline="Validity checker found an issue"

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
        return self._decorate('require_static_graph', f)

    def ensure_fundamentals(self, f):
        return self._decorate('require_fundamentals', f)

    def ensure_acyclicity(self, f):
        return self._decorate('require_acyclicity', f)

    def ensure_non_nested_foreach(self, f):
        return self._decorate('require_non_nested_foreach', f)

    def check(self, f):
        self._checks.append(f)
        f.attrs = []
        return f

    def run_checks(self, graph, **kwargs):
        for check in self._checks:
            if any(getattr(self, attr) or kwargs.get(attr)
                   for attr in check.attrs):
                check(graph)

linter = FlowLinter()

@linter.ensure_fundamentals
@linter.check
def check_reserved_words(graph):
    RESERVED = {'name',
                'next',
                'input',
                'index',
                'cmd'}
    msg = 'Step name *%s* is a reserved word. Choose another name for the '\
          'step.'
    for node in graph:
        if node.name in RESERVED:
            raise LintWarn(msg % node.name)

@linter.ensure_fundamentals
@linter.check
def check_basic_steps(graph):
    msg ="Add %s *%s* step in your flow."
    for prefix, node in (('a', 'start'), ('an', 'end')):
        if node not in graph:
            raise LintWarn(msg % (prefix, node))

@linter.ensure_static_graph
@linter.check
def check_that_end_is_end(graph):
    msg0="The *end* step should not have a step.next() transition. "\
         "Just remove it."
    msg1="The *end* step should not be a join step (it gets an extra "\
         "argument). Add a join step before it."

    node=graph['end']

    if node.has_tail_next or node.invalid_tail_next:
        raise LintWarn(msg0, node.tail_next_lineno)
    if node.num_args > 1:
        raise LintWarn(msg1, node.tail_next_lineno)

@linter.ensure_fundamentals
@linter.check
def check_step_names(graph):
    msg =\
    "Step *{0.name}* has an invalid name. Only lowercase ascii "\
    "characters, underscores, and digits are allowed."
    for node in graph:
        if re.search('[^a-z0-9_]', node.name) or node.name[0] == '_':
            raise LintWarn(msg.format(node), node.func_lineno)

@linter.ensure_fundamentals
@linter.check
def check_num_args(graph):
    msg0 =\
    "Step {0.name} has too many arguments. Normal steps take only "\
    "'self' as an argument. Join steps take 'self' and 'inputs'."
    msg1 =\
    "Step *{0.name}* is both a join step (it takes an extra argument) "\
    "and a split step (it transitions to multiple steps). This is not "\
    "allowed. Add a new step so that split and join become separate steps."
    msg2 = "Step *{0.name}* is missing the 'self' argument."
    for node in graph:
        if node.num_args > 2:
            raise LintWarn(msg0.format(node), node.func_lineno)
        elif node.num_args == 2 and node.type != 'join':
            raise LintWarn(msg1.format(node), node.func_lineno)
        elif node.num_args == 0:
            raise LintWarn(msg2.format(node), node.func_lineno)

@linter.ensure_static_graph
@linter.check
def check_static_transitions(graph):
    msg =\
    "Step *{0.name}* is missing a self.next() transition to "\
    "the next step. Add a self.next() as the last line in the "\
    "function."
    for node in graph:
        if node.type != 'end' and not node.has_tail_next:
            raise LintWarn(msg.format(node), node.func_lineno)

@linter.ensure_static_graph
@linter.check
def check_valid_transitions(graph):
    msg =\
    "Step *{0.name}* specifies an invalid self.next() transition. "\
    "Make sure the self.next() expression matches with one of the "\
    "supported transition types."
    for node in graph:
        if node.type != 'end' and\
           node.has_tail_next and\
           node.invalid_tail_next:
            raise LintWarn(msg.format(node), node.tail_next_lineno)

@linter.ensure_static_graph
@linter.check
def check_unknown_transitions(graph):
    msg =\
    "Step *{0.name}* specifies a self.next() transition to "\
    "an unknown step, *{step}*."
    for node in graph:
        unknown = [n for n in node.out_funcs if n not in graph]
        if unknown:
            raise LintWarn(msg.format(node, step=unknown[0]),
                           node.tail_next_lineno)

@linter.ensure_acyclicity
@linter.ensure_static_graph
@linter.check
def check_for_acyclicity(graph):
    msg = "There is a loop in your flow: *{0}*. Break the loop "\
          "by fixing self.next() transitions."
    def check_path(node, seen):
        for n in node.out_funcs:
            if n in seen:
                path = '->'.join(seen + [n])
                raise LintWarn(msg.format(path),
                               node.tail_next_lineno)
            else:
                check_path(graph[n], seen + [n])
    for start in graph:
        check_path(start, [])

@linter.ensure_static_graph
@linter.check
def check_for_orphans(graph):
    msg =\
    "Step *{0.name}* is unreachable from the start step. Add "\
    "self.next({0.name}) in another step or remove *{0.name}*."
    seen = set(['start'])
    def traverse(node):
        for n in node.out_funcs:
            if n not in seen:
                seen.add(n)
                traverse(graph[n])
    traverse(graph['start'])
    nodeset = frozenset(n.name for n in graph)
    orphans = nodeset - seen
    if orphans:
        orphan = graph[list(orphans)[0]]
        raise LintWarn(msg.format(orphan), orphan.func_lineno)

@linter.ensure_static_graph
@linter.check
def check_split_join_balance(graph):
    msg0 = "Step *end* reached before a split started at step(s) *{roots}* "\
           "were joined. Add a join step before *end*."
    msg1 = "Step *{0.name}* seems like a join step (it takes an extra input "\
           "argument) but an incorrect number of steps (*{paths}*) lead to "\
           "it. This join was expecting {num_roots} incoming paths, starting "\
           "from splitted step(s) *{roots}*."
    msg2 = "Step *{0.name}* seems like a join step (it takes an extra input "\
           "argument) but it is not preceded by a split. Ensure that there is "\
           "a matching split for every join."
    msg3 = "Step *{0.name}* joins steps from unrelated splits. Ensure that "\
           "there is a matching join for every split."

    def traverse(node, split_stack):
        if node.type == 'linear':
            new_stack = split_stack
        elif node.type in ('split-or', 'split-and', 'foreach'):
            new_stack = split_stack + [('split', node.out_funcs)]
        elif node.type == 'end':
            if split_stack:
                split_type, split_roots = split_stack.pop()
                roots = ', '.join(split_roots)
                raise LintWarn(msg0.format(roots=roots))
        elif node.type == 'join':
            if split_stack:
                split_type, split_roots = split_stack[-1]
                new_stack = split_stack[:-1]
                if len(node.in_funcs) != len(split_roots):
                    paths = ', '.join(node.in_funcs)
                    roots = ', '.join(split_roots)
                    raise LintWarn(msg1.format(node,
                                               paths=paths,
                                               num_roots=len(split_roots),
                                               roots=roots),
                                   node.func_lineno)
            else:
                raise LintWarn(msg2.format(node), node.func_lineno)
            # check that incoming steps come from the same lineage
            # (no cross joins)
            def parents(n):
                if graph[n].type == 'join':
                    return tuple(graph[n].split_parents[:-1])
                else:
                    return tuple(graph[n].split_parents)

            if not all_equal(map(parents, node.in_funcs)):
                raise LintWarn(msg3.format(node), node.func_lineno)

        for n in node.out_funcs:
            traverse(graph[n], new_stack)

    traverse(graph['start'], [])

@linter.ensure_static_graph
@linter.check
def check_empty_foreaches(graph):
    msg = "Step *{0.name}* is a foreach split that has no children: "\
          "it is followed immeditately by a join step, *{join}*. Add "\
          "at least one step between the split and the join."
    for node in graph:
        if node.type == 'foreach':
            joins = [n for n in node.out_funcs if graph[n].type == 'join']
            if joins:
                raise LintWarn(msg.format(node, join=joins[0]))

@linter.ensure_non_nested_foreach
@linter.check
def check_nested_foreach(graph):
    msg = "Nested foreaches are not allowed: Step *{0.name}* is a foreach "\
          "split that is nested under another foreach split."
    for node in graph:
        if node.type == 'foreach':
            if any(graph[p].type == 'foreach' for p in node.split_parents):
                raise LintWarn(msg.format(node))

