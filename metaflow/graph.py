import inspect
import ast
import re

def deindent_docstring(doc):
    if doc:
        # Find the indent to remove from the doctring. We consider the following possibilities:
        # Option 1:
        #  """This is the first line
        #    This is the second line
        #  """
        # Option 2:
        #  """
        # This is the first line
        # This is the second line
        # """
        # Option 3:
        #  """
        #     This is the first line
        #     This is the second line
        #  """
        #
        # In all cases, we can find the indent to remove by doing the following:
        #  - Check the first non-empty line, if it has an indent, use that as the base indent
        #  - If it does not have an indent and there is a second line, check the indent of the
        #    second line and use that
        saw_first_line = False
        matched_indent = None
        for line in doc.splitlines():
            if line:
                matched_indent = re.match('[\t ]+', line)
                if matched_indent is not None or saw_first_line:
                    break
                saw_first_line = True
        if matched_indent:
            return re.sub(r'\n' + matched_indent.group(), '\n', doc).strip()
        else:
            return doc
    else:
        return ''

class DAGNode(object):
    def __init__(self, func_ast, decos, doc):
        self.name = func_ast.name
        self.func_lineno = func_ast.lineno
        self.decorators = decos
        self.doc = deindent_docstring(doc)

        # these attributes are populated by _parse
        self.tail_next_lineno = 0
        self.type = None
        self.out_funcs = []
        self.has_tail_next = False
        self.invalid_tail_next = False
        self.num_args = 0
        self.condition = None
        self.foreach_param = None
        self._parse(func_ast)

        # these attributes are populated by _traverse_graph
        self.in_funcs = set()
        self.split_parents = []
        self.matching_join = None

        # these attributes are populated by _postprocess
        self.is_inside_foreach = False

    def _expr_str(self, expr):
        return '%s.%s' % (expr.value.id, expr.attr)

    def _parse(self, func_ast):

        self.num_args = len(func_ast.args.args)
        tail = func_ast.body[-1]

        # end doesn't need a transition
        if self.name == 'end':
            # TYPE: end
            self.type = 'end'

        # ensure that the tail an expression
        if not isinstance(tail, ast.Expr):
            return

        # determine the type of self.next transition
        try:
            if not self._expr_str(tail.value.func) == 'self.next':
                return

            self.has_tail_next = True
            self.invalid_tail_next = True
            self.tail_next_lineno = tail.lineno
            self.out_funcs = [e.attr for e in tail.value.args]
            keywords = dict((k.arg, k.value.s) for k in tail.value.keywords)

            if len(keywords) == 1:
                if 'foreach' in keywords:
                    # TYPE: foreach
                    self.type = 'foreach'
                    if len(self.out_funcs) == 1:
                        self.foreach_param = keywords['foreach']
                        self.invalid_tail_next = False
                elif 'condition' in keywords:
                    # TYPE: split-or
                    self.type = 'split-or'
                    if len(self.out_funcs) == 2:
                        self.condition = keywords['condition']
                        self.invalid_tail_next = False
            elif len(keywords) == 0:
                if len(self.out_funcs) > 1:
                    # TYPE: split-and
                    self.type = 'split-and'
                    self.invalid_tail_next = False
                elif len(self.out_funcs) == 1:
                    # TYPE: linear
                    if self.num_args > 1:
                        self.type = 'join'
                    else:
                        self.type = 'linear'
                    self.invalid_tail_next = False

        except AttributeError:
            return

    def __str__(self):
        return\
"""*[{0.name} {0.type} (line {0.func_lineno})]*
    in_funcs={in_funcs}
    out_funcs={out_funcs}
    split_parents={parents}
    matching_join={matching_join}
    is_inside_foreach={is_inside_foreach}
    decorators={decos}
    num_args={0.num_args}
    has_tail_next={0.has_tail_next} (line {0.tail_next_lineno})
    invalid_tail_next={0.invalid_tail_next}
    condition={0.condition}
    foreach_param={0.foreach_param}
    -> {out}"""\
    .format(self,
            matching_join=self.matching_join and '[%s]' % self.matching_join,
            is_inside_foreach=self.is_inside_foreach,
            out_funcs=', '.join('[%s]' % x for x in self.out_funcs),
            in_funcs=', '.join('[%s]' % x for x in self.in_funcs),
            parents=', '.join('[%s]' % x for x in self.split_parents),
            decos=' | '.join(map(str, self.decorators)),
            out=', '.join('[%s]' % x for x in self.out_funcs))

class StepVisitor(ast.NodeVisitor):

    def __init__(self, nodes, flow):
        self.nodes = nodes
        self.flow = flow
        super(StepVisitor, self).__init__()

    def visit_FunctionDef(self, node):
        func = getattr(self.flow, node.name)
        if hasattr(func, 'is_step'):
            self.nodes[node.name] = DAGNode(node, func.decorators, func.__doc__)

class FlowGraph(object):

    def __init__(self, flow):
        self.name = flow.__name__
        self.nodes = self._create_nodes(flow)
        self.doc = deindent_docstring(flow.__doc__)
        self._traverse_graph()
        self._postprocess()

    def _create_nodes(self, flow):
        module = __import__(flow.__module__)
        tree = ast.parse(inspect.getsource(module)).body
        root = [n for n in tree\
                if isinstance(n, ast.ClassDef) and n.name == self.name][0]
        nodes = {}
        StepVisitor(nodes, flow).visit(root)
        return nodes

    def _postprocess(self):
        # any node who has a foreach as any of its split parents
        # has is_inside_foreach=True *unless* all of those foreaches
        # are joined by the node
        for node in self.nodes.values():
            foreaches = [p for p in node.split_parents
                         if self.nodes[p].type == 'foreach']
            if [f for f in foreaches
                if self.nodes[f].matching_join != node.name]:
                node.is_inside_foreach = True

    def _traverse_graph(self):

        def traverse(node, seen, split_parents):

            if node.type in ('split-or', 'split-and', 'foreach'):
                node.split_parents = split_parents
                split_parents = split_parents + [node.name]
            elif node.type == 'join':
                # ignore joins without splits
                if split_parents:
                    self[split_parents[-1]].matching_join = node.name
                    node.split_parents = split_parents
                    split_parents = split_parents[:-1]
            else:
                node.split_parents = split_parents

            for n in node.out_funcs:
                # graph may contain loops - ignore them
                if n not in seen:
                    # graph may contain unknown transitions - ignore them
                    if n in self:
                        child = self[n]
                        child.in_funcs.add(node.name)
                        traverse(child, seen + [n], split_parents)

        if 'start' in self:
            traverse(self['start'], [], [])

        # fix the order of in_funcs
        for node in self.nodes.values():
            node.in_funcs = sorted(node.in_funcs)

    def __getitem__(self, x):
        return self.nodes[x]

    def __contains__(self, x):
        return x in self.nodes

    def __iter__(self):
        return iter(self.nodes.values())

    def __str__(self):
        return '\n'.join(str(n) for _, n in sorted((n.func_lineno, n)\
                                for n in self.nodes.values()))

    def output_dot(self):

        def edge_specs():
            for node in self.nodes.values():
                for edge in node.out_funcs:
                    yield '%s -> %s;' % (node.name, edge)

        def node_specs():
            for node in self.nodes.values():
                nodetype = 'join' if node.num_args > 1 else node.type
                yield '"{0.name}"'\
                      '[ label = <<b>{0.name}</b> | <font point-size="10">{type}</font>> '\
                      '  fontname = "Helvetica" '\
                      '  shape = "record" ];'.format(node, type=nodetype)

        return "digraph {0.name} {{\n"\
               "rankdir=LR;\n"\
               "{nodes}\n"\
               "{edges}\n"\
               "}}".format(self,
                           nodes='\n'.join(node_specs()),
                           edges='\n'.join(edge_specs()))

