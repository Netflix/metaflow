import inspect
import ast
import re

from itertools import chain


from .util import to_pod


def deindent_docstring(doc):
    if doc:
        # Find the indent to remove from the docstring. We consider the following possibilities:
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
                matched_indent = re.match("[\t ]+", line)
                if matched_indent is not None or saw_first_line:
                    break
                saw_first_line = True
        if matched_indent:
            return re.sub(r"\n" + matched_indent.group(), "\n", doc).strip()
        else:
            return doc
    else:
        return ""


class DAGNode(object):
    def __init__(
        self, func_ast, decos, wrappers, config_decorators, doc, source_file, lineno
    ):
        self.name = func_ast.name
        self.source_file = source_file
        # lineno is the start line of decorators in source_file
        # func_ast.lineno is lines from decorators start to def of function
        self.func_lineno = lineno + func_ast.lineno - 1
        self.decorators = decos
        self.wrappers = wrappers
        self.config_decorators = config_decorators
        self.doc = deindent_docstring(doc)
        self.parallel_step = any(getattr(deco, "IS_PARALLEL", False) for deco in decos)

        # these attributes are populated by _parse
        self.tail_next_lineno = 0
        self.type = None
        self.out_funcs = []
        self.has_tail_next = False
        self.invalid_tail_next = False
        self.num_args = 0
        self.switch_cases = {}
        self.condition = None
        self.foreach_param = None
        self.num_parallel = 0
        self.parallel_foreach = False
        self._parse(func_ast, lineno)

        # these attributes are populated by _traverse_graph
        self.in_funcs = set()
        self.split_parents = []
        self.split_branches = []
        self.matching_join = None
        # these attributes are populated by _postprocess
        self.is_inside_foreach = False

    def _expr_str(self, expr):
        return "%s.%s" % (expr.value.id, expr.attr)

    def _parse_switch_dict(self, dict_node):
        switch_cases = {}

        if isinstance(dict_node, ast.Dict):
            for key, value in zip(dict_node.keys, dict_node.values):
                case_key = None

                # handle string literals
                if hasattr(ast, "Str") and isinstance(key, ast.Str):
                    case_key = key.s
                elif isinstance(key, ast.Constant):
                    case_key = key.value
                elif isinstance(key, ast.Attribute):
                    if isinstance(key.value, ast.Attribute) and isinstance(
                        key.value.value, ast.Name
                    ):
                        # This handles self.config.some_key
                        if key.value.value.id == "self":
                            config_var = key.value.attr
                            config_key = key.attr
                            case_key = f"config:{config_var}.{config_key}"
                        else:
                            return None
                    else:
                        return None

                # handle variables or other dynamic expressions - not allowed
                elif isinstance(key, ast.Name):
                    return None
                else:
                    # can't statically analyze this key
                    return None

                if case_key is None:
                    return None

                # extract the step name from the value
                if isinstance(value, ast.Attribute) and isinstance(
                    value.value, ast.Name
                ):
                    if value.value.id == "self":
                        step_name = value.attr
                        switch_cases[case_key] = step_name
                    else:
                        return None
                else:
                    return None

        return switch_cases if switch_cases else None

    def _parse(self, func_ast, lineno):
        self.num_args = len(func_ast.args.args)
        tail = func_ast.body[-1]

        # end doesn't need a transition
        if self.name == "end":
            # TYPE: end
            self.type = "end"

        # ensure that the tail an expression
        if not isinstance(tail, ast.Expr):
            return

        # determine the type of self.next transition
        try:
            if not self._expr_str(tail.value.func) == "self.next":
                return

            self.has_tail_next = True
            self.invalid_tail_next = True
            self.tail_next_lineno = lineno + tail.lineno - 1

            # Check if first argument is a dictionary (switch case)
            if (
                len(tail.value.args) == 1
                and isinstance(tail.value.args[0], ast.Dict)
                and any(k.arg == "condition" for k in tail.value.keywords)
            ):
                # This is a switch statement
                switch_cases = self._parse_switch_dict(tail.value.args[0])
                condition_name = None

                # Get condition parameter
                for keyword in tail.value.keywords:
                    if keyword.arg == "condition":
                        if hasattr(ast, "Str") and isinstance(keyword.value, ast.Str):
                            condition_name = keyword.value.s
                        elif isinstance(keyword.value, ast.Constant) and isinstance(
                            keyword.value.value, str
                        ):
                            condition_name = keyword.value.value
                        break

                if switch_cases and condition_name:
                    self.type = "split-switch"
                    self.condition = condition_name
                    self.switch_cases = switch_cases
                    self.out_funcs = list(switch_cases.values())
                    self.invalid_tail_next = False
                    return

            else:
                self.out_funcs = [e.attr for e in tail.value.args]

            keywords = dict(
                (k.arg, getattr(k.value, "s", None)) for k in tail.value.keywords
            )
            if len(keywords) == 1:
                if "foreach" in keywords:
                    # TYPE: foreach
                    self.type = "foreach"
                    if len(self.out_funcs) == 1:
                        self.foreach_param = keywords["foreach"]
                        self.invalid_tail_next = False
                elif "num_parallel" in keywords:
                    self.type = "foreach"
                    self.parallel_foreach = True
                    if len(self.out_funcs) == 1:
                        self.num_parallel = keywords["num_parallel"]
                        self.invalid_tail_next = False
            elif len(keywords) == 0:
                if len(self.out_funcs) > 1:
                    # TYPE: split
                    self.type = "split"
                    self.invalid_tail_next = False
                elif len(self.out_funcs) == 1:
                    # TYPE: linear
                    if self.name == "start":
                        self.type = "start"
                    elif self.num_args > 1:
                        self.type = "join"
                    else:
                        self.type = "linear"
                    self.invalid_tail_next = False
        except AttributeError:
            return

    def __str__(self):
        return """*[{0.name} {0.type} ({0.source_file} line {0.func_lineno})]*
    in_funcs={in_funcs}
    out_funcs={out_funcs}
    split_parents={parents}
    split_branches={branches}
    matching_join={matching_join}
    is_inside_foreach={is_inside_foreach}
    decorators={decos}
    num_args={0.num_args}
    has_tail_next={0.has_tail_next} (line {0.tail_next_lineno})
    invalid_tail_next={0.invalid_tail_next}
    foreach_param={0.foreach_param}
    condition={0.condition}
    parallel_step={0.parallel_step}
    parallel_foreach={0.parallel_foreach}
    -> {out}""".format(
            self,
            matching_join=self.matching_join and "[%s]" % self.matching_join,
            is_inside_foreach=self.is_inside_foreach,
            out_funcs=", ".join("[%s]" % x for x in self.out_funcs),
            in_funcs=", ".join("[%s]" % x for x in self.in_funcs),
            parents=", ".join("[%s]" % x for x in self.split_parents),
            branches=", ".join("[%s]" % x for x in self.split_branches),
            decos=" | ".join(map(str, self.decorators)),
            out=", ".join("[%s]" % x for x in self.out_funcs),
        )


class FlowGraph(object):
    def __init__(self, flow):
        self.name = flow.__name__
        self.nodes = self._create_nodes(flow)
        self.doc = deindent_docstring(flow.__doc__)
        # nodes sorted in topological order.
        self.sorted_nodes = []
        self._traverse_graph()
        self._postprocess()

    def _create_nodes(self, flow):
        nodes = {}
        for element in dir(flow):
            func = getattr(flow, element)
            if callable(func) and hasattr(func, "is_step"):
                source_file = inspect.getsourcefile(func)
                source_lines, lineno = inspect.getsourcelines(func)
                # This also works for code (strips out leading whitspace based on
                # first line)
                source_code = deindent_docstring("".join(source_lines))
                function_ast = ast.parse(source_code).body[0]
                node = DAGNode(
                    function_ast,
                    func.decorators,
                    func.wrappers,
                    func.config_decorators,
                    func.__doc__,
                    source_file,
                    lineno,
                )
                nodes[element] = node
        return nodes

    def _postprocess(self):
        # any node who has a foreach as any of its split parents
        # has is_inside_foreach=True *unless* all of those `foreach`s
        # are joined by the node
        for node in self.nodes.values():
            foreaches = [
                p for p in node.split_parents if self.nodes[p].type == "foreach"
            ]
            if [f for f in foreaches if self.nodes[f].matching_join != node.name]:
                node.is_inside_foreach = True

    def _traverse_graph(self):
        def traverse(node, seen, split_parents, split_branches):
            add_split_branch = False
            try:
                self.sorted_nodes.remove(node.name)
            except ValueError:
                pass
            self.sorted_nodes.append(node.name)
            if node.type in ("split", "foreach"):
                node.split_parents = split_parents
                node.split_branches = split_branches
                add_split_branch = True
                split_parents = split_parents + [node.name]
            elif node.type == "split-switch":
                node.split_parents = split_parents
                node.split_branches = split_branches
            elif node.type == "join":
                # ignore joins without splits
                if split_parents:
                    self[split_parents[-1]].matching_join = node.name
                    node.split_parents = split_parents
                    node.split_branches = split_branches[:-1]
                    split_parents = split_parents[:-1]
                    split_branches = split_branches[:-1]
            else:
                node.split_parents = split_parents
                node.split_branches = split_branches

            for n in node.out_funcs:
                # graph may contain loops - ignore them
                if n not in seen:
                    # graph may contain unknown transitions - ignore them
                    if n in self:
                        child = self[n]
                        child.in_funcs.add(node.name)
                        traverse(
                            child,
                            seen + [n],
                            split_parents,
                            split_branches + ([n] if add_split_branch else []),
                        )

        if "start" in self:
            traverse(self["start"], [], [], [])

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
        return "\n".join(str(self[n]) for n in self.sorted_nodes)

    def output_dot(self):
        def edge_specs():
            for node in self.nodes.values():
                if node.type == "split-switch":
                    # Label edges for switch cases
                    for case_value, step_name in node.switch_cases.items():
                        yield (
                            '{0} -> {1} [label="{2}" color="blue" fontcolor="blue"];'.format(
                                node.name, step_name, case_value
                            )
                        )
                else:
                    for edge in node.out_funcs:
                        yield "%s -> %s;" % (node.name, edge)

        def node_specs():
            for node in self.nodes.values():
                if node.type == "split-switch":
                    # Hexagon shape for switch nodes
                    condition_label = (
                        f"switch: {node.condition}" if node.condition else "switch"
                    )
                    yield (
                        '"{0.name}" '
                        '[ label = <<b>{0.name}</b><br/><font point-size="9">{condition}</font>> '
                        '  fontname = "Helvetica" '
                        '  shape = "hexagon" '
                        '  style = "filled" fillcolor = "lightgreen" ];'
                    ).format(node, condition=condition_label)
                else:
                    nodetype = "join" if node.num_args > 1 else node.type
                    yield '"{0.name}"' '[ label = <<b>{0.name}</b> | <font point-size="10">{type}</font>> ' '  fontname = "Helvetica" ' '  shape = "record" ];'.format(
                        node, type=nodetype
                    )

        return (
            "digraph {0.name} {{\n"
            "{nodes}\n"
            "{edges}\n"
            "}}".format(
                self, nodes="\n".join(node_specs()), edges="\n".join(edge_specs())
            )
        )

    def output_steps(self):
        steps_info = {}
        graph_structure = []

        def node_to_type(node):
            if node.type in ["linear", "start", "end", "join"]:
                return node.type
            elif node.type == "split":
                return "split-static"
            elif node.type == "foreach":
                if node.parallel_foreach:
                    return "split-parallel"
                return "split-foreach"
            elif node.type == "split-switch":
                return "split-switch"
            return "unknown"  # Should never happen

        def node_to_dict(name, node):
            d = {
                "name": name,
                "type": node_to_type(node),
                "line": node.func_lineno,
                "source_file": node.source_file,
                "doc": node.doc,
                "decorators": [
                    {
                        "name": deco.name,
                        "attributes": to_pod(deco.attributes),
                        "statically_defined": deco.statically_defined,
                        "inserted_by": deco.inserted_by,
                    }
                    for deco in node.decorators
                    if not deco.name.startswith("_")
                ]
                + [
                    {
                        "name": deco.decorator_name,
                        "attributes": {"_args": deco._args, **deco._kwargs},
                        "statically_defined": deco.statically_defined,
                        "inserted_by": deco.inserted_by,
                    }
                    for deco in chain(node.wrappers, node.config_decorators)
                ],
                "next": node.out_funcs,
            }
            if d["type"] == "split-foreach":
                d["foreach_artifact"] = node.foreach_param
            elif d["type"] == "split-parallel":
                d["num_parallel"] = node.num_parallel
            elif d["type"] == "split-switch":
                d["condition"] = node.condition
                d["switch_cases"] = node.switch_cases
            if node.matching_join:
                d["matching_join"] = node.matching_join
            return d

        def populate_block(start_name, end_name):
            cur_name = start_name
            resulting_list = []
            while cur_name != end_name:
                cur_node = self.nodes[cur_name]
                node_dict = node_to_dict(cur_name, cur_node)

                steps_info[cur_name] = node_dict
                resulting_list.append(cur_name)

                node_type = node_to_type(cur_node)
                if node_type in ("split-static", "split-foreach"):
                    resulting_list.append(
                        [
                            populate_block(s, cur_node.matching_join)
                            for s in cur_node.out_funcs
                        ]
                    )
                    cur_name = cur_node.matching_join
                elif node_type == "split-switch":
                    all_paths = [
                        populate_block(s, end_name)
                        for s in cur_node.out_funcs
                        if s != cur_name
                    ]
                    resulting_list.append(all_paths)
                    cur_name = end_name
                else:
                    # handles only linear, start, and join steps.
                    if cur_node.out_funcs:
                        cur_name = cur_node.out_funcs[0]
                    else:
                        # handles terminal nodes or when we jump to 'end_name'.
                        break
            return resulting_list

        graph_structure = populate_block("start", "end")

        steps_info["end"] = node_to_dict("end", self.nodes["end"])
        graph_structure.append("end")

        return steps_info, graph_structure
