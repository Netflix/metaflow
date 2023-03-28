import ast

import metaflow as mf
from metaflow.flowspec import FlowSpecMeta
from metaflow.graph import parse_flow
from metaflow.meta import FOREACH, IS_STEP, JOIN, META_KEY, ORIG_FN, PREV_STEP
from metaflow.parameters import Parameter
from .decorators import get_meta


class StepVisitor(ast.NodeVisitor):
    def __init__(self, nodes, flow, file):
        self.nodes = nodes
        self.flow = flow
        self.file = file
        super(StepVisitor, self).__init__()

    def visit_FunctionDef(self, node):
        name = node.name
        func = getattr(self.flow, name)
        if getattr(func, META_KEY, {}).get(IS_STEP):
            from metaflow.graph import DAGNode

            self.nodes[name] = DAGNode(node, func, parse=False, file=self.file)
        elif getattr(func, IS_STEP, None):
            # As a safety check, make sure we are not applying the `metaflow.api` Metaclass to a `metaflow.FlowSpec`
            raise ValueError(func)


def parse_steps(flow):
    nodes = {}

    # Parse graph nodes (`@step`s) from `FlowSpec` subclasses
    mro = flow.mro()
    assert mro[0] is flow

    bases = [
        base
        for base in mro[1:]
        if base is not mf.FlowSpec and issubclass(base, mf.FlowSpec)
    ]
    for base in bases:
        nodes.update(parse_steps(base)[0])

    # Parse graph nodes (`@step`s) from this `FlowSpec` class
    file = flow.file
    name = flow.name
    with open(file, "r") as f:
        source = f.read()

    root, tree = parse_flow(file, name)

    # Infer an ending line number (used when reporting/logging a @step's source location). Python <3.8 doesn't set
    # `end_lineno`, so this is a best-effort calculation that should be consistent across Python versions.
    successors = [nxt for cur, nxt in zip(tree, tree[1:]) if cur is root]
    if successors:
        [successor] = successors
        end_lineno = successor.lineno - 1
    else:
        end_lineno = len(source.split("\n"))

    StepVisitor(nodes, flow, file).visit(root)

    return nodes, tree, root, end_lineno


class FlowSpecMeta(mf.FlowSpecMeta):
    """Metaclass that activates alternate graph-parsing for `FlowSpec`s

    - Inject trivial start/end steps
    - Add missing self.next() calls from each step to its successor (in the order defined in the FlowSpec)
    """

    def __new__(cls, name, bases, dct):
        from metaflow.graph import DAGNode

        sup = cls

        # Inject FlowSpec superclass
        if all(not issubclass(base, mf.FlowSpec) for base in bases):
            bases = (mf.FlowSpec,) + bases

        # TODO: would be nice to find a way to avoid defining the class twice (here and then again just before the end
        # of this `__new__` function. `cls.mro()` may be redundant with `bases`, and various `getattr` method accesses
        # could go through `dct`?
        cls = super().__new__(sup, name, bases, dct)
        file = cls.file

        nodes, tree, root, end_lineno = parse_steps(cls)

        if nodes:
            first_step = next(iter(nodes.keys()))
        else:
            first_step = "end"

        # Assemble all nodes in the correct order
        for node in nodes.values():
            if node.name == "end":
                continue
            node.has_tail_next = True

        if "start" not in nodes:
            # Build a synthetic `start` step
            # TODO: allow explicit/configurable start node
            [start_tree] = ast.parse(
                "def start(self): self.next(self.%s)" % first_step
            ).body
            start = DAGNode(start_tree, func=None, file=file, lineno=root.lineno)
            # must prepend `start`; order matters for graph-structure inference below
            nodes = {
                "start": start,
                **nodes,
            }

        if "end" not in nodes:
            # Build a synthetic `end` step
            [end_tree] = ast.parse("def end(self): pass").body
            end = DAGNode(end_tree, func=None, file=file, lineno=end_lineno)
            end.type = "end"
            nodes["end"] = end

        def process(pk, pv, ck, cv):
            if ck == "end":
                if cv.type is None:
                    cv.type = "end"
                else:
                    assert cv.type == "end"
                cv.in_funcs = {pk}
                if pv.out_funcs:
                    assert pk == "start"
                    assert pv.out_funcs == ["end"]
                else:
                    pv.out_funcs = [ck]
                if not pv.type:
                    pv.type = "linear"
                return
            fn = getattr(cls, ck, None)
            if fn is None:
                # Synthetic `start`
                assert ck == "start"
                cm = {}
            else:
                cm = get_meta(fn)
            if FOREACH in cm:
                foreach = cm[FOREACH]
                in_step = foreach["prev"]
                in_field = foreach["field"]
                if in_step is None:
                    in_step = pk
                pv = nodes[in_step]
                assert pv.type is None
                pv.type = "foreach"
                pv.foreach_param = in_field
                assert not pv.out_funcs
                pv.out_funcs = [ck]
                assert not cv.in_funcs
                cv.in_funcs = {in_step}
                cv.type = "linear"
                return
            if (
                JOIN in cm
            ):  # None is a valid/meaningful JOIN value ("join the immediately preceding foreach")
                join = cm[JOIN]
                assert cv.type is None
                cv.type = "join"
                if join is None:
                    pf = getattr(cls, pk)
                    if not pv.type:
                        pv.type = "linear"
                    assert not cv.in_funcs
                    cv.in_funcs = {pk}
                    assert not pv.out_funcs
                    pv.out_funcs = [ck]
                else:
                    for pk in join:
                        pv = nodes[pk]
                        if not pv.type:
                            pv.type = "linear"
                        cv.in_funcs.add(pk)
                        pv.out_funcs.append(ck)
                return
            if PREV_STEP in cm:
                pk = cm[PREV_STEP] or pk
                pv = nodes[pk]
                if cv.in_funcs:
                    assert pk == "start"
                    assert cv.in_funcs == {"start"}
                else:
                    cv.in_funcs = {pk}
                if pk == "start":
                    assert pv.out_funcs == [ck]
                elif pv.out_funcs:
                    pv.type = "split"
                    pv.out_funcs.append(ck)
                else:
                    if not pv.type:
                        pv.type = "linear"
                    pv.out_funcs = [ck]
                return
            raise RuntimeError("Unrecognized node %s: %s" % (ck, str(cm)))

        items = list(nodes.items())
        for (pk, pv), (ck, cv) in zip(items, items[1:]):
            process(pk, pv, ck, cv)

        # Rewrite step methods to include correct self.next calls
        # TODO: detect overloaded step names
        steps = {}
        parameters = {}

        def expand(cur):
            mro = cur.mro()
            assert cur is mro[0]
            bases = mro[1:]

            for base in bases:
                if issubclass(base, mf.FlowSpec) and not base is mf.FlowSpec:
                    expand(base)

            items = cur.__dict__.items()
            file = getattr(cur, "file", None)
            for k, v in items:
                meta = getattr(v, META_KEY, {})
                if callable(v) and (
                    meta.get(IS_STEP, False) or getattr(v, IS_STEP, False)
                ):
                    if meta.get("synthetic"):
                        continue
                    if not hasattr(v, META_KEY):
                        setattr(v, META_KEY, meta)
                    meta["file"] = file
                    if k in steps:
                        raise RuntimeError(
                            'Flow %s: refusing to mix in multiple implementations of step "%s"'
                            % (
                                name,
                                k,
                            )
                        )
                    # TODO: re-namespace + incorporate existing start/end steps
                    steps[k] = v
                elif isinstance(v, Parameter):
                    parameters[k] = v

        expand(cls)

        for k, v in parameters.items():
            if k in dct:
                if v is not dct[k]:
                    raise RuntimeError(
                        "Conflicting values of %s: %s, %s" % (k, str(dct[k]), str(v))
                    )
            else:
                dct[k] = v

        # Inject a `start` step that just calls the first real step
        # TODO: unify this with the synthetic `start` AST above?
        if "start" not in steps:
            if steps:
                first_step = next(iter(steps.keys()))
                start = lambda self: self.next(getattr(self, first_step))
            else:
                start = lambda self: self.next(self.end)
            start.__name__ = "start"
            meta = {
                "synthetic": True,
            }
            setattr(start, META_KEY, meta)
            dct["start"] = mf.step(start)

        if "end" not in steps:
            # Inject a no-op `end` step
            # TODO: unify this with the synthetic `end` AST above?
            def end(self):
                pass

            meta = {
                "synthetic": True,
            }
            setattr(end, META_KEY, meta)
            dct["end"] = mf.step(end)

        def inject_next_call(self, step_fn, next_fn, *args):
            """Wrap a step function to add a tail-call to a subsequent step.

            Allows omitting the final `self.next(self.next_step)` from @step function definitions. Partially-applied copies of
            this function (filling in all arguments except `self`) are created in the `Flow` metaclass below, and stored on
            FlowSpec instances of type `Flow`."""
            step_fn(self, *args)
            next_fn(self)

        def wrap_step(name, node, fn):
            """Wrap a step `fn`: add tail-call to next step, set name / step metadata."""
            if name == "end":
                return fn
            meta = get_meta(fn)
            orig_fn = meta.get(ORIG_FN)
            if node.type == "foreach":
                [nxt] = node.out_funcs
                field = node.foreach_param
                next_fn = lambda self: self.next(getattr(self, nxt), foreach=field)
            elif node.type == "split":
                out_funcs = node.out_funcs
                assert len(out_funcs) > 1
                next_fn = lambda self: self.next(
                    *[getattr(self, nxt) for nxt in out_funcs]
                )
            elif node.type == "join":
                out_funcs = node.out_funcs
                if len(out_funcs) == 1:
                    [nxt] = node.out_funcs
                    next_fn = lambda self: self.next(getattr(self, nxt))
                else:
                    next_fn = lambda self: self.next(
                        *[getattr(self, nxt) for nxt in out_funcs]
                    )
            elif node.type == "linear":
                if len(node.out_funcs) == 1:
                    [nxt] = node.out_funcs
                else:
                    raise RuntimeError(
                        "node %s: expected 1 out_func, found %d%s"
                        % (
                            name,
                            len(node.out_funcs),
                            (" %s" % ",".join(node.out_funcs))
                            if node.out_funcs
                            else "",
                        )
                    )
                next_fn = lambda self: self.next(getattr(self, nxt))
            else:
                raise RuntimeError(
                    "node %s: unexpected node.type %s" % (name, node.type)
                )
            if not orig_fn:
                orig_fn = fn
            if node.type == "join":
                fn2 = lambda self, inputs: inject_next_call(
                    self, orig_fn, next_fn, inputs
                )
            else:
                if FOREACH in meta:
                    from inspect import getfullargspec

                    argspec = getfullargspec(orig_fn)
                    args_spec = argspec.args
                    if len(args_spec) == 2:
                        fn2 = lambda self: inject_next_call(
                            self, orig_fn, next_fn, self.input
                        )
                    elif len(argspec.args) == 1:
                        fn2 = lambda self: inject_next_call(self, orig_fn, next_fn)
                    else:
                        raise RuntimeError(
                            "Expected 0 or 1 arguments to `@foreach` step %s, found %d"
                            % (node.name, len(args_spec))
                        )
                else:
                    fn2 = lambda self: inject_next_call(self, orig_fn, next_fn)

            fn2.__name__ = name
            meta = get_meta(fn2, meta)
            meta[ORIG_FN] = orig_fn
            fn2.decorators = orig_fn.decorators
            assert meta[IS_STEP]
            return mf.step(fn2)

        # Replace each step fn with a wrapped fn that includes a trailing `self.next` call to the next step
        steps = list(steps.items())
        steps = {name: wrap_step(name, nodes[name], fn) for name, fn in steps}

        # Insert wrapped functions into class-methods dictionary
        for k, v in steps.items():
            dct[k] = steps[k]

        # Recreate the FlowSpec class with new methods and augmented graph nodes
        cls = super().__new__(sup, name, bases, dct, nodes=nodes)
        return cls


class FlowSpec(
    mf.FlowSpec,
    metaclass=FlowSpecMeta,
):
    """Alternate FlowSpec implementation with a more powerful metaclass for parsing workflow graph structure.

    Designed to work similarly to `metaflow.FlowSpec` and used with the `metaflow.api.step` decorator (instead of
    `metaflow.step`), with a few added features:
    - graph-structure is described in method-decorators (`@step`, `@foreach`, `@join`), not in `self.next` calls inside
      steps
      - `self.next` calls are omitted altogether; the metaclass synthesizes them
    - `start` and `end` methods not required:
      - under the hood, they are synthesized if absent
      - FlowSpecs can contain 0 or 1 `@step`s (as opposed to requiring ≥2 including `start` and `end`)
    - Composition of flows, via mix-in style inheritance, is supported, with some caveats:
      - Step names can't collide
      - No explicitly-defined `start` and `end` steps should exist in the mixed-in flows (mixing in "old-style"
        `FlowSpec`s is unsupported, for this reason)
      - TODO: this could be worked around / made to work, given info available at graph-construction time; may require
        mangling, fully-qualifying, or otherwise namespacing step names from different flows
    """

    pass
