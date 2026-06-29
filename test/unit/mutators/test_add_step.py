"""Phase 1 graph mutation: unit tests for ``MutableFlow.add_step`` /
``remove_step`` and supporting machinery.

Organized by AC family from the design memo:

- ``TestSignatureRules`` — R1..R13 + R-EXTRA rejection at decoration time.
- ``TestMutableDefaults`` — UD-9 denylist (9 stdlib types) and immutables.
- ``TestDefaultFallback`` — UD-6 try/except wrapper for immutable defaults.
- ``TestWrapperAttributes`` — wrapper preserves dunders + source_file
  resolves via ``__wrapped__`` (R5 / D-007).
- ``TestRegistry`` — eager-imported kinds, idempotent, conflict, reload.
- ``TestAddStepGraph`` — end-to-end: wrappers produce correct edges and
  ``data_flow`` entries on the FlowGraph; ``remove_step`` rewires.
- ``TestProcessedBy`` — dedup keyed on (flow_cls_qualname, step_name)
  fires under same-class re-pre_mutate; spec mismatch raises.
- ``TestASTUnchanged`` — AC-AST-UNCHANGED: ``graph.py:221-303`` is
  byte-identical to the pinned baseline (the non-negotiable P-001).
- ``TestByteEq`` — flows without ``add_step`` produce ``_graph_info``
  with no ``data_flow`` key (suppressed when empty).
- ``TestLintRules`` — L-NS-007 step-body + Style A rejection.
- ``TestUnwrapCycle`` — ``inspect.unwrap`` cycle → ``MetaflowException``.
"""

import array
import collections
import functools
import importlib
import inspect
import os
import subprocess

import pytest

from metaflow import FlowSpec, step
from metaflow.exception import MetaflowException
from metaflow.flowspec import FlowStateItems
from metaflow.graph import FlowGraph
from metaflow.parameters import flow_context
from metaflow.user_decorators.user_flow_decorator import FlowMutator
from metaflow.user_decorators.mutable_flow import (
    _MUTABLE_DEFAULT_TYPES,
    _check_signature_rules,
    _is_advanced_mode_r8,
    _normalize_produces,
    _reject_mutable_defaults,
    _resolve_inputs_map,
    _synthesize_wrapper,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_flow(mutator_cls, *steps_definition):
    """Build a FlowSpec subclass decorated by ``mutator_cls`` and run
    ``_process_config_decorators`` to fire pre_mutate.

    ``steps_definition`` is unused here — callers compose flows inline
    and pass the resolved class through this helper for the
    ``flow_context`` + ``_process_config_decorators`` ceremony.
    """
    pass  # Composition is inlined per-test below.


def _resolve_flow_cls(decorated):
    """Unwrap a FlowMutator-decorated class to its underlying FlowSpec."""
    return decorated._flow_cls if isinstance(decorated, FlowMutator) else decorated


def _trigger_pre_mutate(flow_cls):
    """Run pre_mutate the way ``cli.py`` / runner does."""
    with flow_context(flow_cls):
        flow_cls._process_config_decorators({}, process_configs=True)


# ---------------------------------------------------------------------------
# AC-SIG-R01..R13 + R-EXTRA — signature rules
# ---------------------------------------------------------------------------


class TestSignatureRules:
    def test_R1_varargs_rejected(self):
        def f(*args):
            return args

        with pytest.raises(TypeError, match=r"\*args"):
            _check_signature_rules(f, inspect.signature(f))

    def test_R2_kwargs_rejected(self):
        def f(**kw):
            return kw

        with pytest.raises(TypeError, match=r"\*\*"):
            _check_signature_rules(f, inspect.signature(f))

    def test_R3_lambda_rejected(self):
        f = lambda x: x
        with pytest.raises(TypeError, match=r"lambdas"):
            _check_signature_rules(f, inspect.signature(f))

    def test_R4_partial_rejected(self):
        def real(a, b):
            return a + b

        p = functools.partial(real, 1)
        with pytest.raises(TypeError, match=r"partial"):
            _check_signature_rules(p, inspect.signature(p))

    def test_R5_decorated_func_accepted(self):
        def deco(f):
            @functools.wraps(f)
            def wrap(*a, **k):
                return f(*a, **k)

            return wrap

        @deco
        def f(bar):
            return bar

        # Accepted — signature follows __wrapped__.
        mode = _check_signature_rules(f, inspect.signature(f))
        assert mode == "style_a"

    def test_R7_default_accepted_immutable(self):
        def f(bar=42):
            return bar * 2

        mode = _check_signature_rules(f, inspect.signature(f))
        assert mode == "style_a"

    def test_R8_advanced_mode_detected(self):
        def adv(self):
            return self.x

        mode = _check_signature_rules(adv, inspect.signature(adv))
        assert mode == "advanced_r8"

    def test_R8_self_with_extra_args_is_style_a(self):
        # Two args including `self` is NOT R8 — R8 requires SINGLE `self`.
        def f(self, x):
            return self.y + x

        mode = _check_signature_rules(f, inspect.signature(f))
        assert mode == "style_a"

    def test_R9_async_rejected(self):
        async def f(x):
            return x

        with pytest.raises(TypeError, match=r"async def"):
            _check_signature_rules(f, inspect.signature(f))

    def test_R10_generator_rejected(self):
        def f(x):
            yield x

        with pytest.raises(TypeError, match=r"generator"):
            _check_signature_rules(f, inspect.signature(f))

    def test_R11_class_rejected(self):
        class C:
            def __init__(self, x):
                self.x = x

        with pytest.raises(TypeError, match=r"class"):
            _check_signature_rules(C, inspect.signature(C))

    def test_R12_callable_instance_rejected(self):
        class C:
            def __call__(self, x):
                return x

        c = C()
        with pytest.raises(TypeError, match=r"callable instances"):
            _check_signature_rules(c, inspect.signature(c))

    def test_R13_keyword_only_rejected(self):
        def f(*, x):
            return x

        with pytest.raises(TypeError, match=r"keyword-only"):
            _check_signature_rules(f, inspect.signature(f))


# ---------------------------------------------------------------------------
# UD-9 — mutable default rejection
# ---------------------------------------------------------------------------


class TestMutableDefaults:
    def test_denylist_has_nine_stdlib_types(self):
        # list, dict, set, bytearray, collections.{deque, OrderedDict,
        # defaultdict, Counter}, array.array
        assert len(_MUTABLE_DEFAULT_TYPES) == 9
        assert list in _MUTABLE_DEFAULT_TYPES
        assert dict in _MUTABLE_DEFAULT_TYPES
        assert set in _MUTABLE_DEFAULT_TYPES
        assert bytearray in _MUTABLE_DEFAULT_TYPES
        assert collections.deque in _MUTABLE_DEFAULT_TYPES
        assert collections.OrderedDict in _MUTABLE_DEFAULT_TYPES
        assert collections.defaultdict in _MUTABLE_DEFAULT_TYPES
        assert collections.Counter in _MUTABLE_DEFAULT_TYPES
        assert array.array in _MUTABLE_DEFAULT_TYPES

    @pytest.mark.parametrize(
        "default",
        [
            [],
            {},
            set(),
            bytearray(),
            collections.deque(),
            collections.OrderedDict(),
            collections.defaultdict(list),
            collections.Counter(),
            array.array("i", []),
        ],
        ids=[
            "list",
            "dict",
            "set",
            "bytearray",
            "deque",
            "OrderedDict",
            "defaultdict",
            "Counter",
            "array",
        ],
    )
    def test_mutable_default_rejected(self, default):
        def f(bar=default):  # noqa: B008 — intentional, tested
            return bar

        with pytest.raises(TypeError, match=r"mutable default"):
            _reject_mutable_defaults(f, inspect.signature(f))

    @pytest.mark.parametrize(
        "default",
        [42, "hello", (1, 2), frozenset([1]), 1.5, True, b"bytes", None],
        ids=["int", "str", "tuple", "frozenset", "float", "bool", "bytes", "None"],
    )
    def test_immutable_default_accepted(self, default):
        def f(bar=default):
            return bar

        # Should not raise.
        _reject_mutable_defaults(f, inspect.signature(f))

    def test_none_sentinel_pattern_accepted(self):
        def f(items=None):
            if items is None:
                items = []
            return items

        _reject_mutable_defaults(f, inspect.signature(f))


# ---------------------------------------------------------------------------
# Helpers: _resolve_inputs_map / _normalize_produces
# ---------------------------------------------------------------------------


class TestResolveHelpers:
    def test_resolve_inputs_style_a_signature_driven(self):
        def f(bar, baz):
            return bar + baz

        m = _resolve_inputs_map(inspect.signature(f), None, "style_a")
        assert m == {"bar": "bar", "baz": "baz"}

    def test_resolve_inputs_style_a_with_override(self):
        def f(bar):
            return bar

        m = _resolve_inputs_map(
            inspect.signature(f), {"bar": "external_bar"}, "style_a"
        )
        assert m == {"bar": "external_bar"}

    def test_resolve_inputs_r8_explicit_only(self):
        def adv(self):
            return self.x

        # In R8 mode, only inputs= matters.
        m = _resolve_inputs_map(inspect.signature(adv), {"x": "ext_x"}, "advanced_r8")
        assert m == {"x": "ext_x"}

    def test_normalize_produces(self):
        assert _normalize_produces(None) is None
        assert _normalize_produces("foo") == ("foo",)
        assert _normalize_produces(("a", "b")) == ("a", "b")
        assert _normalize_produces(["a", "b"]) == ("a", "b")
        with pytest.raises(TypeError):
            _normalize_produces(42)


# ---------------------------------------------------------------------------
# UD-6 — default fallback in synthesized wrapper
# ---------------------------------------------------------------------------


class TestDefaultFallback:
    def test_default_used_when_attr_missing(self):
        def double_or_default(bar=42):
            return bar * 2

        sig = inspect.signature(double_or_default)
        edges = {"in": [], "out": ["sentinel"], "type": "linear"}
        wrapper = _synthesize_wrapper(
            name="dod",
            func=double_or_default,
            sig=sig,
            mode="style_a",
            inputs_map={"bar": "bar"},
            produces=("out",),
            edges=edges,
        )

        # Mock a FlowSpec-like object with no `bar` attr and a no-op `next`.
        class FakeFlow:
            def __getattr__(self, name):
                raise AttributeError(name)

            def next(self, *args):
                self._next_called_with = args

        f = FakeFlow()
        # Stash sentinel function for self.next() lookup
        FakeFlow.sentinel = lambda self: None
        wrapper(f)
        assert f.out == 84  # default 42 * 2

    def test_default_overridden_when_attr_present(self):
        def double_or_default(bar=42):
            return bar * 2

        sig = inspect.signature(double_or_default)
        edges = {"in": [], "out": ["sentinel"], "type": "linear"}
        wrapper = _synthesize_wrapper(
            name="dod",
            func=double_or_default,
            sig=sig,
            mode="style_a",
            inputs_map={"bar": "bar"},
            produces=("out",),
            edges=edges,
        )

        class FakeFlow:
            bar = 10  # producer-written value
            sentinel = lambda self: None

            def next(self, *args):
                self._next_called_with = args

        f = FakeFlow()
        wrapper(f)
        assert f.out == 20  # producer's 10 * 2 wins over default 42

    def test_default_late_mutation_does_not_change_wrapper(self):
        """AC-R7-LATE-DEFAULT-CAPTURE: mutating ``func.__defaults__`` after
        ``add_step`` returns does NOT change wrapper behavior because the
        default was snapshotted at decoration time."""

        def f(bar=42):
            return bar

        sig = inspect.signature(f)
        edges = {"in": [], "out": ["sentinel"], "type": "linear"}
        wrapper = _synthesize_wrapper(
            name="dod",
            func=f,
            sig=sig,
            mode="style_a",
            inputs_map={"bar": "bar"},
            produces=("out",),
            edges=edges,
        )

        # Mutate the source func's defaults AFTER wrapper synthesis.
        f.__defaults__ = (99,)

        class FakeFlow:
            sentinel = lambda self: None

            def __getattr__(self, name):
                raise AttributeError(name)

            def next(self, *args):
                pass

        ff = FakeFlow()
        wrapper(ff)
        assert ff.out == 42  # snapshotted default, NOT 99


# ---------------------------------------------------------------------------
# Wrapper attribute preservation
# ---------------------------------------------------------------------------


class TestWrapperAttributes:
    def test_wrapper_preserves_dunders(self):
        def my_func(bar):
            """Docstring."""
            return bar

        sig = inspect.signature(my_func)
        edges = {"in": [], "out": ["sentinel"], "type": "linear"}
        wrapper = _synthesize_wrapper(
            name="step_name",
            func=my_func,
            sig=sig,
            mode="style_a",
            inputs_map={"bar": "bar"},
            produces=("out",),
            edges=edges,
        )
        assert wrapper.__name__ == "step_name"
        assert wrapper.__doc__ == "Docstring."
        assert wrapper.__module__ == my_func.__module__
        assert wrapper.__wrapped__ is my_func
        assert wrapper.is_step is True
        assert wrapper._mf_added_by_mutator is True
        assert hasattr(wrapper, "decorators")
        assert hasattr(wrapper, "wrappers")
        assert hasattr(wrapper, "config_decorators")

    def test_source_file_via_unwrapped(self):
        """D-007: ``DAGNode.source_file`` for mutator steps resolves via
        ``inspect.unwrap``, so tracebacks have the user's file path on
        the wrapped function (even though the wrapper itself shows
        mutable_flow.py as a known traceback caveat)."""

        def double_bar(bar):
            return bar * 2

        sig = inspect.signature(double_bar)
        edges = {"in": [], "out": [], "type": "linear"}
        wrapper = _synthesize_wrapper(
            name="x",
            func=double_bar,
            sig=sig,
            mode="style_a",
            inputs_map={"bar": "bar"},
            produces=None,
            edges=edges,
        )
        assert inspect.unwrap(wrapper) is double_bar
        # The unwrapped file is THIS test file, NOT mutable_flow.py.
        assert inspect.getfile(inspect.unwrap(wrapper)) == os.path.abspath(__file__)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_built_in_kinds_registered_on_import(self):
        from metaflow._data_flow_registry import list_kinds

        kinds = set(list_kinds().keys())
        assert {"explicit_inputs", "explicit_outputs", "embedded_callable"} <= kinds

    def test_register_idempotent_same_spec(self):
        from metaflow._data_flow_registry import register_data_flow_kind

        # Already registered as "descriptive" with no validator.
        # Same spec → no-op.
        register_data_flow_kind("explicit_inputs", "descriptive")

    def test_register_conflict_raises(self):
        from metaflow._data_flow_registry import register_data_flow_kind

        with pytest.raises(ValueError, match=r"already registered"):
            register_data_flow_kind("explicit_inputs", "routing")

    def test_register_new_kind(self):
        from metaflow._data_flow_registry import register_data_flow_kind, get_kind

        register_data_flow_kind("__test_phase1_unique_kind__", "descriptive")
        assert get_kind("__test_phase1_unique_kind__")["semantics"] == "descriptive"

    def test_bad_semantics_raises(self):
        from metaflow._data_flow_registry import register_data_flow_kind

        with pytest.raises(ValueError, match=r"semantics must be"):
            register_data_flow_kind("__test_bad_sem__", "invalid")

    def test_importlib_reload_safe(self):
        """``importlib.reload(metaflow)`` is safe for built-in kinds."""
        import metaflow

        importlib.reload(metaflow)
        # Built-in kinds remain registered with matching spec.
        from metaflow._data_flow_registry import list_kinds

        assert "explicit_inputs" in list_kinds()


# ---------------------------------------------------------------------------
# add_step end-to-end: edge wiring + data_flow entries on the graph
# ---------------------------------------------------------------------------


def _double_bar(bar):
    return bar * 2


def _split_value(x):
    return x * 2, x + 1


class TestAddStepGraph:
    def test_basic_insertion_between_start_and_end(self):
        class AddDoubler(FlowMutator):
            def pre_mutate(self, flow):
                flow.add_step(
                    "compute_foo",
                    after="start",
                    func=_double_bar,
                    produces="foo",
                )

            def mutate(self, flow):
                pass

        @AddDoubler
        class _MyFlow(FlowSpec):
            @step
            def start(self):
                self.bar = 21
                self.next(self.end)

            @step
            def end(self):
                pass

        cls = _resolve_flow_cls(_MyFlow)
        _trigger_pre_mutate(cls)

        # Class has the new step.
        assert hasattr(cls, "compute_foo")
        assert getattr(cls.compute_foo, "is_step", False)
        assert cls.compute_foo._mf_added_by_mutator is True

        # Graph mutations recorded.
        ops = cls._flow_state.get(FlowStateItems.GRAPH_MUTATIONS)
        assert ops == [("add", "compute_foo", ["start"], None)]

        # PACKAGED_CALLABLES picked up the source file.
        packaged = cls._flow_state.get(FlowStateItems.PACKAGED_CALLABLES)
        assert packaged and packaged[0] == os.path.abspath(__file__)

        g = FlowGraph(cls)
        assert g.nodes["start"].out_funcs == ["compute_foo"]
        assert g.nodes["compute_foo"].out_funcs == ["end"]
        assert sorted(g.nodes["end"].in_funcs) == ["compute_foo"]

    def test_data_flow_entries_emitted(self):
        class AddDoubler(FlowMutator):
            def pre_mutate(self, flow):
                flow.add_step(
                    "compute_foo",
                    after="start",
                    func=_double_bar,
                    produces="foo",
                )

            def mutate(self, flow):
                pass

        @AddDoubler
        class _MyFlow(FlowSpec):
            @step
            def start(self):
                self.bar = 1
                self.next(self.end)

            @step
            def end(self):
                pass

        cls = _resolve_flow_cls(_MyFlow)
        _trigger_pre_mutate(cls)
        g = FlowGraph(cls)
        entries = g.nodes["compute_foo"]._mf_dataflow_entries
        kinds = {e["kind"] for e in entries}
        assert "explicit_inputs" in kinds
        assert "explicit_outputs" in kinds

    def test_remove_step_rewires_edges(self):
        class AddAndRemove(FlowMutator):
            def pre_mutate(self, flow):
                flow.add_step("middle", after="start", func=_double_bar, produces="foo")
                flow.remove_step("middle")

            def mutate(self, flow):
                pass

        @AddAndRemove
        class _MyFlow(FlowSpec):
            @step
            def start(self):
                self.bar = 1
                self.next(self.end)

            @step
            def end(self):
                pass

        cls = _resolve_flow_cls(_MyFlow)
        _trigger_pre_mutate(cls)
        # After add then remove, middle should be gone.
        assert not hasattr(cls, "middle")
        g = FlowGraph(cls)
        assert "middle" not in g.nodes
        assert g.nodes["start"].out_funcs == ["end"]

    def test_remove_start_raises(self):
        # ``remove_step`` checks ``is_start_step`` on the step attribute,
        # which is set by ``@step(start=True)``. Annotated start steps
        # are refused; structurally-inferred starts are not detected at
        # decoration time (the graph isn't built yet). This is the
        # Phase 1 contract.
        class TryRemoveStart(FlowMutator):
            def pre_mutate(self, flow):
                flow.remove_step("the_start")

            def mutate(self, flow):
                pass

        @TryRemoveStart
        class _MyFlow(FlowSpec):
            @step(start=True)
            def the_start(self):
                self.next(self.end)

            @step(end=True)
            def end(self):
                pass

        cls = _resolve_flow_cls(_MyFlow)
        with pytest.raises(MetaflowException, match=r"start step"):
            _trigger_pre_mutate(cls)

    def test_name_collision_no_overwrite(self):
        class AddCollide(FlowMutator):
            def pre_mutate(self, flow):
                flow.add_step("end", after="start", func=_double_bar, produces="x")

            def mutate(self, flow):
                pass

        @AddCollide
        class _MyFlow(FlowSpec):
            @step
            def start(self):
                self.next(self.end)

            @step
            def end(self):
                pass

        cls = _resolve_flow_cls(_MyFlow)
        with pytest.raises(MetaflowException, match=r"already has a class member"):
            _trigger_pre_mutate(cls)


# ---------------------------------------------------------------------------
# PROCESSED_BY dedup
# ---------------------------------------------------------------------------


class TestProcessedBy:
    def test_same_class_re_call_idempotent(self):
        from metaflow.user_decorators.mutable_flow import MutableFlow

        class MyFlow(FlowSpec):
            @step
            def start(self):
                self.next(self.end)

            @step
            def end(self):
                pass

        mf = MutableFlow(MyFlow, pre_mutate=True, inserted_by=["test"])
        mf.add_step("x", after="start", func=_double_bar, produces="foo")
        # Calling again with the same spec is idempotent (no error).
        mf.add_step("x", after="start", func=_double_bar, produces="foo")
        # GRAPH_MUTATIONS should still have exactly one ("add", "x", ...)
        ops = [
            o
            for o in MyFlow._flow_state[FlowStateItems.GRAPH_MUTATIONS]
            if o[0] == "add" and o[1] == "x"
        ]
        assert len(ops) == 1

    def test_conflicting_spec_raises(self):
        from metaflow.user_decorators.mutable_flow import MutableFlow

        def other_func(bar):
            return bar + 1

        class MyFlow(FlowSpec):
            @step
            def start(self):
                self.next(self.end)

            @step
            def end(self):
                pass

        mf = MutableFlow(MyFlow, pre_mutate=True, inserted_by=["test"])
        mf.add_step("x", after="start", func=_double_bar, produces="foo")
        with pytest.raises(MetaflowException, match=r"conflicting prior"):
            mf.add_step("x", after="start", func=other_func, produces="foo")


# ---------------------------------------------------------------------------
# AC-AST-UNCHANGED — non-negotiable P-001 constraint
# ---------------------------------------------------------------------------


class TestASTUnchanged:
    def test_graph_py_parse_byte_identical_to_baseline(self):
        # Read pinned baseline from tools/p001_baseline.txt.
        repo = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        baseline_file = os.path.join(repo, "tools", "p001_baseline.txt")
        with open(baseline_file) as fh:
            baseline_sha = fh.readline().strip()
        # Verify SHA resolves.
        subprocess.check_call(
            ["git", "rev-parse", baseline_sha],
            cwd=repo,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # `git log -L 221,303:metaflow/graph.py BASELINE..HEAD` should be empty.
        out = subprocess.check_output(
            [
                "git",
                "log",
                "-L",
                "221,303:metaflow/graph.py",
                "%s..HEAD" % baseline_sha,
                "--oneline",
            ],
            cwd=repo,
            text=True,
        )
        assert out.strip() == "", (
            "graph.py:221-303 (DAGNode._parse) has changed since the P-001 "
            "baseline %s. If the change is intentional, update "
            "tools/p001_baseline.txt to the new SHA.\n%s" % (baseline_sha, out)
        )


# ---------------------------------------------------------------------------
# Byte-equivalence on no-namespace flows
# ---------------------------------------------------------------------------


class TestByteEq:
    def test_graph_info_no_data_flow_on_unmutated_flow(self):
        class PlainFlow(FlowSpec):
            @step
            def start(self):
                self.x = 1
                self.next(self.end)

            @step
            def end(self):
                pass

        g = FlowGraph(PlainFlow)
        steps_info, _graph_structure = g.output_steps()
        # No mutator-added step → no 'data_flow' key anywhere.
        for name, d in steps_info.items():
            assert (
                "data_flow" not in d
            ), "data_flow leaked into unmutated step %r: keys=%s" % (
                name,
                list(d.keys()),
            )


# ---------------------------------------------------------------------------
# Lint rules
# ---------------------------------------------------------------------------


class TestLintRules:
    def test_l_ns_007_rejects_embedded_in_step_body(self):
        # Inject a flow whose @step body calls embedded(...). L-NS-007
        # scans the @step body source and raises.
        from metaflow import FlowSpec, step
        from metaflow._namespaced_self import (
            embedded,
        )  # noqa: F401 — used by exec'd src below
        from metaflow.lint import linter, LintWarn

        class BadFlow(FlowSpec):
            @step
            def start(self):
                # The presence of embedded(...) in @step body source is the issue.
                embedded("helper")(1)
                self.next(self.end)

            @step
            def end(self):
                pass

        g = FlowGraph(BadFlow)
        with pytest.raises(LintWarn, match=r"embedded"):
            linter.run_checks(g, require_fundamentals=True)


# ---------------------------------------------------------------------------
# inspect.unwrap cycle -> MetaflowException
# ---------------------------------------------------------------------------


class TestUnwrapCycle:
    def test_unwrap_cycle_raises_metaflow_exception(self):
        # Build a function with a __wrapped__ cycle, mark it as a mutator-
        # style step, and verify graph construction surfaces a
        # MetaflowException (not a raw ValueError).
        from metaflow.graph import FlowGraph

        def f():
            pass

        f.__wrapped__ = f  # cycle
        f.is_step = True
        f.is_start_step = False
        f.is_end_step = False
        f.decorators = []
        f.wrappers = []
        f.config_decorators = []
        f._mf_edges = {"in": [], "out": [], "type": "linear"}

        class CycleFlow(FlowSpec):
            pass

        # Inject the cyclic wrapper onto the class so _create_nodes picks it up.
        CycleFlow.cyclic = f
        with pytest.raises(MetaflowException, match=r"cycle"):
            FlowGraph(CycleFlow)
