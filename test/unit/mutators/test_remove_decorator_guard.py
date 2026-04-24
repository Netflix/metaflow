"""Regression test for remove_decorator's pre_mutate guard on StepMutators.

Before the fix, calling `remove_decorator(name)` with no args/kwargs
(`do_all=True`) on a StepMutator from `mutate()` would silently strip the
StepMutator, bypassing the documented contract that StepMutators can only be
removed during `pre_mutate`. The guard is now checked on both the do_all path
and the specific-match path.
"""

import pytest

from metaflow import FlowSpec, step
from metaflow.exception import MetaflowException
from metaflow.user_decorators.mutable_step import MutableStep
from metaflow.user_decorators.user_step_decorator import StepMutator


class dummy_step_mutator(StepMutator):
    """A minimal StepMutator used as a removal target."""

    def mutate(self, mutable_step):
        pass


class FlowWithStepMutator(FlowSpec):

    @dummy_step_mutator
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def _make_mutable_step(pre_mutate):
    cls = FlowWithStepMutator
    step_obj = getattr(cls, "start")
    return MutableStep(
        cls,
        step_obj,
        pre_mutate=pre_mutate,
        statically_defined=True,
        inserted_by=["test"],
    )


class TestRemoveDecoratorGuard:
    def test_do_all_from_mutate_raises(self):
        """Calling remove_decorator(name) without args/kwargs (do_all=True)
        from a non-pre_mutate MutableStep must raise on a StepMutator."""
        ms = _make_mutable_step(pre_mutate=False)
        with pytest.raises(MetaflowException, match="only allowed in the `pre_mutate`"):
            ms.remove_decorator("dummy_step_mutator")

    def test_specific_match_from_mutate_raises(self):
        """Same guard applies when an explicit deco_args/deco_kwargs match
        is provided (this path already worked before the fix; keep covered)."""
        ms = _make_mutable_step(pre_mutate=False)
        with pytest.raises(MetaflowException, match="only allowed in the `pre_mutate`"):
            ms.remove_decorator("dummy_step_mutator", deco_args=[], deco_kwargs={})

    def test_do_all_from_pre_mutate_succeeds(self):
        """From pre_mutate, removing the StepMutator via do_all should work."""
        ms = _make_mutable_step(pre_mutate=True)
        removed = ms.remove_decorator("dummy_step_mutator")
        assert removed is True
