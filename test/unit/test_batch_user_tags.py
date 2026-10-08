"""
Regression test for user-defined AWS Batch tags.

aws_batch_tags (and the BATCH_DEFAULT_TAGS config default that merges into
it) require the Batch:TagResource IAM permission to apply, the same
permission that gates Metaflow's own internal observability tags via
BATCH_EMIT_TAGS. Applying user tags unconditionally, regardless of
BATCH_EMIT_TAGS, would turn a missing IAM permission into an outright
job-submission failure for any deployment relying on BATCH_DEFAULT_TAGS,
not just deployments that explicitly opted into tagging.

So aws_batch_tags stays gated behind BATCH_EMIT_TAGS like everything else
that requires Batch:TagResource. To keep the original failure mode
(silent, undiagnosable tag loss, see #3209) from recurring, batch_decorator
warns at flow-submission time, on the user's own machine, when tags were
requested but BATCH_EMIT_TAGS is not set, so the drop is at least visible
and explained rather than silent.

See: https://github.com/Netflix/metaflow/issues/3209
"""

import ast
import inspect
import textwrap
from unittest.mock import MagicMock, patch


def _get_create_job_ast():
    """Parse create_job source into an AST tree."""
    from metaflow.plugins.aws.batch.batch import Batch

    source = inspect.getsource(Batch.create_job)
    source = textwrap.dedent(source)
    return ast.parse(source)


def _get_step_init_ast():
    """Parse BatchDecorator.step_init source into an AST tree."""
    from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator

    source = inspect.getsource(BatchDecorator.step_init)
    source = textwrap.dedent(source)
    return ast.parse(source)


def _find_emit_tags_guards(tree):
    """Find all 'if BATCH_EMIT_TAGS:' blocks."""
    guards = []
    for child in ast.walk(tree):
        if isinstance(child, ast.If):
            test = child.test
            if isinstance(test, ast.Name) and test.id == "BATCH_EMIT_TAGS":
                guards.append(child)
    return guards


def _contains_name(node, name):
    """Check if an AST node contains a reference to the given name."""
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id == name:
            return True
    return False


def test_user_tags_inside_emit_tags_guard():
    """
    Verify that the aws_batch_tags block in batch.py IS inside the
    BATCH_EMIT_TAGS conditional, since applying tags requires the same
    Batch:TagResource IAM permission that guard exists to protect
    against. See module docstring for the full reasoning.
    """
    tree = _get_create_job_ast()
    guards = _find_emit_tags_guards(tree)
    assert guards, "Could not find 'if BATCH_EMIT_TAGS:' block in create_job"

    found_inside_guard = False
    for guard in guards:
        for stmt in guard.body:
            if _contains_name(stmt, "aws_batch_tags"):
                found_inside_guard = True

    assert found_inside_guard, (
        "aws_batch_tags is not used inside any 'if BATCH_EMIT_TAGS:' block "
        "in create_job. User-defined tags require the same IAM permission "
        "as Metaflow's internal tags and must be gated the same way."
    )


def test_user_tags_still_applied_when_emitted():
    """
    Verify that aws_batch_tags is still referenced somewhere in
    create_job at all. Guards against silent removal of the entire
    feature while revising the BATCH_EMIT_TAGS gating.
    """
    tree = _get_create_job_ast()
    assert _contains_name(tree, "aws_batch_tags"), (
        "aws_batch_tags is not referenced anywhere in create_job. "
        "User-defined tag application may have been accidentally removed."
    )


def _contains_attribute_call(tree, attr_name):
    """Check if an AST tree contains a call to self.<attr_name>(...).

    _contains_name only matches bare ast.Name nodes, so it cannot find
    method calls like self._warn_if_tags_will_be_dropped(...), which
    parse as ast.Attribute nodes (a self.<attr> access), not ast.Name.
    This is the Attribute-aware equivalent, used specifically for
    checking method calls rather than bare identifiers.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == attr_name:
            return True
    return False


def test_step_init_calls_the_tag_drop_warning():
    """
    Structural check that step_init still calls
    _warn_if_tags_will_be_dropped when aws_batch_tags is set, guarding
    against the call being silently removed during a future refactor.
    The warning logic itself is exercised directly and behaviorally by
    the test_warns_* / test_no_warning_when_emit_tags_true tests below,
    this test only confirms step_init still wires it up.
    """
    tree = _get_step_init_ast()
    assert _contains_attribute_call(tree, "_warn_if_tags_will_be_dropped"), (
        "step_init no longer calls _warn_if_tags_will_be_dropped. The "
        "flow-submission-time warning for dropped aws_batch_tags may "
        "have been accidentally removed."
    )


def _make_decorator(explicit_tags, aws_batch_tags):
    """Minimal BatchDecorator, bypassing __init__, for testing
    _warn_if_tags_will_be_dropped directly without needing a real flow,
    graph, or environment. Matches the object.__new__ pattern already
    used in test_s3_storage.py for the same kind of narrow, dependency-free
    unit test.
    """
    from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator

    deco = object.__new__(BatchDecorator)
    deco.attributes = {"aws_batch_tags": aws_batch_tags}
    deco._explicit_aws_batch_tags = explicit_tags
    return deco


def test_warns_with_explicit_tag_source_message():
    """When aws_batch_tags came from this step's own decorator argument,
    the warning should say so specifically, not just that tags were set.
    """
    with patch("metaflow.plugins.aws.batch.batch_decorator.BATCH_EMIT_TAGS", False):
        deco = _make_decorator(explicit_tags=True, aws_batch_tags={"CostCenter": "123"})
        logger = MagicMock()

        deco._warn_if_tags_will_be_dropped(logger)

        logger.assert_called_once()
        msg = logger.call_args[0][0]
        assert "aws_batch_tags argument on this step's @batch decorator" in msg
        assert logger.call_args[1].get("system_msg") is True


def test_warns_with_default_tag_source_message():
    """When aws_batch_tags came only from BATCH_DEFAULT_TAGS (no explicit
    decorator argument), the warning should attribute it to the config
    default, not the decorator, since the user may not have set anything
    on this specific step themselves.
    """
    with patch("metaflow.plugins.aws.batch.batch_decorator.BATCH_EMIT_TAGS", False):
        deco = _make_decorator(
            explicit_tags=False, aws_batch_tags={"CostCenter": "123"}
        )
        logger = MagicMock()

        deco._warn_if_tags_will_be_dropped(logger)

        logger.assert_called_once()
        msg = logger.call_args[0][0]
        assert "BATCH_DEFAULT_TAGS configuration default" in msg


def test_no_warning_when_emit_tags_true():
    """When BATCH_EMIT_TAGS is true, tags will actually be applied, so
    no warning should fire regardless of tag source.
    """
    with patch("metaflow.plugins.aws.batch.batch_decorator.BATCH_EMIT_TAGS", True):
        deco = _make_decorator(explicit_tags=True, aws_batch_tags={"CostCenter": "123"})
        logger = MagicMock()

        deco._warn_if_tags_will_be_dropped(logger)

        logger.assert_not_called()
