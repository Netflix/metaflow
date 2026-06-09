"""
Regression test for user-defined AWS Batch tags.

User-defined tags (aws_batch_tags) and METAFLOW_BATCH_DEFAULT_TAGS should
always be applied to Batch jobs, regardless of the BATCH_EMIT_TAGS setting.
BATCH_EMIT_TAGS controls only Metaflow's internal observability tags
(app=metaflow, metaflow.flow_name, etc.), not user-specified tags.

See: https://github.com/Netflix/metaflow/issues/3209
"""

import ast
import inspect
import textwrap

import pytest


def _get_create_job_ast():
    """Parse create_job source into an AST tree."""
    from metaflow.plugins.aws.batch.batch import Batch

    source = inspect.getsource(Batch.create_job)
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


def test_user_tags_not_inside_emit_tags_guard():
    """
    Verify that the aws_batch_tags block in batch.py is NOT inside
    the BATCH_EMIT_TAGS conditional.
    """
    tree = _get_create_job_ast()
    guards = _find_emit_tags_guards(tree)
    assert guards, "Could not find 'if BATCH_EMIT_TAGS:' block in create_job"

    for guard in guards:
        for stmt in guard.body:
            assert not _contains_name(stmt, "aws_batch_tags"), (
                "aws_batch_tags is used inside 'if BATCH_EMIT_TAGS:' block. "
                "User-defined tags should be applied unconditionally, outside "
                "the BATCH_EMIT_TAGS guard."
            )


def test_user_tags_are_applied_in_create_job():
    """
    Verify that aws_batch_tags is actually referenced in create_job
    at the top level (not just absent from the guard). Guards against
    silent removal of the entire feature.
    """
    tree = _get_create_job_ast()
    assert _contains_name(tree, "aws_batch_tags"), (
        "aws_batch_tags is not referenced anywhere in create_job. "
        "User-defined tag application may have been accidentally removed."
    )
