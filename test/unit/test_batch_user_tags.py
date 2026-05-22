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


def test_user_tags_not_inside_emit_tags_guard():
    """
    Verify that the aws_batch_tags block in batch.py is NOT inside
    the BATCH_EMIT_TAGS conditional. This is a structural test that
    checks the source code's AST to ensure user tags are always applied.
    """
    from metaflow.plugins.aws.batch.batch import Batch

    source = inspect.getsource(Batch.create_job)
    # dedent because inspect.getsource preserves class indentation
    source = textwrap.dedent(source)
    tree = ast.parse(source)

    # Find all If nodes that test BATCH_EMIT_TAGS
    def find_emit_tags_guards(node):
        """Find all 'if BATCH_EMIT_TAGS:' blocks and return their AST nodes."""
        guards = []
        for child in ast.walk(node):
            if isinstance(child, ast.If):
                # Check if the test is just 'BATCH_EMIT_TAGS'
                test = child.test
                if isinstance(test, ast.Name) and test.id == "BATCH_EMIT_TAGS":
                    guards.append(child)
        return guards

    def contains_aws_batch_tags_usage(node):
        """Check if an AST node contains reference to 'aws_batch_tags'."""
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and child.id == "aws_batch_tags":
                return True
        return False

    guards = find_emit_tags_guards(tree)
    assert guards, "Could not find 'if BATCH_EMIT_TAGS:' block in create_job"

    for guard in guards:
        # Check the body of each BATCH_EMIT_TAGS guard
        for stmt in guard.body:
            assert not contains_aws_batch_tags_usage(stmt), (
                "aws_batch_tags is used inside 'if BATCH_EMIT_TAGS:' block. "
                "User-defined tags should be applied unconditionally, outside "
                "the BATCH_EMIT_TAGS guard."
            )