import pytest
import contextlib

from metaflow.plugins.argo.argo_workflows_cli import (
    resolve_workflow_name,
    old_resolve_workflow_name,
    ArgoWorkflowsNameTooLong,
)
from metaflow import current
from metaflow.current import Current


class DummyObject(object):
    # For testing workflow name generation
    def __init__(self, name) -> None:
        self.name = name


@contextlib.contextmanager
def mocked_current(flow, project=None, branch=None):
    prev = current._flow_name
    current._flow_name = flow
    if project is not None:
        current._update_env(
            {
                "project_name": project,
                "branch_name": branch,
                "project_flow_name": "%s.%s.%s" % (project, branch, flow),
            }
        )
    yield
    # cleanup
    try:
        current._flow_name = prev
        delattr(Current, "project_name")
        delattr(Current, "branch_name")
        delattr(Current, "project_flow_name")
    except Exception:
        pass


# Test 253 character length names


@pytest.mark.parametrize(
    "name,expected_name",
    [
        ("test-flow", "test-flow"),
        ("test.flow.with.dots", "test.flow.with.dots"),
    ],
)
def test_old_workflow_names_with_name(name, expected_name):
    _obj = DummyObject("test")
    workflow_name, token_prefix, is_project = old_resolve_workflow_name(_obj, name)
    assert expected_name == workflow_name


@pytest.mark.parametrize(
    "flow, project, branch ,name,expected_name",
    [
        ("flow", "project", "user.test", None, "project.user.test.flow"),
        ("TestFlow", None, None, None, "testflow"),
    ],
)
def test_old_workflow_names_with_object(flow, project, branch, name, expected_name):
    with mocked_current(flow, project, branch):
        obj = DummyObject(name)
        workflow_name, token_prefix, is_project = old_resolve_workflow_name(obj, name)
        assert expected_name == workflow_name


def test_old_workflow_names_errors():
    long_name = 254 * "a"
    obj = DummyObject("test")
    with pytest.raises(ArgoWorkflowsNameTooLong):
        old_resolve_workflow_name(obj, long_name)

    with pytest.raises(ArgoWorkflowsNameTooLong):
        with mocked_current(long_name):
            old_resolve_workflow_name(obj, None)

    with mocked_current(long_name, "project_a", "user.b"):
        # should truncate instead
        workflow_name, _, _ = old_resolve_workflow_name(obj, None)
        assert "projecta.user.b" in workflow_name
        assert len(workflow_name) == 250


# Test 63 character length names
@pytest.mark.parametrize(
    "obj,name,expected_name",
    [
        ("test-flow", "test-flow"),
        ("test.flow.with.dots", "test.flow.with.dots"),
        ("TestFlow", "testflow"),
    ],
)
def test_new_workflow_names_with_object(name, expected_name):
    _obj = DummyObject("test")
    workflow_name, token_prefix, is_project = resolve_workflow_name(_obj, name)
    assert expected_name == workflow_name


@pytest.mark.parametrize(
    "flow, project, branch ,name,expected_name",
    [
        ("flow", "project", "user.test", None, "project.user.test.flow"),
        ("TestFlow", None, None, None, "testflow"),
    ],
)
def test_new_workflow_names_with_object(flow, project, branch, name, expected_name):
    with mocked_current(flow, project, branch):
        obj = DummyObject(name)
        workflow_name, token_prefix, is_project = resolve_workflow_name(obj, name)
        assert expected_name == workflow_name


def test_new_workflow_names_errors():
    long_name = 64 * "a"
    obj = DummyObject("test")
    with pytest.raises(ArgoWorkflowsNameTooLong):
        resolve_workflow_name(obj, long_name)

    with pytest.raises(ArgoWorkflowsNameTooLong):
        with mocked_current(long_name):
            resolve_workflow_name(obj, None)

    with mocked_current(long_name, "project_a", "user.b"):
        # should truncate instead
        workflow_name, _, _ = resolve_workflow_name(obj, None)
        assert "projecta.user.b" in workflow_name
        assert len(workflow_name) == 60
