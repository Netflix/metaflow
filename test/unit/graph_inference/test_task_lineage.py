from pathlib import Path

import pytest

from metaflow import Runner


@pytest.fixture(scope="module")
def conditional_foreach_run(tmp_path_factory):
    flow_file = Path(__file__).with_name("flows") / "conditional_foreach_join_flow.py"
    cwd = tmp_path_factory.mktemp("conditional_foreach")
    with Runner(
        str(flow_file),
        cwd=str(cwd),
        metadata="local",
        datastore="local",
        show_output=False,
    ).run() as execution:
        run = execution.run
        assert run.successful
        yield run


def test_foreach_join_lists_parents_from_conditional_branches(conditional_foreach_run):
    join_tasks = list(conditional_foreach_run["join_items"])
    assert len(join_tasks) == 2
    for task in join_tasks:
        assert {parent.pathspec for parent in task.parent_tasks} == set(
            task.data.input_pathspecs
        )


def test_conditional_tasks_list_foreach_join_as_child(conditional_foreach_run):
    join_by_input = {
        pathspec: task.pathspec
        for task in conditional_foreach_run["join_items"]
        for pathspec in task.data.input_pathspecs
    }
    for branch in ("left", "right"):
        for task in conditional_foreach_run[branch]:
            assert [child.pathspec for child in task.child_tasks] == [
                join_by_input[task.pathspec]
            ]
