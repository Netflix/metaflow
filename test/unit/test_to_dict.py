from unittest.mock import patch, PropertyMock
from datetime import datetime, timezone


def test_run_to_dict_keys():
    from metaflow.client.core import Run
    run = Run.__new__(Run)
    run.id = "123"
    with patch.object(Run, 'pathspec', new_callable=PropertyMock, return_value="TestFlow/123"), \
         patch.object(Run, 'successful', new_callable=PropertyMock, return_value=True), \
         patch.object(Run, 'finished', new_callable=PropertyMock, return_value=True), \
         patch.object(Run, 'finished_at', new_callable=PropertyMock, return_value=datetime(2024, 1, 1, tzinfo=timezone.utc)), \
         patch.object(Run, 'created_at', new_callable=PropertyMock, return_value=datetime(2024, 1, 1, tzinfo=timezone.utc)), \
         patch.object(Run, 'tags', new_callable=PropertyMock, return_value=frozenset(["prod"])), \
         patch.object(Run, 'user_tags', new_callable=PropertyMock, return_value=frozenset(["prod"])), \
         patch.object(Run, 'origin_pathspec', new_callable=PropertyMock, return_value=None):
        result = run.to_dict()
    expected_keys = {"pathspec", "id", "successful", "finished", "finished_at", "created_at", "tags", "user_tags", "origin_pathspec"}
    assert expected_keys == set(result.keys())


def test_run_to_dict_finished_at_is_isoformat():
    from metaflow.client.core import Run
    run = Run.__new__(Run)
    run.id = "1"
    dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    with patch.object(Run, 'pathspec', new_callable=PropertyMock, return_value="TestFlow/1"), \
         patch.object(Run, 'successful', new_callable=PropertyMock, return_value=False), \
         patch.object(Run, 'finished', new_callable=PropertyMock, return_value=False), \
         patch.object(Run, 'finished_at', new_callable=PropertyMock, return_value=dt), \
         patch.object(Run, 'created_at', new_callable=PropertyMock, return_value=dt), \
         patch.object(Run, 'tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Run, 'user_tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Run, 'origin_pathspec', new_callable=PropertyMock, return_value=None):
        result = run.to_dict()
    assert isinstance(result["finished_at"], str)
    assert result["finished_at"] == dt.isoformat()


def test_run_to_dict_none_finished_at():
    from metaflow.client.core import Run
    run = Run.__new__(Run)
    run.id = "1"
    with patch.object(Run, 'pathspec', new_callable=PropertyMock, return_value="TestFlow/1"), \
         patch.object(Run, 'successful', new_callable=PropertyMock, return_value=False), \
         patch.object(Run, 'finished', new_callable=PropertyMock, return_value=False), \
         patch.object(Run, 'finished_at', new_callable=PropertyMock, return_value=None), \
         patch.object(Run, 'created_at', new_callable=PropertyMock, return_value=None), \
         patch.object(Run, 'tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Run, 'user_tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Run, 'origin_pathspec', new_callable=PropertyMock, return_value=None):
        result = run.to_dict()
    assert result["finished_at"] is None
    assert result["created_at"] is None


def test_task_to_dict_keys():
    from metaflow.client.core import Task
    task = Task.__new__(Task)
    task.id = "1"
    with patch.object(Task, 'pathspec', new_callable=PropertyMock, return_value="TestFlow/1/start/1"), \
         patch.object(Task, 'successful', new_callable=PropertyMock, return_value=True), \
         patch.object(Task, 'finished', new_callable=PropertyMock, return_value=True), \
         patch.object(Task, 'finished_at', new_callable=PropertyMock, return_value=None), \
         patch.object(Task, 'created_at', new_callable=PropertyMock, return_value=None), \
         patch.object(Task, 'tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Task, 'user_tags', new_callable=PropertyMock, return_value=frozenset()), \
         patch.object(Task, 'runtime_name', new_callable=PropertyMock, return_value="local"), \
         patch.object(Task, 'current_attempt', new_callable=PropertyMock, return_value=0), \
         patch.object(Task, 'origin_pathspec', new_callable=PropertyMock, return_value=None):
        result = task.to_dict()
    expected_keys = {"pathspec", "id", "successful", "finished", "finished_at", "created_at", "tags", "user_tags", "runtime_name", "current_attempt", "origin_pathspec"}
    assert expected_keys == set(result.keys())
