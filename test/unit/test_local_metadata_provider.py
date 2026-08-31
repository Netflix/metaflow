from metaflow.plugins.metadata_providers.local import LocalMetadataProvider


def test_deduce_run_id_from_meta_dir():
    test_cases = [
        {
            "meta_path": ".metaflow/BasicParameterTestFlow/1652384326805262/start/1/_meta",
            "sub_type": "task",
            "expected_run_id": "1652384326805262",
        },
        {
            "meta_path": ".metaflow/BasicParameterTestFlow/1652384326805262/start/_meta",
            "sub_type": "step",
            "expected_run_id": "1652384326805262",
        },
        {
            "meta_path": ".metaflow/BasicParameterTestFlow/1652384326805262/_meta",
            "sub_type": "run",
            "expected_run_id": "1652384326805262",
        },
        {
            "meta_path": ".metaflow/BasicParameterTestFlow/_meta",
            "sub_type": "flow",
            "expected_run_id": None,
        },
    ]
    for case in test_cases:
        actual_run_id = LocalMetadataProvider._deduce_run_id_from_meta_dir(
            case["meta_path"], case["sub_type"]
        )
        assert case["expected_run_id"] == actual_run_id


def test_filter_tasks_by_metadata_matches_exact_foreach_path(monkeypatch):
    # A foreach with 10+ splits produces execution paths like "middle:1" and
    # "middle:10". regex.match only anchors the start, so querying "middle:1"
    # also pulled in "middle:10"/"middle:11", giving Task.parent_tasks and
    # child_tasks the wrong tasks (issue #3341).
    paths = {
        "t1": "middle:1",
        "t2": "middle:10",
        "t3": "middle:11",
        "t4": "middle:1,inner:0",
    }

    def fake_get_object(cls, obj_type, sub_type, filters, attempt, *args):
        if sub_type == "task":
            return [{"task_id": task_id} for task_id in paths]
        task_id = args[-1]
        return [{"field_name": "foreach-execution-path", "value": paths[task_id]}]

    monkeypatch.setattr(
        LocalMetadataProvider, "get_object", classmethod(fake_get_object)
    )

    def matches(pattern):
        return LocalMetadataProvider.filter_tasks_by_metadata(
            "Flow", "run", "middle", "foreach-execution-path", pattern
        )

    # an exact path must not pull in the longer indices that start with it
    assert matches("middle:1") == ["Flow/run/middle/t1"]
    assert matches("middle:10") == ["Flow/run/middle/t2"]
    # a nested foreach still resolves its children through the "{path},.*" form
    assert matches("middle:1,.*") == ["Flow/run/middle/t4"]
    # the match-all pattern keeps returning every task
    assert sorted(matches(".*")) == sorted(
        f"Flow/run/middle/{task_id}" for task_id in paths
    )
