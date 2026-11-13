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


def test_filter_tasks_by_metadata_does_not_match_prefixes(monkeypatch):
    # A foreach with 11+ items produces execution paths like "middle:1" and
    # "middle:10". Matching with re.match only anchors the start, so the
    # pattern "middle:1" also selected "middle:10" and "middle:11", giving
    # Task.parent_tasks/child_tasks the wrong tasks.
    paths = {
        "1": "middle:1",
        "2": "middle:10",
        "3": "middle:11",
        "4": "middle:1,inner:0",
    }

    def fake_get_object(cls, obj_type, sub_type, filters, attempt, *args):
        if sub_type == "task":
            return [{"task_id": task_id} for task_id in sorted(paths)]
        task_id = args[-1]
        return [
            {"field_name": "foreach-execution-path", "value": paths[task_id]},
        ]

    monkeypatch.setattr(
        LocalMetadataProvider, "get_object", classmethod(fake_get_object)
    )

    def filter_for(pattern):
        return LocalMetadataProvider.filter_tasks_by_metadata(
            "Flow", "run", "middle", "foreach-execution-path", pattern
        )

    # the exact path must not pull in the longer indices that start with it
    assert filter_for("middle:1") == ["Flow/run/middle/1"]
    assert filter_for("middle:10") == ["Flow/run/middle/2"]
    # a nested foreach still resolves its children through the ",.*" pattern
    assert filter_for("middle:1,.*") == ["Flow/run/middle/4"]
    # and the match-all pattern keeps returning everything
    assert len(filter_for(".*")) == len(paths)
