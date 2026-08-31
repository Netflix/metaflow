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
