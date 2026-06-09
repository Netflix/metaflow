import pytest

from metaflow.plugins.metadata_providers.local import LocalMetadataProvider


@pytest.mark.parametrize(
    "meta_path, sub_type, expected_run_id",
    [
        (
            ".metaflow/BasicParameterTestFlow/1652384326805262/start/1/_meta",
            "task",
            "1652384326805262",
        ),
        (
            ".metaflow/BasicParameterTestFlow/1652384326805262/start/_meta",
            "step",
            "1652384326805262",
        ),
        (
            ".metaflow/BasicParameterTestFlow/1652384326805262/_meta",
            "run",
            "1652384326805262",
        ),
        (
            ".metaflow/BasicParameterTestFlow/_meta",
            "flow",
            None,
        ),
    ],
    ids=["task_level", "step_level", "run_level", "flow_level"],
)
def test_deduce_run_id_from_meta_dir(meta_path, sub_type, expected_run_id):
    actual_run_id = LocalMetadataProvider._deduce_run_id_from_meta_dir(
        meta_path, sub_type
    )
    assert actual_run_id == expected_run_id
