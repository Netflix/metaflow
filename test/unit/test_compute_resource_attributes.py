from collections import namedtuple
from metaflow.plugins.aws.aws_utils import compute_resource_attributes


MockDeco = namedtuple("MockDeco", ["name", "attributes"])


def test_compute_resource_attributes():

    # use default if nothing is set
    assert compute_resource_attributes([], MockDeco("batch", {}), {"cpu": "1"}) == {
        "cpu": "1"
    }

    # @batch overrides default and you can use ints as attributes
    assert compute_resource_attributes(
        [], MockDeco("batch", {"cpu": 1}), {"cpu": "2"}
    ) == {"cpu": "1"}

    # Same but value set as str not int
    assert compute_resource_attributes(
        [], MockDeco("batch", {"cpu": "1"}), {"cpu": "2"}
    ) == {"cpu": "1"}

    # same but use default memory
    assert compute_resource_attributes(
        [], MockDeco("batch", {"cpu": "1"}), {"cpu": "2", "memory": "100"}
    ) == {"cpu": "1", "memory": "100"}

    # same but cpu set via @resources
    assert compute_resource_attributes(
        [], MockDeco("resources", {"cpu": "1"}), {"cpu": "2", "memory": "100"}
    ) == {"cpu": "1", "memory": "100"}

    # take largest of @resources and @batch if both are present
    assert (
        compute_resource_attributes(
            [MockDeco("resources", {"cpu": "2"})],
            MockDeco("batch", {"cpu": 1}),
            {"cpu": "3"},
        )
        == {"cpu": "2"}
    )
