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
    assert compute_resource_attributes(
        [MockDeco("resources", {"cpu": "2"})],
        MockDeco("batch", {"cpu": 1}),
        {"cpu": "3"},
    ) == {"cpu": "2.0"}

    # take largest of @resources and @batch if both are present
    assert compute_resource_attributes(
        [MockDeco("resources", {"cpu": 0.83})],
        MockDeco("batch", {"cpu": "0.5"}),
        {"cpu": "1"},
    ) == {"cpu": "0.83"}


def test_compute_resource_attributes_string():
    """Test string-valued resource attributes"""

    # if default is None and the value is not set in @batch, the value is not included in computed attributes in the end
    assert compute_resource_attributes(
        [], MockDeco("batch", {}), {"cpu": "1", "instance_type": None}
    ) == {"cpu": "1"}

    # use string value from deco if set (default is None)
    assert compute_resource_attributes(
        [],
        MockDeco("batch", {"instance_type": "p3.xlarge"}),
        {"cpu": "1", "instance_type": None},
    ) == {"cpu": "1", "instance_type": "p3.xlarge"}

    # use string value from deco if set (default is not None)
    assert compute_resource_attributes(
        [],
        MockDeco("batch", {"instance_type": "p3.xlarge"}),
        {"cpu": "1", "instance_type": "p4.xlarge"},
    ) == {"cpu": "1", "instance_type": "p3.xlarge"}

    # use string value from defaults if @batch has it set to None
    assert compute_resource_attributes(
        [],
        MockDeco("batch", {"instance_type": None}),
        {"cpu": "1", "instance_type": "p4.xlarge"},
    ) == {"cpu": "1", "instance_type": "p4.xlarge"}
