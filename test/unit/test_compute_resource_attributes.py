from collections import namedtuple

import pytest

from metaflow.plugins.aws.aws_utils import compute_resource_attributes

MockDeco = namedtuple("MockDeco", ["name", "attributes"])


@pytest.mark.parametrize(
    "decorators, primary_deco, defaults, expected",
    [
        # --- Numeric attribute resolution ---
        # use default if nothing is set
        ([], MockDeco("batch", {}), {"cpu": "1"}, {"cpu": "1"}),
        # @batch overrides default and you can use ints as attributes
        ([], MockDeco("batch", {"cpu": 1}), {"cpu": "2"}, {"cpu": "1"}),
        # Same but value set as str not int
        ([], MockDeco("batch", {"cpu": "1"}), {"cpu": "2"}, {"cpu": "1"}),
        # same but use default memory
        (
            [],
            MockDeco("batch", {"cpu": "1"}),
            {"cpu": "2", "memory": "100"},
            {"cpu": "1", "memory": "100"},
        ),
        # same but cpu set via @resources
        (
            [],
            MockDeco("resources", {"cpu": "1"}),
            {"cpu": "2", "memory": "100"},
            {"cpu": "1", "memory": "100"},
        ),
        # --- Max/Largest resource resolution across decorators ---
        # take largest of @resources and @batch if both are present
        (
            [MockDeco("resources", {"cpu": "2"})],
            MockDeco("batch", {"cpu": 1}),
            {"cpu": "3"},
            {"cpu": "2.0"},
        ),
        # take largest of @resources and @batch if both are present (floats)
        (
            [MockDeco("resources", {"cpu": 0.83})],
            MockDeco("batch", {"cpu": "0.5"}),
            {"cpu": "1"},
            {"cpu": "0.83"},
        ),
        # --- String attribute resolution ---
        # if default is None and the value is not set in @batch, it is omitted
        (
            [],
            MockDeco("batch", {}),
            {"cpu": "1", "instance_type": None},
            {"cpu": "1"},
        ),
        # use string value from deco if set (default is None)
        (
            [],
            MockDeco("batch", {"instance_type": "p3.xlarge"}),
            {"cpu": "1", "instance_type": None},
            {"cpu": "1", "instance_type": "p3.xlarge"},
        ),
        # use string value from deco if set (default is not None)
        (
            [],
            MockDeco("batch", {"instance_type": "p3.xlarge"}),
            {"cpu": "1", "instance_type": "p4.xlarge"},
            {"cpu": "1", "instance_type": "p3.xlarge"},
        ),
        # use string value from defaults if @batch has it set to None
        (
            [],
            MockDeco("batch", {"instance_type": None}),
            {"cpu": "1", "instance_type": "p4.xlarge"},
            {"cpu": "1", "instance_type": "p4.xlarge"},
        ),
    ],
    ids=[
        "fallback-to-default",
        "batch-int-overrides-default",
        "batch-str-overrides-default",
        "merge-batch-with-default-memory",
        "merge-resources-with-default-memory",
        "resolve-max-between-resources-and-batch-int",
        "resolve-max-between-resources-and-batch-float",
        "omit-none-default-if-missing-in-batch",
        "batch-str-overrides-none-default",
        "batch-str-overrides-str-default",
        "none-in-batch-falls-back-to-default-str",
    ],
)
def test_compute_resource_attributes(decorators, primary_deco, defaults, expected):
    """Verify that resource attributes are correctly merged and resolved from decorators and defaults."""
    assert compute_resource_attributes(decorators, primary_deco, defaults) == expected
