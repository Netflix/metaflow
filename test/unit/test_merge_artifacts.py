from types import SimpleNamespace

import pytest

from metaflow import FlowSpec, step
from metaflow.exception import UnhandledInMergeArtifactsException


class MergeFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class FakeDatastore:
    def __init__(self, artifacts=None):
        self.artifacts = artifacts or {}
        self.passed_down = []

    def items(self):
        return [(name, sha) for name, (sha, _) in self.artifacts.items()]

    def __contains__(self, name):
        return name in self.artifacts

    def __getitem__(self, name):
        value = self.artifacts[name][1]
        if isinstance(value, Exception):
            raise value
        return value

    def passdown_partial(self, origin, variables):
        self.passed_down.extend(variables)


class ArrayLike:
    """Like numpy arrays, == does not return a plain bool."""

    def __eq__(self, other):
        return [True, True]


@pytest.fixture
def merge():
    def _merge(*artifacts):
        flow = MergeFlow(use_cli=False)
        flow._current_step = "join"
        flow._graph = {"join": SimpleNamespace(type="join")}
        flow._datastore = FakeDatastore()
        inputs = [
            SimpleNamespace(_datastore=FakeDatastore({"x": (sha, value)}))
            for sha, value in artifacts
        ]
        flow.merge_artifacts(inputs)
        return flow._datastore.passed_down

    return _merge


def test_equal_values_with_different_hashes_are_merged(merge):
    # Re-pickling a set of strings in another process can change its bytes, and
    # so its hash, without changing its value.
    assert merge(("sha1", {"a", "b"}), ("sha2", {"b", "a"})) == ["x"]


@pytest.mark.parametrize(
    "first, second",
    [
        ({"a"}, {"b"}),
        (1, 1.0),
        (ArrayLike(), ArrayLike()),
        ({"a"}, ValueError("could not load")),
    ],
    ids=["different-values", "different-types", "non-bool-eq", "load-failure"],
)
def test_real_or_undecidable_conflicts_still_raise(merge, first, second):
    with pytest.raises(UnhandledInMergeArtifactsException):
        merge(("sha1", first), ("sha2", second))
