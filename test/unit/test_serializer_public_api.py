"""Smoke tests guarding the public surface of metaflow.datastore.artifacts."""

import pytest

import metaflow.datastore.artifacts as mda
import metaflow.plugins as mplugins
from metaflow.datastore.artifacts import list_serializer_status


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "internal_attribute",
    [
        "register_serializer_for_type",
        "SerializerConfig",
        "register_serializer_config",
        "iter_registered_configs",
        "load_serializer_class",
    ],
    ids=[
        "register_serializer_for_type",
        "serializer_config",
        "register_serializer_config",
        "iter_registered_configs",
        "load_serializer_class",
    ],
)
def test_datastore_artifacts_hides_internal_api(internal_attribute):
    """Ensure internal serialization methods and configs are not exposed publicly."""
    assert not hasattr(mda, internal_attribute)


def test_plugins_hides_artifact_serializers_global():
    """
    metaflow.plugins does not expose a resolved ARTIFACT_SERIALIZERS global.
    Dispatch reads directly from SerializerStore.get_ordered_serializers().
    """
    assert not hasattr(
        mplugins, "ARTIFACT_SERIALIZERS"
    ), "Expected ARTIFACT_SERIALIZERS to be absent; still present"


def test_pickle_serializer_defaults_to_active_state():
    """After importing metaflow, PickleSerializer should be in the ACTIVE state."""
    status = list_serializer_status()
    pickle_rec = next((r for r in status if r.get("type") == "pickle"), None)

    assert pickle_rec is not None, f"PickleSerializer record missing; status={status!r}"
    assert (
        pickle_rec["state"] == "active"
    ), f"Expected pickle active; got {pickle_rec!r}"
