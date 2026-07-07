"""Smoke tests guarding the public surface of metaflow.datastore.artifacts."""


def test_register_serializer_for_type_not_public():
    """Imperative per-type registration is not a public API."""
    import metaflow.datastore.artifacts as mda

    assert not hasattr(mda, "register_serializer_for_type")


def test_serializer_config_not_public():
    """SerializerConfig is not a public export."""
    import metaflow.datastore.artifacts as mda

    assert not hasattr(mda, "SerializerConfig")


def test_register_serializer_config_not_public():
    import metaflow.datastore.artifacts as mda

    assert not hasattr(mda, "register_serializer_config")


def test_iter_registered_configs_not_public():
    import metaflow.datastore.artifacts as mda

    assert not hasattr(mda, "iter_registered_configs")


def test_load_serializer_class_not_public():
    import metaflow.datastore.artifacts as mda

    assert not hasattr(mda, "load_serializer_class")


def test_plugins_has_no_artifact_serializers_global():
    """metaflow.plugins does not expose a resolved ARTIFACT_SERIALIZERS global.
    Dispatch reads directly from SerializerStore.get_ordered_serializers()."""
    import metaflow.plugins as mplugins

    assert not hasattr(
        mplugins, "ARTIFACT_SERIALIZERS"
    ), "Expected ARTIFACT_SERIALIZERS to be absent; still present"


def test_pickle_serializer_is_active_after_import():
    """After import metaflow, PickleSerializer should be in ACTIVE state."""
    from metaflow.datastore.artifacts import list_serializer_status

    status = list_serializer_status()
    pickle_rec = next((r for r in status if r.get("type") == "pickle"), None)
    assert pickle_rec is not None, "PickleSerializer record missing; status=%r" % status
    assert pickle_rec["state"] == "active", (
        "Expected pickle active; got %r" % pickle_rec
    )
