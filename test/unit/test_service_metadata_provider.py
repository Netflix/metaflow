from metaflow.metadata_provider.metadata import ObjectOrder
from metaflow.plugins.metadata_providers.service import ServiceMetadataProvider


class FakeServiceMetadataProvider(ServiceMetadataProvider):
    @classmethod
    def _request(cls, monitor, path, method, data=None, headers=None):
        if path.endswith("/runs/1"):
            return {"flow_id": "Flow", "run_number": "1", "keep": True}, {}
        return (
            [
                {"flow_id": "Flow", "run_number": "1", "keep": True},
                {"flow_id": "Flow", "run_number": "2", "keep": False},
            ],
            {},
        )

    @staticmethod
    def _apply_filter(elts, filters):
        return [elt for elt in elts if elt.get("keep")]


def test_service_metadata_provider_uses_subclass_apply_filter():
    self_result = FakeServiceMetadataProvider._get_object_internal(
        "run",
        ObjectOrder.type_to_order("run"),
        "self",
        ObjectOrder.type_to_order("self"),
        None,
        None,
        "Flow",
        "1",
    )
    collection_result = FakeServiceMetadataProvider._get_object_internal(
        "flow",
        ObjectOrder.type_to_order("flow"),
        "run",
        ObjectOrder.type_to_order("run"),
        None,
        None,
        "Flow",
    )

    assert self_result == {"flow_id": "Flow", "run_number": "1", "keep": True}
    assert collection_result == [{"flow_id": "Flow", "run_number": "1", "keep": True}]
