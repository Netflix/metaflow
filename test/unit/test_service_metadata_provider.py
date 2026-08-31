from urllib.parse import parse_qs, urlparse

from metaflow.plugins.metadata_providers.service import ServiceMetadataProvider


def test_filter_tasks_by_metadata_anchors_patterns(monkeypatch):
    captured = {}

    def fake_request(cls, callback, url, method, *args, **kwargs):
        captured["url"] = url
        return [], None

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    def forwarded_pattern(pattern):
        ServiceMetadataProvider.filter_tasks_by_metadata(
            "Flow", "run", "middle", "foreach-execution-path", pattern
        )
        query = parse_qs(urlparse(captured["url"]).query)
        return query.get("pattern", [None])[0]

    assert forwarded_pattern("middle:1") == "^(?:middle:1)$"
    assert forwarded_pattern("middle:1,.*") == "^(?:middle:1,.*)$"
    assert forwarded_pattern(".*") is None
