from urllib.parse import parse_qs, urlparse

from metaflow.plugins.metadata_providers.service import ServiceMetadataProvider


def _forwarded_pattern(monkeypatch, pattern):
    # The service provider builds the request URL and lets the metadata service
    # do the matching, so we capture what pattern it forwards.
    captured = {}

    def fake_request(cls, callback, url, method, *args, **kwargs):
        captured["url"] = url
        return [], None

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))
    ServiceMetadataProvider.filter_tasks_by_metadata(
        "Flow", "run", "middle", "foreach-execution-path", pattern
    )
    query = parse_qs(urlparse(captured["url"]).query)
    return query.get("pattern", [None])[0]


def test_filter_tasks_by_metadata_anchors_exact_path(monkeypatch):
    # An exact foreach path must be anchored so "middle:1" cannot prefix-match
    # "middle:10"/"middle:11" on the service backend (issue #3341), matching the
    # local provider's fullmatch behavior.
    assert _forwarded_pattern(monkeypatch, "middle:1") == "^(?:middle:1)$"


def test_filter_tasks_by_metadata_anchors_descendant_path(monkeypatch):
    # The "{path},.*" child form is still anchored; the trailing .* keeps
    # matching a task's descendants.
    assert _forwarded_pattern(monkeypatch, "middle:1,.*") == "^(?:middle:1,.*)$"


def test_filter_tasks_by_metadata_match_all_sends_no_pattern(monkeypatch):
    # ".*" is short-circuited to "match every task", so no pattern is forwarded.
    assert _forwarded_pattern(monkeypatch, ".*") is None
