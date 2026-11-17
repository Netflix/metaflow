from urllib.parse import parse_qs, urlsplit

import pytest

from metaflow.client import core as client_core
from metaflow.client.core import Flow, Run, Task, get_namespace, namespace
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metadata_provider.metadata import MetadataProvider
from metaflow.plugins.metadata_providers import service as service_module
from metaflow.plugins.metadata_providers.local import LocalMetadataProvider
from metaflow.plugins.metadata_providers.service import (
    ServiceException,
    ServiceMetadataProvider,
)

PAGINATING_SERVICE_VERSION = (
    ServiceMetadataProvider._MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION
)
LEGACY_SERVICE_VERSION = "2.4.0"


def _record(run_number, tags=None, system_tags=None):
    return {
        "flow_id": "ExampleFlow",
        "run_number": run_number,
        "ts_epoch": run_number * 1000,
        "tags": tags or [],
        "system_tags": system_tags or [],
    }


@pytest.fixture(autouse=True)
def reset_service_capability_cache():
    """Start every test from a cold service-version cache.

    The cache is keyed by service URL on purpose -- see
    test_service_switch_rechecks_pagination_capability, which switches
    ``_INFO`` mid-test and relies on that key rather than on this fixture.
    """
    ServiceMetadataProvider._service_version_cache.clear()
    yield
    ServiceMetadataProvider._service_version_cache.clear()


def _paginating_version(cls, monitor):
    return PAGINATING_SERVICE_VERSION


def _legacy_version(cls, monitor):
    return LEGACY_SERVICE_VERSION


def _use_service_version(monkeypatch, version):
    """Answer the capability ping with a fixed version, no HTTP."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(lambda cls, monitor: version)
    )


def test_service_run_iterator_follows_cursor_and_preserves_filters(monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    responses = [
        (
            [
                _record(3, system_tags=["user:aryan"]),
                _record(2, system_tags=["user:aryan"]),
            ],
            {"X-Next-Cursor": "next-page", "X-Limit": "2"},
        ),
        ([_record(1, system_tags=["user:aryan"])], {"X-Limit": "2"}),
    ]
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append((path, method, kwargs))
        return responses[len(calls) - 1]

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    records = list(
        ServiceMetadataProvider.iter_objects(
            "flow",
            "run",
            {"any_tags": "user:aryan"},
            None,
            "ExampleFlow",
            query_filters={"status:eq": "failed", "_tags:all": "prod"},
            page_size=2,
        )
    )

    assert [record["run_number"] for record in records] == [3, 2, 1]
    first_query = parse_qs(urlsplit(calls[0][0]).query)
    second_query = parse_qs(urlsplit(calls[1][0]).query)
    assert first_query == {
        "_limit": ["2"],
        "_tags:all": ["prod,user:aryan"],
        "status:eq": ["failed"],
    }
    assert second_query["_cursor"] == ["next-page"]
    assert second_query["status:eq"] == ["failed"]
    assert all(method == "GET" for _, method, _ in calls)
    assert all(options == {"return_headers": True} for _, _, options in calls)


@pytest.mark.parametrize(
    "obj_type, sub_type, args, expected_path",
    [
        ("root", "flow", (), "/flows"),
        ("flow", "run", ("ExampleFlow",), "/flows/ExampleFlow/runs"),
        ("run", "step", ("ExampleFlow", "12"), "/flows/ExampleFlow/runs/12/steps"),
        (
            "step",
            "task",
            ("ExampleFlow", "12", "start"),
            "/flows/ExampleFlow/runs/12/steps/start/tasks",
        ),
        (
            "task",
            "metadata",
            ("ExampleFlow", "12", "start", "1"),
            "/flows/ExampleFlow/runs/12/steps/start/tasks/1/metadata",
        ),
        (
            "task",
            "artifact",
            ("ExampleFlow", "12", "start", "1"),
            "/flows/ExampleFlow/runs/12/steps/start/tasks/1/artifacts",
        ),
    ],
    ids=["flows", "runs", "steps", "tasks", "metadata", "artifacts"],
)
def test_service_iterator_paginates_every_paginable_collection(
    obj_type, sub_type, args, expected_path, monkeypatch
):
    """Every collection, artifacts included, goes through the paginated path."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        return [_record(1)], {"X-Limit": "1"}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    list(
        ServiceMetadataProvider.iter_objects(
            obj_type, sub_type, None, None, *args, page_size=1
        )
    )

    assert len(calls) == 1
    assert calls[0].startswith("%s?" % expected_path)
    assert parse_qs(urlsplit(calls[0]).query) == {"_limit": ["1"]}


def test_task_artifacts_paginate_on_service_with_latest_attempt_fix(monkeypatch):
    """Task artifacts take the paginated path once the service can be trusted.

    metaflow-service 2.6.0 paginated artifacts as the newest row per artifact
    *name*, which could surface an artifact that the task's latest attempt never
    wrote. 2.6.1 (metaflow-service#497) reduces to the latest attempt per task,
    the same way the legacy listing does, so from that version on there is no
    reason to keep artifacts on the bulk GET.
    """
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append((path, kwargs))
        return [{"name": "model.pkl", "ts_epoch": 1}], {"X-Limit": "1"}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    records = list(
        ServiceMetadataProvider.iter_objects(
            "task", "artifact", None, None, "ExampleFlow", "12", "start", "1"
        )
    )

    assert [record["name"] for record in records] == ["model.pkl"]
    assert len(calls) == 1
    path, options = calls[0]
    assert path.startswith("/flows/ExampleFlow/runs/12/steps/start/tasks/1/artifacts?")
    assert "_limit" in parse_qs(urlsplit(path).query)
    assert options == {"return_headers": True}


def test_service_without_artifact_fix_keeps_legacy_listing(monkeypatch):
    """A 2.6.0 service is below the gate, so nothing is paginated against it.

    The client cannot tell a 2.6.0 that paginates artifacts wrongly from one
    that does not, so the whole paginated path waits for 2.6.1 rather than
    paginating runs while special-casing artifacts.
    """
    _use_service_version(monkeypatch, "2.6.0")
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append((path, kwargs))
        return [{"name": "model.pkl", "ts_epoch": 1}], None

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    assert ServiceMetadataProvider._service_supports_cursor_pagination() is False

    list(
        ServiceMetadataProvider.iter_objects(
            "task", "artifact", None, None, "ExampleFlow", "12", "start", "1"
        )
    )

    assert len(calls) == 1
    path, options = calls[0]
    assert path == "/flows/ExampleFlow/runs/12/steps/start/tasks/1/artifacts"
    assert options == {}


def test_attempt_scoped_artifacts_never_paginate(monkeypatch):
    """The per-attempt artifact endpoint has no cursor pagination; keep the bulk GET."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append((path, kwargs))
        return [{"name": "model.pkl", "ts_epoch": 1}], None

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    list(
        ServiceMetadataProvider.iter_objects(
            "task", "artifact", None, 0, "ExampleFlow", "12", "start", "1"
        )
    )

    assert len(calls) == 1
    path, options = calls[0]
    assert path == "/flows/ExampleFlow/runs/12/steps/start/tasks/1/attempt/0/artifacts"
    assert options == {}


def test_repeated_cursor_raises_instead_of_truncating(monkeypatch):
    """A repeated cursor is a broken exchange, not the end of the listing.

    Returning quietly would hand the caller a truncated listing that looks
    complete, so the client refuses instead.
    """
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        return [_record(len(calls))], {"X-Next-Cursor": "same-cursor", "X-Limit": "1"}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    yielded = []
    with pytest.raises(ServiceException, match="repeated pagination cursor"):
        for record in ServiceMetadataProvider.iter_objects(
            "flow", "run", None, None, "ExampleFlow", page_size=1
        ):
            yielded.append(record)

    assert len(yielded) == 2
    assert len(calls) == 2


def test_missing_capability_header_on_second_page_raises(monkeypatch):
    """The capability header is checked on every page, not only the first.

    During a rolling deployment page 2 can land on an older instance that
    ignores the cursor and filters and answers with an unpaginated bulk
    listing. Yielding that would duplicate and unfilter the stream.
    """
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            return [_record(2)], {"X-Next-Cursor": "page-2", "X-Limit": "1"}
        return [_record(9), _record(8)], {}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    yielded = []
    with pytest.raises(ServiceException, match="stopped honoring cursor pagination"):
        for record in ServiceMetadataProvider.iter_objects(
            "flow", "run", None, None, "ExampleFlow", page_size=1
        ):
            yielded.append(record)

    assert [record["run_number"] for record in yielded] == [2]
    assert len(calls) == 2


def test_service_switch_rechecks_pagination_capability(monkeypatch):
    """Capability is a property of the selected service, not of the process.

    metadata("service@...") can repoint a running process at another service.
    A single cached boolean made a 2.6 service inherit a 2.5 verdict (and vice
    versa), disabling supported filtering or paginating an older endpoint.
    """
    versions = {
        "http://old-service": "2.5.0",
        "http://new-service": PAGINATING_SERVICE_VERSION,
    }
    monkeypatch.setattr(
        ServiceMetadataProvider,
        "_version",
        classmethod(lambda cls, monitor: versions[cls.INFO]),
    )

    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://old-service")
    assert ServiceMetadataProvider._service_supports_cursor_pagination() is False

    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://new-service")
    assert ServiceMetadataProvider._service_supports_cursor_pagination() is True

    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://old-service")
    assert ServiceMetadataProvider._service_supports_cursor_pagination() is False


def test_capability_checks_share_one_version_ping_per_service(monkeypatch):
    """All capability gates read one cached version per service URL."""
    pings = []

    def counting_version(cls, monitor):
        pings.append(cls.INFO)
        return PAGINATING_SERVICE_VERSION

    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(counting_version)
    )
    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://service-a")

    assert ServiceMetadataProvider._service_supports("2.0.6") is True
    assert ServiceMetadataProvider._service_supports("2.3.0") is True
    assert ServiceMetadataProvider._service_supports_cursor_pagination() is True
    assert pings == ["http://service-a"]

    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://service-b")
    assert ServiceMetadataProvider._service_supports("2.0.6") is True
    assert pings == ["http://service-a", "http://service-b"]


def test_each_capability_keeps_its_own_minimum_version_message(monkeypatch):
    """Sharing the cache must not merge the three user-facing errors."""
    _use_service_version(monkeypatch, "2.0.0")

    def unexpected_request(*args, **kwargs):
        raise AssertionError("capability gate must fail before any request")

    monkeypatch.setattr(
        ServiceMetadataProvider, "_request", classmethod(unexpected_request)
    )

    with pytest.raises(ServiceException, match="2.3.0"):
        ServiceMetadataProvider._mutate_user_tags_for_run(
            "ExampleFlow", "12", tags_to_add=["prod"]
        )

    with pytest.raises(ServiceException, match="2.0.6"):
        ServiceMetadataProvider._get_object_internal(
            "task", 4, "self", 7, None, 1, "ExampleFlow", "12", "start", "1"
        )


def test_old_service_uses_legacy_listing_without_query_params(monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_legacy_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        return [_record(2), _record(1)], {}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    records = list(
        ServiceMetadataProvider.iter_objects("flow", "run", None, None, "ExampleFlow")
    )

    assert [record["run_number"] for record in records] == [2, 1]
    assert calls == ["/flows/ExampleFlow/runs"]
    assert "?" not in calls[0]


def test_old_service_rejects_server_filters_without_listing(monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_legacy_version)
    )

    def unexpected_request(*args, **kwargs):
        raise AssertionError(
            "legacy services must not receive filtered listing requests"
        )

    monkeypatch.setattr(
        ServiceMetadataProvider, "_request", classmethod(unexpected_request)
    )

    with pytest.raises(ServiceException, match="Filtering requires"):
        list(
            ServiceMetadataProvider.iter_objects(
                "flow",
                "run",
                None,
                None,
                "ExampleFlow",
                query_filters={"status:eq": "failed"},
            )
        )


def test_new_service_without_limit_header_does_not_yield_unfiltered_records(
    monkeypatch,
):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    yielded = []

    def fake_request(cls, monitor, path, method, **kwargs):
        return [_record(1)], {}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    with pytest.raises(ServiceException, match="Filtering requires"):
        for record in ServiceMetadataProvider.iter_objects(
            "flow",
            "run",
            None,
            None,
            "ExampleFlow",
            query_filters={"status:eq": "failed"},
        ):
            yielded.append(record)

    assert yielded == []


def test_new_service_without_limit_header_falls_back_to_legacy_listing(monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append((path, kwargs))
        if "return_headers" in kwargs:
            return [_record(9)], {}
        return [_record(2), _record(1)], True

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    records = list(
        ServiceMetadataProvider.iter_objects("flow", "run", None, None, "ExampleFlow")
    )

    assert [record["run_number"] for record in records] == [2, 1]
    assert calls[0][1] == {"return_headers": True}
    assert "?" in calls[0][0]
    assert calls[1][0] == "/flows/ExampleFlow/runs"


@pytest.mark.parametrize("page_size", [0, -1, True, "10"])
def test_service_run_iterator_rejects_invalid_page_size(page_size, monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    with pytest.raises((TypeError, ValueError), match="page_size"):
        list(
            ServiceMetadataProvider.iter_objects(
                "flow", "run", None, None, "ExampleFlow", page_size=page_size
            )
        )


def test_page_size_is_clamped_to_the_service_maximum(monkeypatch):
    """The service refuses a larger _limit, so never ask for one."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        return [_record(1)], {"X-Limit": "500"}

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    list(
        ServiceMetadataProvider.iter_objects(
            "flow", "run", None, None, "ExampleFlow", page_size=10000
        )
    )

    assert parse_qs(urlsplit(calls[0]).query)["_limit"] == [
        str(ServiceMetadataProvider._MAX_SERVICE_PAGE_SIZE)
    ]


@pytest.mark.parametrize("reserved", ["_limit", "_cursor"])
def test_service_run_iterator_owns_pagination_parameters(reserved, monkeypatch):
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    with pytest.raises(ValueError, match=reserved):
        list(
            ServiceMetadataProvider.iter_objects(
                "flow",
                "run",
                None,
                None,
                "ExampleFlow",
                query_filters={reserved: "value"},
            )
        )


def test_default_provider_iterator_sorts_newest_first_and_rejects_server_filters(
    monkeypatch,
):
    monkeypatch.setattr(
        LocalMetadataProvider,
        "get_object",
        classmethod(lambda cls, *args, **kwargs: [_record(1), _record(2)]),
    )

    assert [
        record["run_number"]
        for record in LocalMetadataProvider.iter_objects(
            "flow", "run", None, None, "Flow"
        )
    ] == [2, 1]
    with pytest.raises(MetaflowException, match="not supported"):
        list(
            LocalMetadataProvider.iter_objects(
                "flow",
                "run",
                None,
                None,
                "Flow",
                query_filters={"status:eq": "failed"},
            )
        )


def test_service_request_can_return_success_headers(mocker, monkeypatch):
    response = mocker.Mock()
    response.status_code = 200
    response.headers = {"X-Next-Cursor": "cursor"}
    response.json.return_value = [_record(1)]

    session = mocker.Mock()
    session.get.return_value = response

    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://metadata")
    monkeypatch.setattr(ServiceMetadataProvider, "_session", session)

    body, headers = ServiceMetadataProvider._request(
        None, "/flows/ExampleFlow/runs", "GET", return_headers=True
    )

    assert body == [_record(1)]
    assert headers["X-Next-Cursor"] == "cursor"


@pytest.fixture
def local_flow(monkeypatch, tmp_path):
    """A real Flow backed by the local metadata provider.

    Records are handed back oldest-first on purpose, so the newest-first
    ordering the client promises has to come from the client.
    """
    (tmp_path / LocalMetadataProvider.DATASTORE_DIR).mkdir()
    # Selecting the provider writes to class state -- the provider's INFO and
    # the datastore root it computes. Record both so monkeypatch restores them
    # and the tmp_path never outlives the test as a global.
    storage_class = LocalMetadataProvider._get_storage_class()
    monkeypatch.setattr(LocalMetadataProvider, "_INFO", None, raising=False)
    monkeypatch.setattr(
        storage_class, "datastore_root", storage_class.datastore_root, raising=False
    )
    runs = [
        _record(1, tags=["prod"]),
        _record(2, tags=["dev"]),
        _record(3, tags=["prod"]),
    ]

    def fake_get_object(cls, obj_type, sub_type, filters, attempt, *args):
        if sub_type == "self":
            return {
                "flow_id": "ExampleFlow",
                "ts_epoch": 0,
                "tags": [],
                "system_tags": [],
            }
        if (obj_type, sub_type) == ("flow", "run"):
            # Like the real local provider, apply the namespace filter here.
            return MetadataProvider._apply_filter(list(runs), filters)
        return []

    monkeypatch.setattr(
        LocalMetadataProvider, "get_object", classmethod(fake_get_object)
    )

    previous_namespace = get_namespace()
    namespace(None)
    try:
        yield Flow("ExampleFlow", _current_metadata="local@%s" % tmp_path)
    finally:
        namespace(previous_namespace)


def test_flow_runs_local_tags_and_max_runs_end_to_end(local_flow):
    """Tag filtering and max_runs work on local metadata with nothing mocked in
    the client path: real Flow, real _iter_children, real ordering."""
    assert [run.id for run in local_flow.runs()] == ["3", "2", "1"]
    assert [run.id for run in local_flow.runs("prod")] == ["3", "1"]
    assert [run.id for run in local_flow.runs("prod", max_runs=1)] == ["3"]
    assert list(local_flow.runs("nonexistent-tag")) == []


def test_flow_runs_max_runs_returns_newest_from_oldest_first_provider(local_flow):
    """max_runs is a newest-first bound even when the provider lists oldest
    first, so the bound has to be applied after the client's ordering."""
    assert [run.id for run in local_flow.runs(max_runs=2)] == ["3", "2"]


def test_flow_runs_rejects_server_filters_on_local_metadata(local_flow):
    """The service filter grammar has no meaning for local metadata."""
    with pytest.raises(MetaflowException, match="not supported"):
        list(local_flow.runs(_filters={"status:eq": "failed"}))


def test_flow_runs_forwards_filters_and_bounds_results(mocker):
    captured = {}

    def fake_iter_children(query_filters=None, page_size=None, required_tags=()):
        captured.update(
            query_filters=query_filters,
            page_size=page_size,
            required_tags=required_tags,
        )
        yield from range(4)

    flow = mocker.Mock()
    flow._iter_children = fake_iter_children

    runs = list(
        Flow.runs.__get__(flow, Flow)(
            "prod",
            _filters={"status:eq": "failed"},
            max_runs=2,
        )
    )

    assert runs == [0, 1]
    assert captured == {
        "query_filters": {"status:eq": "failed"},
        # Page size is a service transport detail read from
        # METAFLOW_SERVICE_PAGE_SIZE, never a per-call argument.
        "page_size": None,
        "required_tags": ("prod",),
    }


@pytest.mark.parametrize(
    "kwargs, error, match",
    [
        ({"_filters": ["status:eq"]}, TypeError, "_filters must be a mapping"),
        # bool is a subclass of int: without the explicit guard max_runs=True
        # would silently mean "one run".
        ({"max_runs": True}, TypeError, "max_runs must be an integer"),
        ({"max_runs": "2"}, TypeError, "max_runs must be an integer"),
        ({"max_runs": -1}, ValueError, "max_runs must be non-negative"),
    ],
    ids=["filters-not-a-mapping", "max-runs-bool", "max-runs-str", "max-runs-negative"],
)
def test_flow_runs_rejects_invalid_arguments(kwargs, error, match, mocker):
    flow = mocker.Mock()
    flow._iter_children.side_effect = AssertionError("must fail before listing")

    with pytest.raises(error, match=match):
        Flow.runs.__get__(flow, Flow)(**kwargs)


def test_flow_runs_accepts_mapping_subclasses(mocker):
    """A Mapping that is not a plain dict is still a valid filter set."""

    class ReadOnlyFilters(dict):
        pass

    captured = {}

    def fake_iter_children(query_filters=None, page_size=None, required_tags=()):
        captured.update(query_filters=query_filters)
        yield from ()

    flow = mocker.Mock()
    flow._iter_children = fake_iter_children

    list(Flow.runs.__get__(flow, Flow)(_filters=ReadOnlyFilters({"status:eq": "ok"})))
    assert captured == {"query_filters": {"status:eq": "ok"}}


def test_flow_runs_zero_limit_avoids_starting_iterator(mocker):
    flow = mocker.Mock()
    flow._iter_children.side_effect = AssertionError("iterator should not be started")

    assert list(Flow.runs.__get__(flow, Flow)(max_runs=0)) == []


def test_get_object_internal_returns_none_not_empty_on_404(monkeypatch):
    """A missing (404) collection must return None like the legacy path, not [].

    The paginated iterator swallows a first-page 404, so without care
    list(...) would yield [] and mask "not found" as "empty".
    """
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )

    def fake_request(cls, monitor, path, method, **kwargs):
        raise ServiceException("collection not found", http_code=404)

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    result = ServiceMetadataProvider._get_object_internal(
        "flow", 1, "run", 2, None, None, "ExampleFlow"
    )
    assert result is None


def test_iter_objects_yields_empty_on_404(monkeypatch):
    """Streaming a missing collection yields nothing (no 404 leaking to callers)."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )

    def fake_request(cls, monitor, path, method, **kwargs):
        raise ServiceException("collection not found", http_code=404)

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    assert (
        list(
            ServiceMetadataProvider.iter_objects(
                "flow", "run", None, None, "ExampleFlow"
            )
        )
        == []
    )


def test_get_object_internal_mid_page_404_returns_none(monkeypatch):
    """get_object keeps legacy's atomic contract: a 404 at ANY point in the
    listing resolves to None, never a silently truncated partial list."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            return [_record(2)], {"X-Next-Cursor": "p2", "X-Limit": "1"}
        raise ServiceException("collection deleted mid-scan", http_code=404)

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    result = ServiceMetadataProvider._get_object_internal(
        "flow", 1, "run", 2, None, None, "ExampleFlow"
    )
    assert result is None
    assert len(calls) == 2


def test_iter_objects_mid_page_404_raises_after_first_page(monkeypatch):
    """A 404 after records were yielded is an incomplete listing, not "not
    found": the stream yields the first page, then raises a ServiceException
    that carries no 404 http_code so no caller mistakes it for a missing
    collection."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            return [_record(2)], {"X-Next-Cursor": "p2", "X-Limit": "1"}
        raise ServiceException("collection deleted mid-scan", http_code=404)

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    yielded = []
    with pytest.raises(ServiceException, match="listing is incomplete") as excinfo:
        for record in ServiceMetadataProvider.iter_objects(
            "flow", "run", None, None, "ExampleFlow"
        ):
            yielded.append(record)

    assert [record["run_number"] for record in yielded] == [2]
    assert excinfo.value.http_code is None
    assert isinstance(excinfo.value.__cause__, ServiceException)
    assert excinfo.value.__cause__.http_code == 404
    assert len(calls) == 2


def test_get_object_internal_returns_none_when_legacy_fallback_404s(monkeypatch):
    """The no-X-Limit fallback must not mask 'not found' as 'empty': if the
    legacy GET 404s, get_object returns None, not []."""
    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(_paginating_version)
    )
    calls = []

    def fake_request(cls, monitor, path, method, **kwargs):
        calls.append(kwargs)
        if "return_headers" in kwargs:
            # Paginated probe: 200 but no X-Limit -> triggers legacy fallback.
            return [_record(9)], {}
        raise ServiceException("collection not found", http_code=404)

    monkeypatch.setattr(ServiceMetadataProvider, "_request", classmethod(fake_request))

    result = ServiceMetadataProvider._get_object_internal(
        "flow", 1, "run", 2, None, None, "ExampleFlow"
    )
    assert result is None
    assert len(calls) == 2


def test_service_paginated_iterator_validates_before_any_http(monkeypatch):
    """The paginated path enforces the same obj/sub_type guards as get_object,
    and does so before it touches the network.

    Both _version() (the capability ping) and _request() blow up here, so this
    fails if validation ever moves back below the capability check.
    """

    def unexpected_call(*args, **kwargs):
        raise AssertionError("validation must fail before any HTTP call")

    monkeypatch.setattr(
        ServiceMetadataProvider, "_version", classmethod(unexpected_call)
    )
    monkeypatch.setattr(
        ServiceMetadataProvider, "_request", classmethod(unexpected_call)
    )

    # 'flow' is not slotted below 'run' -> nonsensical; must raise before any call.
    with pytest.raises(MetaflowInternalError, match="not allowed"):
        list(ServiceMetadataProvider.iter_objects("run", "flow", None, None, "F", "1"))


# --- MetaflowObject.__iter__ streams through iter_objects ---------------------

_FLOW_RECORD = {
    "flow_id": "ExampleFlow",
    "ts_epoch": 0,
    "tags": [],
    "system_tags": [],
}
_RUN_RECORD = dict(_record(12), tags=[], system_tags=[])
_TASK_RECORD = {
    "flow_id": "ExampleFlow",
    "run_number": 12,
    "step_name": "start",
    "task_id": "1",
    "ts_epoch": 0,
    "tags": [],
    "system_tags": [],
}


def _step_record(name, ts):
    return {
        "flow_id": "ExampleFlow",
        "run_number": 12,
        "step_name": name,
        "ts_epoch": ts,
        "tags": [],
        "system_tags": [],
    }


def _artifact_record(name, ts):
    return {
        "flow_id": "ExampleFlow",
        "run_number": 12,
        "step_name": "start",
        "task_id": "1",
        "name": name,
        "ts_epoch": ts,
        "tags": [],
        "system_tags": [],
    }


class _FakeService(object):
    """An in-memory metadata service.

    Paginated requests (return_headers=True) are served newest-first in pages
    of ``_limit`` with an offset cursor, like metaflow-service >= 2.6.1. Bulk
    (legacy) GETs return the whole collection oldest-first, so any newest-first
    order observed on that path must come from the client.
    """

    def __init__(self, collections):
        self.collections = collections
        self.calls = []

    def request(self, cls, monitor, path, method, **kwargs):
        base, _, query = path.partition("?")
        records = sorted(
            self.collections[base], key=lambda r: r["ts_epoch"], reverse=True
        )
        if not kwargs.get("return_headers"):
            self.calls.append((base, None))
            return list(reversed(records)), None
        params = parse_qs(query)
        limit = int(params["_limit"][0])
        offset = int(params.get("_cursor", ["0"])[0])
        self.calls.append((base, offset))
        headers = {"X-Limit": str(limit)}
        if offset + limit < len(records):
            headers["X-Next-Cursor"] = str(offset + limit)
        return records[offset : offset + limit], headers


@pytest.fixture
def fake_service(monkeypatch):
    """Point the client at ServiceMetadataProvider backed by _FakeService.

    Returns a callable ``(collections, version=PAGINATING_SERVICE_VERSION)``
    that installs the fake and returns it.
    """
    monkeypatch.setattr(ServiceMetadataProvider, "_INFO", "http://metadata")

    def install(collections, version=PAGINATING_SERVICE_VERSION):
        service = _FakeService(collections)
        _use_service_version(monkeypatch, version)
        monkeypatch.setattr(
            ServiceMetadataProvider, "_request", classmethod(service.request)
        )
        # Child objects resolve the provider through the module-level
        # current_metadata.
        monkeypatch.setattr(client_core, "current_metadata", ServiceMetadataProvider)
        return service

    return install


def _runs_collection():
    return {
        "/flows/ExampleFlow/runs": [
            _record(1, tags=["prod"]),
            _record(2, tags=["dev"]),
            _record(3, tags=["prod"]),
            _record(4, tags=["dev"]),
            _record(5, tags=["prod"]),
        ]
    }


def test_flow_iter_uses_iter_objects_not_get_object(fake_service, monkeypatch):
    """Plain iteration streams page by page instead of materializing the
    listing through get_object, so a newest-first peek fetches one page."""
    service = fake_service(_runs_collection())
    monkeypatch.setattr(service_module, "SERVICE_PAGE_SIZE", 2)

    internal_calls = []
    original = ServiceMetadataProvider._get_object_internal.__func__

    def spying_get_object_internal(cls, obj_type, obj_order, sub_type, *args):
        internal_calls.append((obj_type, sub_type))
        return original(cls, obj_type, obj_order, sub_type, *args)

    monkeypatch.setattr(
        ServiceMetadataProvider,
        "_get_object_internal",
        classmethod(spying_get_object_internal),
    )

    flow = Flow(_object=dict(_FLOW_RECORD), _namespace_check=False)

    assert next(iter(flow)).id == "5"
    assert service.calls == [("/flows/ExampleFlow/runs", 0)]

    del service.calls[:]
    assert flow.latest_run.id == "5"
    assert service.calls == [("/flows/ExampleFlow/runs", 0)]

    del service.calls[:]
    assert [run.id for run in flow] == ["5", "4", "3", "2", "1"]
    assert service.calls == [
        ("/flows/ExampleFlow/runs", 0),
        ("/flows/ExampleFlow/runs", 2),
        ("/flows/ExampleFlow/runs", 4),
    ]
    assert ("flow", "run") not in internal_calls


@pytest.mark.parametrize("provider", ["paginating", "legacy", "local"])
def test_flow_iter_order_and_namespace_parity(provider, fake_service, request):
    """Every provider yields newest-first and applies the namespace filter."""
    if provider == "local":
        # local_flow serves runs 1..3 (prod, dev, prod) oldest-first.
        local_flow = request.getfixturevalue("local_flow")
        metadata_str = local_flow._metaflow.metadata.metadata_str()
        expected_all = ["3", "2", "1"]

        def make_flow(**kwargs):
            return Flow("ExampleFlow", _current_metadata=metadata_str, **kwargs)

    else:
        fake_service(
            _runs_collection(),
            version=(
                PAGINATING_SERVICE_VERSION
                if provider == "paginating"
                else LEGACY_SERVICE_VERSION
            ),
        )
        expected_all = ["5", "4", "3", "2", "1"]

        def make_flow(**kwargs):
            return Flow(_object=dict(_FLOW_RECORD), **kwargs)

    # Odd run numbers are tagged "prod".
    expected_prod = [run_id for run_id in expected_all if int(run_id) % 2 == 1]

    assert [r.id for r in make_flow(_namespace_check=False)] == expected_all
    assert [r.id for r in make_flow(_current_namespace="prod")] == expected_prod


def test_attempt_scoped_task_artifacts_go_through_get_object(fake_service):
    """The per-attempt artifact endpoint has no pagination, so iterating a Task
    with an attempt uses the bulk GET and is still newest-first."""
    path = "/flows/ExampleFlow/runs/12/steps/start/tasks/1/attempt/0/artifacts"
    service = fake_service(
        {
            path: [
                _artifact_record("a", 1000),
                _artifact_record("b", 3000),
                _artifact_record("c", 2000),
            ]
        }
    )
    task = Task(
        "ExampleFlow/12/start/1",
        _object=dict(_TASK_RECORD),
        attempt=0,
        _namespace_check=False,
    )

    assert [artifact.id for artifact in task] == ["b", "c", "a"]
    assert service.calls == [(path, None)]


def test_run_and_task_iter_hide_private_children_when_streaming(fake_service):
    steps_path = "/flows/ExampleFlow/runs/12/steps"
    artifacts_path = "/flows/ExampleFlow/runs/12/steps/start/tasks/1/artifacts"
    service = fake_service(
        {
            steps_path: [
                _step_record("_parameters", 1000),
                _step_record("start", 2000),
                _step_record("end", 3000),
            ],
            artifacts_path: [
                _artifact_record("_foreach_stack", 1000),
                _artifact_record("name", 2000),
                _artifact_record("_task_ok", 3000),
            ],
        }
    )

    run = Run("ExampleFlow/12", _object=dict(_RUN_RECORD), _namespace_check=False)
    task = Task(
        "ExampleFlow/12/start/1", _object=dict(_TASK_RECORD), _namespace_check=False
    )

    assert [step.id for step in run] == ["end", "start"]
    assert [artifact.id for artifact in task] == ["name"]
    # Both went through the paginated path.
    assert service.calls == [(steps_path, 0), (artifacts_path, 0)]


@pytest.mark.parametrize(
    "version, sub_type, attempt, expected",
    [
        (PAGINATING_SERVICE_VERSION, "run", None, True),
        (PAGINATING_SERVICE_VERSION, "artifact", None, True),
        (PAGINATING_SERVICE_VERSION, "artifact", 0, False),
        (PAGINATING_SERVICE_VERSION, "self", None, False),
        (LEGACY_SERVICE_VERSION, "run", None, False),
    ],
)
def test_service_supports_streaming_iteration(
    version, sub_type, attempt, expected, monkeypatch
):
    _use_service_version(monkeypatch, version)
    assert (
        ServiceMetadataProvider.supports_streaming_iteration("flow", sub_type, attempt)
        is expected
    )


def test_base_provider_does_not_support_streaming_iteration():
    assert not LocalMetadataProvider.supports_streaming_iteration("flow", "run", None)


def test_legacy_iteration_uses_get_object(fake_service, monkeypatch):
    """Without streaming support, __iter__ materializes through get_object."""
    service = fake_service(_runs_collection(), version=LEGACY_SERVICE_VERSION)

    def fail_iter_objects(cls, *args, **kwargs):
        raise AssertionError("legacy iteration must not use iter_objects")

    monkeypatch.setattr(
        ServiceMetadataProvider, "iter_objects", classmethod(fail_iter_objects)
    )

    flow = Flow(_object=dict(_FLOW_RECORD), _namespace_check=False)

    assert [run.id for run in flow] == ["5", "4", "3", "2", "1"]
    assert service.calls == [("/flows/ExampleFlow/runs", None)]
