import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError

from metaflow.exception import MetaflowInternalError
from metaflow.plugins.metadata_providers.service import (
    ServiceException,
    ServiceMetadataProvider,
)


def _filter_tasks():
    return ServiceMetadataProvider.filter_tasks_by_metadata(
        "HelloFlow", "1", "start", "attempt_ok", "true"
    )


def test_filter_tasks_by_metadata_propagates_non_service_exception(mocker):
    """A connection-level failure from _request must surface unchanged.

    Regression test: filter_tasks_by_metadata caught bare ``Exception`` and read
    ``e.http_code``, which only exists on ServiceException. A ConnectionError (or
    any error without http_code) therefore raised AttributeError, masking the real
    failure. The handler now catches ServiceException only, so other errors
    propagate as-is.
    """
    mocker.patch.object(
        ServiceMetadataProvider,
        "_request",
        side_effect=RequestsConnectionError("connection refused"),
    )
    with pytest.raises(RequestsConnectionError):
        _filter_tasks()


def test_filter_tasks_by_metadata_missing_endpoint_raises_internal_error(mocker):
    """A 404 ServiceException is translated into an actionable upgrade message."""
    mocker.patch.object(
        ServiceMetadataProvider,
        "_request",
        side_effect=ServiceException("not found", http_code=404),
    )
    with pytest.raises(MetaflowInternalError):
        _filter_tasks()


def test_filter_tasks_by_metadata_reraises_other_service_exception(mocker):
    """A non-404 ServiceException propagates unchanged."""
    mocker.patch.object(
        ServiceMetadataProvider,
        "_request",
        side_effect=ServiceException("server error", http_code=500),
    )
    with pytest.raises(ServiceException):
        _filter_tasks()
