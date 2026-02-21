import sys
import time
from typing import Any, Dict, Optional

import requests

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    # Fallback for Python < 3.8 — use metaflow's vendored copy rather than
    # the system typing_extensions which is not declared as a dependency.
    from metaflow._vendor.typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class MetaflowServiceRequestProvider(Protocol):
    """
    Transport protocol for Metaflow metadata service HTTP requests.

    Implement this Protocol to inject custom authentication, tracing,
    or transport behavior (e.g., mTLS, token refresh, request logging).

    The `request()` method is the single entry point for all HTTP verbs.
    The `base_headers` dict contains Metaflow's configured service headers
    (from METAFLOW_SERVICE_HEADERS) and must be merged into the outgoing
    request — the provider MAY augment them but MUST NOT silently drop them.
    """

    def request(
        self,
        method: str,
        url: str,
        base_headers: Dict[str, str],
        json: Optional[Any] = None,
    ) -> requests.Response: ...

    def close(self) -> None: ...


class DefaultRequestProvider:
    """
    Default transport provider. Wraps requests.Session with the same
    pool config previously hardcoded in ServiceMetadataProvider.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,
            pool_block=False,
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def request(
        self,
        method: str,
        url: str,
        base_headers: Dict[str, str],
        json: Optional[Any] = None,
    ) -> requests.Response:
        return self._session.request(
            method=method,
            url=url,
            headers=base_headers,
            json=json,
        )

    def close(self) -> None:
        self._session.close()


class TracingRequestProvider:
    """
    Example provider that logs all metadata service HTTP calls to stderr.

    Useful for debugging connectivity issues or profiling service latency
    without modifying application code. Each request line includes method,
    URL, HTTP status code, and round-trip latency in milliseconds.

    Enable via environment variable or Metaflow config:

        export METAFLOW_SERVICE_REQUEST_PROVIDER=\\
            metaflow.plugins.metadata_providers.request_provider.TracingRequestProvider

    Example output::

        [METAFLOW_TRACE] POST https://metaflow-svc/flows/MyFlow/run -> 201 (43.2ms)
        [METAFLOW_TRACE] GET  https://metaflow-svc/flows/MyFlow/runs/5 -> 200 (11.8ms)
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,
            pool_block=False,
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def request(
        self,
        method: str,
        url: str,
        base_headers: Dict[str, str],
        json: Optional[Any] = None,
    ) -> requests.Response:
        start = time.time()
        try:
            resp = self._session.request(
                method=method,
                url=url,
                headers=base_headers,
                json=json,
            )
            elapsed_ms = (time.time() - start) * 1000
            sys.stderr.write(
                "[METAFLOW_TRACE] %-6s %s -> %s (%.1fms)\n"
                % (method, url, resp.status_code, elapsed_ms)
            )
            return resp
        except Exception as exc:
            elapsed_ms = (time.time() - start) * 1000
            sys.stderr.write(
                "[METAFLOW_TRACE] %-6s %s -> ERROR: %s (%.1fms)\n"
                % (method, url, type(exc).__name__, elapsed_ms)
            )
            raise

    def close(self) -> None:
        self._session.close()
