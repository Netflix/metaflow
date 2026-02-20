from typing import Any, Dict, Optional
import requests

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    # Fallback for Python 3.7 and below
    from typing_extensions import Protocol, runtime_checkable


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
    pool config currently hardcoded in ServiceMetadataProvider.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,  # retries handled explicitly outside
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
