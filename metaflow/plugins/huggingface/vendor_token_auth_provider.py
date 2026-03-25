"""
HuggingFace auth provider that uses the vendor-token-retrieval /hf-token service.

Returns a short-lived Hugging Face OAuth access token for the authenticated caller.
Requires Metatron mutual TLS. Set METAFLOW_HUGGINGFACE_AUTH_PROVIDER=vendor-token to enable.

Requires: requests, metatron (MetatronAdapter). Ensure Metatron credentials are fresh (metatron refresh).
"""

from typing import Optional

from .auth import HuggingFaceAuthProvider

VENDOR_TOKEN_METATRON_APP = "vendortokenretrieval"


def _require_vendor_token_deps():
    try:
        import requests
        from metatron.http import MetatronAdapter
    except ImportError as e:
        from metaflow.exception import MetaflowException

        raise MetaflowException(
            "@huggingface: vendor-token auth provider requires 'requests' and "
            "'metatron' (MetatronAdapter). Install them for your environment. "
            "Ensure Metatron credentials are fresh (metatron refresh). "
            "Or use HUGGINGFACE_AUTH_PROVIDER=env with HF_TOKEN. Error: %s" % e
        ) from e
    return requests, MetatronAdapter


def _get_vendor_token_url():
    from metaflow.metaflow_config import HUGGINGFACE_VENDOR_TOKEN_URL

    return HUGGINGFACE_VENDOR_TOKEN_URL


def _http_get_json(url: str, requests_mod, metatron_adapter_cls):
    session = requests_mod.Session()
    session.mount("https://", metatron_adapter_cls(VENDOR_TOKEN_METATRON_APP))
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def _normalize_access_token(raw) -> Optional[str]:
    if isinstance(raw, str) and raw:
        return raw.strip()
    return None


class VendorTokenAuthProvider(HuggingFaceAuthProvider):
    """
    Supplies Hugging Face token from the vendor-token-retrieval /hf-token endpoint.
    Uses MetatronAdapter for mutual TLS. Response contains access_token and expires_at.
    """

    TYPE = "vendor-token"

    def get_token(self) -> Optional[str]:
        from metaflow.exception import MetaflowException

        requests_mod, metatron_adapter_cls = _require_vendor_token_deps()
        url = _get_vendor_token_url()
        if not url:
            raise MetaflowException(
                "@huggingface: vendor-token auth requires HUGGINGFACE_VENDOR_TOKEN_URL "
                "(or METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL) to be set."
            )
        try:
            data = _http_get_json(url, requests_mod, metatron_adapter_cls)
        except Exception as e:
            raise MetaflowException(
                "@huggingface: vendor-token-retrieval request failed: %s" % e
            ) from e
        try:
            raw = data.get("access_token")
            return _normalize_access_token(raw)
        except Exception as e:
            raise MetaflowException(
                "@huggingface: vendor-token response invalid (expected JSON with access_token): %s"
                % e
            ) from e
