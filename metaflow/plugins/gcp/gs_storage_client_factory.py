import os
import threading

_client_cache = dict()


def _get_cache_key():
    return os.getpid(), threading.get_ident()


def _get_gs_storage_client_default():
    cache_key = _get_cache_key()
    if cache_key not in _client_cache:
        from google.cloud import storage
        import google.auth

        credentials, project_id = google.auth.default(scopes=storage.Client.SCOPE)
        _client_cache[cache_key] = storage.Client(
            credentials=credentials, project=project_id
        )
    return _client_cache[cache_key]


class GcpDefaultClientProvider(object):
    name = "gcp-default"

    @staticmethod
    def get_gs_storage_client(*args, **kwargs):
        return _get_gs_storage_client_default()

    @staticmethod
    def get_credentials(scopes, *args, **kwargs):
        import google.auth

        return google.auth.default(scopes=scopes)


cached_provider_class = None


def get_gs_storage_client():
    global cached_provider_class
    if cached_provider_class is None:
        from metaflow.metaflow_config import DEFAULT_GCP_CLIENT_PROVIDER
        from metaflow.plugins import GCP_CLIENT_PROVIDERS

        for p in GCP_CLIENT_PROVIDERS:
            if p.name == DEFAULT_GCP_CLIENT_PROVIDER:
                cached_provider_class = p
                break
        else:
            raise ValueError(
                "Cannot find GCP Client provider %s" % DEFAULT_GCP_CLIENT_PROVIDER
            )
    return cached_provider_class.get_gs_storage_client()


def get_credentials(scopes, *args, **kwargs):
    global cached_provider_class
    if cached_provider_class is None:
        from metaflow.metaflow_config import DEFAULT_GCP_CLIENT_PROVIDER
        from metaflow.plugins import GCP_CLIENT_PROVIDERS

        for p in GCP_CLIENT_PROVIDERS:
            if p.name == DEFAULT_GCP_CLIENT_PROVIDER:
                cached_provider_class = p
                break
        else:
            raise ValueError(
                "Cannot find GCP Client provider %s" % DEFAULT_GCP_CLIENT_PROVIDER
            )
    return cached_provider_class.get_credentials(scopes, *args, **kwargs)
