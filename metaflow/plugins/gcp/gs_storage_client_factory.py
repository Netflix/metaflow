import os
import threading

_client_cache = dict()


def _get_cache_key():
    return os.getpid(), threading.get_ident()


def get_gs_storage_client():
    cache_key = _get_cache_key()
    if cache_key not in _client_cache:
        from google.cloud import storage
        import google.auth

        credentials, project_id = google.auth.default(scopes=storage.Client.SCOPE)
        _client_cache[cache_key] = storage.Client(
            credentials=credentials, project=project_id
        )
    return _client_cache[cache_key]
