"""
Step decorator for resolving Hugging Face models before user code runs.

At task start the decorator downloads snapshots (or fetches metadata only), using a
pluggable auth provider and ``huggingface_hub``. Step code reads paths or ``ModelInfo``
from ``current.huggingface``. Pass ``models`` as a list or dict of repo specs; see
``docs/huggingface.md`` (Behavior) and ``HuggingFaceDecorator`` below.
"""

import os
import sys
from typing import Dict, List, Optional, Tuple, Union

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_current import current


class HuggingFaceContext:
    """
    Values exposed as ``current.huggingface`` on ``@huggingface`` steps.

    ``models`` maps each key (repo id or alias) to a local directory path when the
    decorator downloaded files. ``model_info`` maps each key to a Hugging Face
    ``ModelInfo`` when the decorator used metadata-only mode. For a given task, the
    active fields depend on ``metadata_only``; the other mapping is empty.
    """

    def __init__(
        self,
        models: Optional[Dict[str, str]] = None,
        model_info: Optional[Dict[str, object]] = None,
    ):
        self.models = models or {}
        self.model_info = model_info or {}


def _parse_repo_spec(value: str) -> Tuple[str, str]:
    """Parse 'repo_id' or 'repo_id@revision' into (repo_id, revision)."""
    value = (value or "").strip()
    if not value:
        raise MetaflowException(
            "@huggingface: empty model spec; use repo_id or repo_id@revision"
        )
    if "@" in value:
        repo_id, revision = value.rsplit("@", 1)
        repo_id = repo_id.strip()
        revision = revision.strip()
        if not repo_id or not revision:
            raise MetaflowException(
                "@huggingface: invalid spec '%s'; use repo_id@revision" % value
            )
        return repo_id, revision
    return value, "main"


def _add_specs_from_list(
    spec_map: Dict[str, Tuple[str, str]], entries: List[str]
) -> None:
    for v in entries:
        if not isinstance(v, str):
            raise MetaflowException(
                "@huggingface: models list must contain strings, got %s" % type(v)
            )
        repo_id, revision = _parse_repo_spec(v)
        spec_map[repo_id] = (repo_id, revision)


def _add_specs_from_dict(
    spec_map: Dict[str, Tuple[str, str]], mapping: Dict[str, str]
) -> None:
    for k, v in mapping.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise MetaflowException("@huggingface: models dict must be str -> str")
        repo_id, revision = _parse_repo_spec(v)
        spec_map[k] = (repo_id, revision)


def _build_spec_map(
    models: Optional[Union[List[str], Dict[str, str]]],
) -> Dict[str, Tuple[str, str]]:
    """Build key -> (repo_id, revision). models is a list of repo specs or dict of alias -> repo spec."""
    spec_map = {}
    if not models:
        return spec_map
    if isinstance(models, list):
        _add_specs_from_list(spec_map, models)
    elif isinstance(models, dict):
        _add_specs_from_dict(spec_map, models)
    else:
        raise MetaflowException(
            "@huggingface: models must be a list or dict, got %s" % type(models)
        )
    return spec_map


def _get_auth_provider():
    from metaflow.metaflow_config import HUGGINGFACE_AUTH_PROVIDER
    from metaflow.plugins import HF_AUTH_PROVIDERS

    provider_type = HUGGINGFACE_AUTH_PROVIDER or "env"
    provider_cls = next(
        (p for p in HF_AUTH_PROVIDERS if getattr(p, "TYPE", None) == provider_type),
        None,
    )
    if provider_cls is None:
        if provider_type == "env":
            from metaflow.plugins.huggingface.env_auth_provider import (
                EnvHuggingFaceAuthProvider,
            )

            return EnvHuggingFaceAuthProvider()
        raise MetaflowException(
            "@huggingface: unknown Hugging Face auth provider '%s'. "
            "Check METAFLOW_HUGGINGFACE_AUTH_PROVIDER / HUGGINGFACE_AUTH_PROVIDER, "
            "that the provider is registered via HF_AUTH_PROVIDERS_DESC (e.g. in "
            "metaflow_extensions), and ENABLED_HF_AUTH_PROVIDER if you use an allowlist."
            % provider_type
        )
    return provider_cls()


def _import_snapshot_download():
    try:
        from huggingface_hub import snapshot_download
    except ImportError as e:
        raise MetaflowException(
            "@huggingface requires the 'huggingface_hub' package. "
            "Install it with: pip install huggingface_hub or pip install metaflow[huggingface]. Error: %s"
            % e
        ) from e
    return snapshot_download


def _import_hf_api():
    try:
        from huggingface_hub import HfApi
    except ImportError as e:
        raise MetaflowException(
            "@huggingface requires the 'huggingface_hub' package. "
            "Install it with: pip install huggingface_hub or pip install metaflow[huggingface]. Error: %s"
            % e
        ) from e
    return HfApi


def _download_model(
    repo_id: str,
    revision: str,
    token: Optional[str],
    local_dir: str,
    endpoint: Optional[str] = None,
) -> str:
    snapshot_download = _import_snapshot_download()
    kwargs = dict(
        repo_id=repo_id,
        revision=revision,
        token=token,
        local_dir=local_dir,
    )
    if endpoint is not None:
        kwargs["endpoint"] = endpoint
    try:
        return snapshot_download(**kwargs)
    except MetaflowException:
        raise
    except Exception as e:
        raise MetaflowException(
            "@huggingface: snapshot download failed for %s@%s: %s"
            % (repo_id, revision, e)
        ) from e


def _model_info_404_hint(repo_id: str) -> str:
    return (
        "Token was obtained but Hugging Face returned 404. "
        "Ensure the token has read access to repo '%s' and is for the correct account "
        "(e.g. enterprise vs open-source)."
    ) % repo_id


def _get_model_info(
    repo_id: str,
    revision: str,
    token: Optional[str],
    endpoint: Optional[str] = None,
) -> object:
    """Fetch model metadata from Hugging Face without downloading files."""
    HfApi = _import_hf_api()
    base_url = endpoint or "https://huggingface.co"
    api = HfApi(token=token, endpoint=base_url)
    try:
        return api.model_info(repo_id, revision=revision)
    except Exception as e:
        err_str = str(e).lower()
        if token and (
            "404" in err_str or "not found" in err_str or "repository" in err_str
        ):
            raise MetaflowException(
                "@huggingface: failed to get model info for %s@%s from %s: %s. %s"
                % (repo_id, revision, base_url, e, _model_info_404_hint(repo_id))
            ) from e
        raise


def _log_auth_provider(provider_type: str, token: Optional[str]) -> None:
    msg = "@huggingface: using auth provider '%s', token %s"
    sys.stderr.write(msg % (provider_type, "obtained" if token else "none") + "\n")


def _resolve_auth_token():
    """Return the Hub API token string, or ``None`` for public-only access.

    Logs which auth provider ran via ``_log_auth_provider``. Exceptions from
    ``get_token()`` propagate to the caller.
    """
    auth_provider = _get_auth_provider()
    provider_type = getattr(auth_provider, "TYPE", "unknown")
    token = auth_provider.get_token()
    _log_auth_provider(provider_type, token)
    return token


def _download_to_task_dir(
    repo_id: str,
    revision: str,
    token: Optional[str],
    endpoint: Optional[str],
) -> str:
    base_dir = os.path.join(current.tempdir or "/tmp", "metaflow_huggingface")
    os.makedirs(base_dir, exist_ok=True)
    task_subdir = os.path.join(
        base_dir, "%s_%s" % (repo_id.replace("/", "_"), revision)
    )
    return _download_model(repo_id, revision, token, task_subdir, endpoint=endpoint)


def _fill_huggingface_maps(
    spec_map: Dict[str, Tuple[str, str]],
    metadata_only: bool,
    token: Optional[str],
    endpoint: Optional[str],
) -> Tuple[Dict[str, str], Dict[str, object]]:
    path_map = {}
    info_map = {}
    for key, (repo_id, revision) in spec_map.items():
        if metadata_only:
            info_map[key] = _get_model_info(repo_id, revision, token, endpoint=endpoint)
        else:
            path_map[key] = _download_to_task_dir(repo_id, revision, token, endpoint)
    return path_map, info_map


class HuggingFaceDecorator(StepDecorator):
    """
    Declare which Hugging Face repos this step needs. Resolution and authentication run
    in ``task_pre_step``; the step body then reads ``current.huggingface``.

    The Hub base URL defaults to ``https://huggingface.co``. Set
    ``METAFLOW_HUGGINGFACE_ENDPOINT`` only when the API is hosted elsewhere (for example
    on-prem). Enterprise versus public access follows the token, not a separate setting.

    Parameters
    ----------
    models : list or dict, optional
        Either a list of repo specs, e.g.
        ["meta-llama/Llama-2-7b", "bert-base-uncased@v1.0"],
        or a dict of alias -> repo spec, e.g.
        {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"}.
        Access in step via current.huggingface.models["key"] (key is repo_id or alias).
    metadata_only : bool, optional
        If True, only fetch model metadata from Hugging Face (no file download).
        Use current.huggingface.model_info["key"] to get the ModelInfo object.

    MF Add To Current
    -----------------
    huggingface -> HuggingFaceContext
        Object with ``models`` (key -> local path when not metadata_only) and
        ``model_info`` (key -> ModelInfo when metadata_only=True). Use
        current.huggingface.models["key"] for paths or
        current.huggingface.model_info["key"] for metadata only.
    """

    name = "huggingface"
    defaults = {"models": None, "metadata_only": False}

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        models = self.attributes.get("models")
        self._metadata_only = self.attributes.get("metadata_only", False)
        if not models:
            raise MetaflowException("@huggingface: specify 'models' (list or dict)")
        self._spec_map = _build_spec_map(models)
        if not self._spec_map:
            raise MetaflowException(
                "@huggingface: 'models' must contain at least one entry"
            )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        try:
            token = _resolve_auth_token()
        except Exception as e:
            raise MetaflowException("@huggingface: auth provider failed: %s" % e) from e

        from metaflow.metaflow_config import HUGGINGFACE_ENDPOINT

        endpoint = HUGGINGFACE_ENDPOINT
        path_map, info_map = _fill_huggingface_maps(
            self._spec_map, self._metadata_only, token, endpoint
        )
        ctx = HuggingFaceContext(models=path_map, model_info=info_map)
        current._update_env({"huggingface": ctx})
