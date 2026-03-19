"""
@huggingface step decorator: pluggable auth for HuggingFace models (Part 1).

Provides current.huggingface.models[key] -> local path. Supports models=[] and
model_mapping={alias: repo_id@revision}. Uses huggingface_hub for download.
"""

import os
from typing import Dict, List, Optional, Tuple

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_current import current


# Minimal object exposed as current.huggingface with a .models mapping
class HuggingFaceContext:
    """
    Context object attached to current.huggingface when @huggingface is used.
    models maps user-facing key (alias or repo_id) to local filesystem path (str).
    """

    def __init__(self, models: Dict[str, str]):
        self.models = models


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


def _build_spec_map(
    models: Optional[List[str]], model_mapping: Optional[Dict[str, str]]
) -> Dict[str, Tuple[str, str]]:
    """Build key -> (repo_id, revision). Key is alias or repo_id."""
    spec_map = {}
    if models:
        for v in models:
            if not isinstance(v, str):
                raise MetaflowException(
                    "@huggingface: models must be a list of strings, got %s" % type(v)
                )
            repo_id, revision = _parse_repo_spec(v)
            spec_map[repo_id] = (repo_id, revision)
    if model_mapping:
        for k, v in model_mapping.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise MetaflowException(
                    "@huggingface: model_mapping must be dict of str -> str"
                )
            repo_id, revision = _parse_repo_spec(v)
            spec_map[k] = (repo_id, revision)
    return spec_map


def _get_auth_provider():
    from metaflow.metaflow_config import METAFLOW_HUGGINGFACE_AUTH_PROVIDER
    from metaflow.plugins import HF_AUTH_PROVIDERS

    provider_type = METAFLOW_HUGGINGFACE_AUTH_PROVIDER or "env"
    provider_cls = next(
        (p for p in HF_AUTH_PROVIDERS if getattr(p, "TYPE", None) == provider_type),
        None,
    )
    if provider_cls is None:
        from metaflow.plugins.huggingface.env_auth_provider import (
            EnvHuggingFaceAuthProvider,
        )

        return EnvHuggingFaceAuthProvider()
    return provider_cls()


def _download_model(
    repo_id: str, revision: str, token: Optional[str], local_dir: str
) -> str:
    try:
        from huggingface_hub import snapshot_download
    except ImportError as e:
        raise MetaflowException(
            "@huggingface requires the 'huggingface_hub' package. "
            "Install it with: pip install huggingface_hub. Error: %s" % e
        ) from e
    path = snapshot_download(
        repo_id=repo_id,
        revision=revision,
        token=token,
        local_dir=local_dir,
        local_dir_use_symlinks=False,
    )
    return path


class HuggingFaceDecorator(StepDecorator):
    """
    Declares HuggingFace models needed for this step. Auth is pluggable;
    model paths are exposed via current.huggingface.models[key].

    Parameters
    ----------
    models : list, optional
        List of repo ids (and optional revisions), e.g.
        ["meta-llama/Llama-2-7b", "bert-base-uncased@v1.0"].
    model_mapping : dict, optional
        Alias -> repo spec, e.g.
        {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"}.
        Access in step via current.huggingface.models["llama"].

    MF Add To Current
    -----------------
    huggingface -> HuggingFaceContext
        Object with a ``models`` attribute: dict-like mapping from model key
        (alias or repo_id) to local filesystem path (str). Use
        current.huggingface.models["key"] to get the path for loading with
        transformers or other HF APIs.
    """

    name = "huggingface"
    defaults = {"models": None, "model_mapping": None}

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        models = self.attributes.get("models")
        model_mapping = self.attributes.get("model_mapping")
        if not models and not model_mapping:
            raise MetaflowException(
                "@huggingface: specify at least one of 'models' or 'model_mapping'"
            )
        self._spec_map = _build_spec_map(models, model_mapping)
        if not self._spec_map:
            raise MetaflowException(
                "@huggingface: at least one model or model_mapping entry is required"
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
        token = None
        try:
            auth_provider = _get_auth_provider()
            token = auth_provider.get_token()
        except Exception as e:
            raise MetaflowException(
                "@huggingface: auth provider failed: %s" % e
            ) from e

        base_dir = os.path.join(current.tempdir or "/tmp", "metaflow_huggingface")
        os.makedirs(base_dir, exist_ok=True)
        path_map = {}  # key -> local path

        for key, (repo_id, revision) in self._spec_map.items():
            task_subdir = os.path.join(
                base_dir, "%s_%s" % (repo_id.replace("/", "_"), revision)
            )
            local_path = _download_model(repo_id, revision, token, task_subdir)
            path_map[key] = local_path

        ctx = HuggingFaceContext(models=path_map)
        current._update_env({"huggingface": ctx})
