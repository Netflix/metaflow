#!/usr/bin/env python
"""
Demo script for the @huggingface decorator.

Run with HUGGINGFACE_DEMO_MODE (see demos/huggingface/run_huggingface_demo.sh):
  none             – public model, metadata only
  env              – private model, metadata only (HF_TOKEN or HUGGING_FACE_HUB_TOKEN)
  download         – private model, full download (same env token vars)
  vendor           – private model, metadata only (vendor-token retrieval service)
  vendor-download  – private model, full download (vendor-token retrieval service)

See docs/huggingface.md for configuration and auth.
"""
import os

_VALID_MODES = (
    "none",
    "download",
    "env",
    "vendor",
    "vendor-download",
)
_raw_mode = (os.environ.get("HUGGINGFACE_DEMO_MODE", "none") or "none").strip().lower()
if _raw_mode not in _VALID_MODES:
    raise RuntimeError(
        "Unknown HUGGINGFACE_DEMO_MODE=%r; expected one of %s"
        % (os.environ.get("HUGGINGFACE_DEMO_MODE"), _VALID_MODES)
    )
_DEMO_MODE = _raw_mode


def _validate_demo_mode(mode: str) -> None:
    """Fail fast if required environment variables for the chosen mode are missing."""
    if mode in ("env", "download"):
        if not (os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")):
            raise RuntimeError(
                "HUGGINGFACE_DEMO_MODE=%r requires HF_TOKEN or HUGGING_FACE_HUB_TOKEN "
                "in the environment (env-based Hugging Face auth)." % mode
            )
    if mode in ("vendor", "vendor-download"):
        url = os.environ.get("METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL") or os.environ.get(
            "HUGGINGFACE_VENDOR_TOKEN_URL"
        )
        if not url or not str(url).strip():
            raise RuntimeError(
                "HUGGINGFACE_DEMO_MODE=%r requires METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL "
                "or HUGGINGFACE_VENDOR_TOKEN_URL to be set to the hf-token endpoint URL "
                "(vendor token retrieval service)." % mode
            )
        if os.environ.get("METAFLOW_HUGGINGFACE_AUTH_PROVIDER") != "vendor-token":
            raise RuntimeError(
                "HUGGINGFACE_DEMO_MODE=%r requires METAFLOW_HUGGINGFACE_AUTH_PROVIDER=vendor-token "
                "(use demos/huggingface/run_huggingface_demo.sh so the script sets it)."
                % mode
            )


_validate_demo_mode(_DEMO_MODE)

from metaflow import FlowSpec, step, huggingface, current


def _print_model_metadata(info) -> None:
    print("Model id:", info.id)
    print("Revision (sha):", getattr(info, "sha", "N/A"))
    print("Files (siblings):", len(info.siblings) if info.siblings else 0)


# -----------------------------------------------------------------------------
# 1. Public model (no token)
# -----------------------------------------------------------------------------
class HuggingFaceDemoFlowPublic(FlowSpec):
    """Fetch metadata for a public model. No token required."""

    @huggingface(
        models={"gpt2": "openai-community/gpt2@main"},
        metadata_only=True,
    )
    @step
    def start(self):
        info = current.huggingface.model_info["gpt2"]
        _print_model_metadata(info)
        self.model_id = info.id
        self.next(self.end)

    @step
    def end(self):
        print("Done. Model id:", self.model_id)


# -----------------------------------------------------------------------------
# 2. Private model – full download (set HF_TOKEN, HUGGING_FACE_TOKEN, or HUGGING_FACE_HUB_TOKEN)
# -----------------------------------------------------------------------------
class HuggingFaceDemoFlowDownload(FlowSpec):
    """Download a private model to disk. Set a Hugging Face token env var (see docs/huggingface.md)."""

    @huggingface(models={"gpt2": "netflix/my-gpt2@main"})
    @step
    def start(self):
        path = current.huggingface.models["gpt2"]
        print("Model path:", path)
        self.model_path = path
        self.next(self.end)

    @step
    def end(self):
        print("Done. Model at:", self.model_path)


# -----------------------------------------------------------------------------
# 3. Private model – metadata only (set token env)
# -----------------------------------------------------------------------------
class HuggingFaceDemoFlowEnv(FlowSpec):
    """Fetch metadata for a private model using a Hugging Face token env var (see docs/huggingface.md)."""

    @huggingface(
        models={"gpt2": "netflix/my-gpt2@main"},
        metadata_only=True,
    )
    @step
    def start(self):
        info = current.huggingface.model_info["gpt2"]
        _print_model_metadata(info)
        self.model_id = info.id
        self.next(self.end)

    @step
    def end(self):
        print("Done. Model id:", self.model_id)


_FLOWS = {
    "none": HuggingFaceDemoFlowPublic,
    "download": HuggingFaceDemoFlowDownload,
    "env": HuggingFaceDemoFlowEnv,
    "vendor": HuggingFaceDemoFlowEnv,
    "vendor-download": HuggingFaceDemoFlowDownload,
}

if __name__ == "__main__":
    _FLOWS[_DEMO_MODE]()
