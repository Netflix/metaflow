#!/usr/bin/env python
"""
Demo script for the @huggingface decorator.

Three separate flows so the decorators are clear for each mode. Run with:
  HUGGINGFACE_DEMO_MODE=none     (default) – public model, metadata only
  HUGGINGFACE_DEMO_MODE=download – private model, full download (HF_TOKEN)
  HUGGINGFACE_DEMO_MODE=env      – private model, metadata only (HF_TOKEN)

See run_huggingface_demo.sh run [none|download|env].
See docs/huggingface.md for configuration and auth.
"""
import os

_DEMO_MODE = os.environ.get("HUGGINGFACE_DEMO_MODE", "none").strip().lower()
if _DEMO_MODE not in ("none", "download", "env"):
    _DEMO_MODE = "none"

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
# 2. Private model – full download (set HF_TOKEN for private repos)
# -----------------------------------------------------------------------------
class HuggingFaceDemoFlowDownload(FlowSpec):
    """Download a private model to disk. Set HF_TOKEN (or HUGGING_FACE_HUB_TOKEN)."""

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
# 3. Private model – metadata only (set HF_TOKEN)
# -----------------------------------------------------------------------------
class HuggingFaceDemoFlowEnv(FlowSpec):
    """Fetch metadata for a private model using HF_TOKEN (or HUGGING_FACE_HUB_TOKEN)."""

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
}

if __name__ == "__main__":
    _FLOWS[_DEMO_MODE]()
