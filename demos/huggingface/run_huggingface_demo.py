#!/usr/bin/env python
"""
Demo entrypoint for ``@huggingface``.

The script defines three small flows (one per mode). Select the mode with
``HUGGINGFACE_DEMO_MODE``:

- ``none`` (default): public model, metadata only, no token.
- ``download``: private model, full snapshot (set a Hugging Face token in the environment).
- ``env``: private model, metadata only (token env).

See ``demos/huggingface/README.md`` for how to run the shell wrapper. Conceptual
documentation and configuration are in ``docs/huggingface.md``. The private-repo
examples use ``netflix/my-gpt2``; change ``models=`` for your own repo.
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
}

if __name__ == "__main__":
    _FLOWS[_DEMO_MODE]()
