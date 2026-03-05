#!/usr/bin/env python
"""
Demo script for the @huggingface decorator.

Run from repo root:
  python run_huggingface_demo.py run

Or run a single step (no download):
  python run_huggingface_demo.py run --no-pylint 2>&1 | head -30

Requires: pip install -e .  and  pip install huggingface_hub
Optional: HF_TOKEN or HUGGING_FACE_HUB_TOKEN for gated models.
"""
from metaflow import FlowSpec, step, huggingface, current


class HuggingFaceDemoFlow(FlowSpec):
    """Single step: download bert-base-uncased via @huggingface and print path."""

    @huggingface(models=["bert-base-uncased"])
    @step
    def start(self):
        path = current.huggingface.models["bert-base-uncased"]
        print("Model path:", path)
        self.model_path = path
        self.next(self.end)

    @step
    def end(self):
        print("Done. Model at:", self.model_path)


if __name__ == "__main__":
    HuggingFaceDemoFlow()
