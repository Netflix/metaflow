"""
Minimal flow to test @huggingface locally.

Run from repo root:
  PYTHONPATH=. python -m metaflow.plugins.huggingface.example_flow run

Requires: pip install metaflow[huggingface] or pip install huggingface_hub
Optional: set HF_TOKEN or HUGGING_FACE_HUB_TOKEN for gated models.
More detail: docs/huggingface.md (configuration, custom auth). Demo CLI: demos/huggingface/README.md.
"""

from metaflow import FlowSpec, step, huggingface


class HuggingFaceExampleFlow(FlowSpec):
    @huggingface(models=["bert-base-uncased"])
    @step
    def start(self):
        from metaflow import current

        self.model_path = current.huggingface.models["bert-base-uncased"]
        print("Model path:", self.model_path)
        self.next(self.end)

    @step
    def end(self):
        print("Done. Model was at:", self.model_path)


if __name__ == "__main__":
    HuggingFaceExampleFlow()
