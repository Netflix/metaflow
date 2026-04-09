# Hugging Face integration (`@huggingface`)

The `@huggingface` step decorator declares models your step needs from [Hugging Face](https://huggingface.co). It resolves authentication before your step runs, then exposes paths or `ModelInfo` on `current.huggingface`. **By default**, each repo is downloaded (or metadata is fetched) **on first access** to `current.huggingface.models[key]` or `model_info[key]`, so you only pay for models you actually use. Pass **`lazy=False`** if you want every listed model prefetched in full before your step body runs.

## Installation

Install Metaflow with the Hugging Face extra so `huggingface_hub` is available:

```bash
pip install "metaflow[huggingface]"
```

## Usage

```python
from metaflow import FlowSpec, step, huggingface, current

class MyFlow(FlowSpec):
    @huggingface(
        models={"llama": "meta-llama/Llama-2-7b@main"},
        metadata_only=False,  # omit or False to download; True for metadata only
    )
    @step
    def train(self):
        path = current.huggingface.models["llama"]
        # load from path with transformers, etc.
        self.next(self.end)

    @step
    def end(self):
        pass
```

- **`models`**: a **list** of repo specs (`"org/model"` or `"org/model@revision"`) or a **dict** mapping your alias to a repo spec. With a list, keys in `current.huggingface.models` are the repo ids; with a dict, keys are your aliases.
- **`metadata_only`**: if `True`, the Hugging Face API is used to fetch model metadata only (no full download). Use `current.huggingface.model_info["alias"]` for the `ModelInfo` object.
- **`lazy`**: defaults to **`True`**. When `True`, each model is resolved on first key access (good when you list several models but only use some per run). When **`False`**, every model is prefetched before your step function starts. Iterating `values()` or `dict(current.huggingface.models)` still touches every entry when `lazy=True`.
- **`local_dir`**: optional parent directory for snapshots for this step. If unset, the **`HUGGINGFACE_LOCAL_DIR`** config (see table below) is used when set; otherwise downloads go under **`<task temp>/metaflow_huggingface`**.

## Authentication

By default the decorator uses the **`env`** auth provider: it reads `HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN` from the environment (first non-empty wins). For private or gated models, set one of these before running the flow.

To use a **custom** backend (vault, internal token service, etc.), subclass **`HuggingFaceAuthProvider`** (see `metaflow/plugins/huggingface/auth.py`), set a unique `TYPE`, and register the class via **`HF_AUTH_PROVIDERS_DESC`** (usually from a Metaflow extension package). Then set `HUGGINGFACE_AUTH_PROVIDER` / `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to that id. **This repository** registers only the built-in **`env`** provider; anything else is supplied by your organization’s plugins.

## Configuration

| Setting | Environment variable | Description |
|--------|----------------------|-------------|
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | Optional Hugging Face base URL if not using the default `https://huggingface.co` (e.g. on-prem). |
| `HUGGINGFACE_LOCAL_DIR` | `METAFLOW_HUGGINGFACE_LOCAL_DIR` | Optional parent directory for downloaded model snapshots. Default: `<task temp>/metaflow_huggingface`. Override per step with `@huggingface(local_dir=...)`. |

## Example in the repository

See `metaflow/plugins/huggingface/example_flow.py` for a minimal runnable flow.

## Demo

The [demos/huggingface/](../demos/huggingface/) directory contains **`run_huggingface_demo.py`**, a small argparse CLI that exercises the decorator. **How to run it, defaults, the test matrix, and troubleshooting** are documented in **[demos/huggingface/README.md](../demos/huggingface/README.md)** (not duplicated here).

Quick smoke test from the repository root (after `pip install -e .` or `export PYTHONPATH="$PWD"`—see [demos/huggingface/README.md](../demos/huggingface/README.md)):

```bash
python demos/huggingface/run_huggingface_demo.py run --help
```
