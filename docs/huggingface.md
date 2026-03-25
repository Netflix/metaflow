# Hugging Face Hub integration (`@huggingface`)

The `@huggingface` step decorator declares models your step needs from the [Hugging Face Hub](https://huggingface.co). Metaflow downloads them (or can fetch metadata only) before your step runs and exposes paths or `ModelInfo` on `current.huggingface`.

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
- **`metadata_only`**: if `True`, the Hub API is used to fetch model metadata only (no full download). Use `current.huggingface.model_info["alias"]` for the `ModelInfo` object.

## Authentication

By default the decorator uses the **`env`** auth provider: it reads `HF_TOKEN` or `HUGGING_FACE_HUB_TOKEN` from the environment. For private or gated models, set one of these before running the flow.

You can select a different provider with configuration (see below). Organizations can register **custom** auth providers via Metaflow’s plugin system (`HF_AUTH_PROVIDERS_DESC`); see `metaflow/plugins/huggingface/auth.py`.

## Configuration

| Setting | Environment variable | Description |
|--------|----------------------|-------------|
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | Optional Hub base URL if not using the default `https://huggingface.co` (e.g. on-prem Hub). |

## Example in the repository

See `metaflow/plugins/huggingface/example_flow.py` and `run_huggingface_demo.py` at the repo root for runnable examples.
