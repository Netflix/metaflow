# Hugging Face integration (`@huggingface`)

The `@huggingface` step decorator declares models your step needs from [Hugging Face](https://huggingface.co). Metaflow downloads them (or can fetch metadata only) before your step runs and exposes paths or `ModelInfo` on `current.huggingface`.

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

## Authentication

By default the decorator uses the **`env`** auth provider: it reads `HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN` from the environment (first non-empty wins). For private or gated models, set one of these before running the flow.

You can select a different provider with configuration (see below). Organizations can register **custom** auth providers via Metaflow’s plugin system (`HF_AUTH_PROVIDERS_DESC`); see `metaflow/plugins/huggingface/auth.py`.

## Configuration

| Setting | Environment variable | Description |
|--------|----------------------|-------------|
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | Optional Hugging Face base URL if not using the default `https://huggingface.co` (e.g. on-prem). |

## Example in the repository

See `metaflow/plugins/huggingface/example_flow.py` for a minimal runnable flow.

## Demo

The [demos/huggingface/](../demos/huggingface/) directory has a multi-mode demo (`run_huggingface_demo.py` and `run_huggingface_demo.sh`).

**CLI:** `./demos/huggingface/run_huggingface_demo.sh run [none|download|env]`. Default is `none`.

### Prerequisites

```bash
pip install -e .
pip install "metaflow[huggingface]"
```

Optional venv:

```bash
PYTHON_PATH=.venv/bin/python ./demos/huggingface/run_huggingface_demo.sh run none
```

### Case 1: Public model (`none`, default)

Metadata only for `openai-community/gpt2@main`. No token.

```bash
./demos/huggingface/run_huggingface_demo.sh run
```

### Case 2: Private model – full download (`download`)

Downloads `netflix/my-gpt2@main`. Requires `HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN` with repo access.

```bash
export HF_TOKEN=hf_xxxxxxxx
./demos/huggingface/run_huggingface_demo.sh run download
```

### Case 3: Private model – metadata only (`env`)

Metadata only for `netflix/my-gpt2@main`. Requires a token (`HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN`).

```bash
export HF_TOKEN=hf_xxxxxxxx
./demos/huggingface/run_huggingface_demo.sh run env
```

### Summary

| Mode     | Model                   | Auth      | Command                                                |
|----------|-------------------------|-----------|--------------------------------------------------------|
| none     | openai-community/gpt2   | none      | `./demos/huggingface/run_huggingface_demo.sh run`      |
| download | netflix/my-gpt2         | token env | `./demos/huggingface/run_huggingface_demo.sh run download` |
| env      | netflix/my-gpt2         | token env | `./demos/huggingface/run_huggingface_demo.sh run env`  |

### Running tests (`test`)

From repo root:

```bash
./demos/huggingface/run_huggingface_demo.sh test
```

### Troubleshooting

- **401 / 404:** Confirm the token has read access to the repo. Enterprise vs open-source is determined by the token, not a separate endpoint (unless you use `METAFLOW_HUGGINGFACE_ENDPOINT` for a custom Hugging Face host).
