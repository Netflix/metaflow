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

You can select a different provider with configuration (see below). Organizations can register **custom** auth providers via Metaflow’s plugin system (`HF_AUTH_PROVIDERS_DESC`); see `metaflow/plugins/huggingface/auth.py`.

## Configuration

| Setting | Environment variable | Description |
|--------|----------------------|-------------|
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | Optional Hugging Face base URL if not using the default `https://huggingface.co` (e.g. on-prem). |
| `HUGGINGFACE_LOCAL_DIR` | `METAFLOW_HUGGINGFACE_LOCAL_DIR` | Optional parent directory for downloaded model snapshots. Default: `<task temp>/metaflow_huggingface`. Override per step with `@huggingface(local_dir=...)`. |

## Example in the repository

See `metaflow/plugins/huggingface/example_flow.py` for a minimal runnable flow.

## Demo

The [demos/huggingface/](../demos/huggingface/) directory provides **`run_huggingface_demo.py`**, an **argparse**-driven CLI. The shell wrapper forwards arguments:

```bash
./demos/huggingface/run_huggingface_demo.sh run --help
# same as:
PYTHONPATH=<repo-root> python demos/huggingface/run_huggingface_demo.py run --help
```

### Defaults (no arguments)

If you run **`run`** with no extra flags, the demo uses:

| Option | Default | Meaning |
|--------|---------|---------|
| `--auth` | `public` | Hugging Face Hub **public** repos only; no `HF_TOKEN` required for the built-in model. |
| `--fetch` | `metadata` | Hub **metadata only** (`metadata_only=True`); no snapshot download. |
| `@huggingface` `lazy` | `True` | Each model is resolved on **first access** to `current.huggingface.model_info[...]` (unless you pass `--prefetch`). |
| Models | Built-in | One public model: `openai-community/gpt2@main` under alias `gpt2`. |

So **`./demos/huggingface/run_huggingface_demo.sh run`** is a safe, token-free smoke test for metadata on a small public model.

### How to test each use case

The table below is the **authoritative checklist** for what the demo exercises and how to run it. All commands assume repo root; prefix with `PYTHONPATH=.` or use **`./demos/huggingface/run_huggingface_demo.sh run`** so `PYTHONPATH` is set.

| # | What we are testing | Command (minimal) | Notes |
|---|---------------------|-------------------|-------|
| 1 | **Public model, metadata only** — `@huggingface` with `metadata_only=True`, default **`lazy=True`**, one Hub `model_info` fetch when you read the key. | `./demos/huggingface/run_huggingface_demo.sh run` | Built-in: `gpt2` → `openai-community/gpt2@main`. No token. |
| 2 | **Lazy behavior with two models** — two repos declared on `@huggingface`, step **only** calls `current.huggingface.model_info[<first key>]` so the **second repo is never contacted** while `lazy=True`. | `./demos/huggingface/run_huggingface_demo.sh run --only-read-first-model` | Built-in pair: `used` / `not_accessed` (gpt2 + bert-base). **Requires** `--auth public --fetch metadata`. Conflicts with `--prefetch`. |
| 3 | **Private model, full download** — `snapshot_download` into task temp (or default `local_dir`), **`lazy=True`** unless you add `--prefetch`. | `export HF_TOKEN=… && ./demos/huggingface/run_huggingface_demo.sh run --auth env --fetch download` | Built-in: `gpt2` → `netflix/my-gpt2@main`. Needs token with repo access. |
| 4 | **Prefetch before step** — `lazy=False`: every listed model resolved in **`task_pre_step`** before step body. | `export HF_TOKEN=… && ./demos/huggingface/run_huggingface_demo.sh run --auth env --fetch download --prefetch` | Same built-in private repo as #3. |
| 5 | **Explicit download parent** — `@huggingface(local_dir=...)` via **`--use-demo-cache`** (gitignored `demos/huggingface/.demo_hf_cache`). | `export HF_TOKEN=… && ./demos/huggingface/run_huggingface_demo.sh run --auth env --fetch download --use-demo-cache` | Overrides default `<temp>/metaflow_huggingface` for the parent directory. |
| 6 | **Custom download directory** — same as #5 but any path. | `... run --auth env --fetch download --local-dir /path/to/parent` | `--local-dir` wins over `--use-demo-cache` if both are passed (see `--help`). |

**Flags reference:** `--auth {public,env}` sets `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to **`env`** (the demo uses the standard env-based Hugging Face auth provider). `--fetch {metadata,download}` maps to decorator `metadata_only` and download vs metadata-only. `--prefetch` sets **`lazy=False`**.

Custom auth providers (for example a vendor-token integration) are registered via Metaflow’s plugin hooks (`HF_AUTH_PROVIDERS_DESC`); they are not required for the public demo CLI above.

### Using your own models

Do **not** rely on the built-in `netflix/my-gpt2` or public defaults when you want your own repos.

1. Pass one or more **`--model KEY=SPEC`** arguments. **`SPEC`** is `org/model` or `org/model@revision` (same as `@huggingface` `models`).
2. Choose **`--fetch metadata`** or **`--fetch download`**.
3. Choose **`--auth public`** only if every repo is readable **without** a token. For private or gated models, use **`--auth env`** and set `HF_TOKEN` / `HUGGING_FACE_HUB_TOKEN`.
4. If you pass **`--model`**, the demo **does not** require `HF_TOKEN` for `--auth env` automatically—you are expected to supply credentials that match your repos.

Examples:

```bash
# Your public model, metadata only, no token
./demos/huggingface/run_huggingface_demo.sh run \
  --auth public --fetch metadata --model m=distilbert-base-uncased

# Your private org repo (token required)
export HF_TOKEN=hf_xxxxxxxx
./demos/huggingface/run_huggingface_demo.sh run \
  --auth env --fetch download --model mine=my-org/my-model@main
```

**`--only-read-first-model`:** same as **row #2** in [How to test each use case](#how-to-test-each-use-case): pass **two** `--model KEY=SPEC` lines (first = the one the step reads), or omit `--model` for the built-in two-model public pair.

### Prerequisites

```bash
pip install -e .
pip install "metaflow[huggingface]"
```

Optional venv:

```bash
PYTHON_PATH=.venv/bin/python ./demos/huggingface/run_huggingface_demo.sh run --help
```

### Running tests (`test`)

From repo root:

```bash
./demos/huggingface/run_huggingface_demo.sh test
```

### Troubleshooting

- **401 / 404:** Confirm the token has read access to the repo. Enterprise vs open-source is determined by the token, not a separate endpoint (unless you use `METAFLOW_HUGGINGFACE_ENDPOINT` for a custom Hugging Face host).
