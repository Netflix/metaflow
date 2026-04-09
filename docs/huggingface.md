# Hugging Face integration (`@huggingface`)

The `@huggingface` step decorator declares models your step needs from [Hugging Face](https://huggingface.co). It resolves authentication before your step runs, then exposes paths or `ModelInfo` on `current.huggingface`. **By default**, each repo is downloaded (or metadata is fetched) **on first access** to `current.huggingface.models[key]` or `model_info[key]`, so you only pay for models you actually use. Pass `lazy=False` if you want every listed model prefetched in full before your step body runs.

## Installation

```bash
pip install "metaflow[huggingface]"
```

This pulls in `huggingface_hub` alongside Metaflow.

## Usage

### Download snapshots (`metadata_only=False`)

Use `current.huggingface.models[alias]` for local paths after the Hub snapshot is downloaded:

```python
from metaflow import FlowSpec, step, huggingface, current


class HFModelsFlow(FlowSpec):
    @huggingface(
        models={
            "llama": "meta-llama/Llama-2-7b@main",
            "bert": "google-bert/bert-base-uncased",
        },
        metadata_only=False,
        lazy=True,  # default: resolve each model on first key access
        # local_dir="/path/to/parent",  # optional; see Configuration > Default download location
    )
    @step
    def train(self):
        llama_path = current.huggingface.models["llama"]
        # bert_path = current.huggingface.models["bert"]  # fetched only if you access this key
        self.next(self.end)

    @step
    def end(self):
        pass
```

You can pass **models** as a **list** of repo strings instead of a dict; keys on `current.huggingface.models` are then the repo ids (see [Minimal example flow](#minimal-example-flow)).

### Metadata only (`metadata_only=True`)

No full download; use `current.huggingface.model_info[alias]` for Hub metadata:

```python
@huggingface(
    models={"probe": "openai-community/gpt2"},
    metadata_only=True,
)
@step
def inspect(self):
    info = current.huggingface.model_info["probe"]
    print(info.id, getattr(info, "sha", ""))
```

### Parameters

- **models**: a **list** of repo specs (`"org/model"` or `"org/model@revision"`) or a **dict** mapping your alias to a repo spec. With a list, keys in `current.huggingface.models` are the repo ids; with a dict, keys are your aliases.
- **metadata_only**: if `True`, only Hub metadata is fetched. Use `current.huggingface.model_info["alias"]` for the `ModelInfo` object. If `False`, use `current.huggingface.models["alias"]` for local paths.
- **lazy**: defaults to `True` (resolve each model on first key access). If `False`, every listed model is prefetched in `task_pre_step` before your step body runs.
- **local_dir**: optional **parent** directory for snapshots (each model gets a subdirectory derived from repo and revision). If omitted, resolution order is: this argument → `METAFLOW_HUGGINGFACE_LOCAL_DIR` / `HUGGINGFACE_LOCAL_DIR` → [`<task temp>/metaflow_huggingface`](#default-download-location).

> **Note:** Auth tokens and environment variables are covered under [Configuration](#configuration).

## Configuration

### Authentication

By default the decorator uses the **env** auth provider: it reads `HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN` from the environment (first non-empty wins). For private or gated models, set one of these before running the flow.

For a **custom** backend (vault, internal token service, etc.), subclass `HuggingFaceAuthProvider` ([`auth.py`](../metaflow/plugins/huggingface/auth.py)), set a unique `TYPE`, and register the class via `HF_AUTH_PROVIDERS_DESC` (usually from a Metaflow extension package). Then set `HUGGINGFACE_AUTH_PROVIDER` / `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to that id. **This repository** ships only the built-in **env** provider; other providers come from your organization’s plugins.

### Default download location

If you do not set `local_dir` or `METAFLOW_HUGGINGFACE_LOCAL_DIR`, files go under a directory named `metaflow_huggingface` inside the step’s temporary root.

**`<task temp>`** means **`current.tempdir`**: Metaflow’s temp directory for the task, typically from **`METAFLOW_TEMPDIR`** (default **`.`**, the process working directory on a typical local run). The plugin uses:

`os.path.join(current.tempdir or "/tmp", "metaflow_huggingface")`.

To set the parent path, use **`METAFLOW_TEMPDIR`**, **`METAFLOW_HUGGINGFACE_LOCAL_DIR`**, or **`@huggingface(local_dir=...)`**.

### Environment variables

| Setting | Environment variable | Description |
| ------- | -------------------- | ----------- |
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | Optional Hugging Face base URL if not using `https://huggingface.co` (e.g. on-prem). |
| `HUGGINGFACE_LOCAL_DIR` | `METAFLOW_HUGGINGFACE_LOCAL_DIR` | Optional parent directory for snapshots. Default: [`<task temp>/metaflow_huggingface`](#default-download-location). Override per step with `@huggingface(local_dir=...)`. |

## Examples

Optional ways to exercise the decorator from a repository checkout (after `pip install -e .` or `PYTHONPATH=.`).

### Minimal flow

[`example_flow.py`](../metaflow/plugins/huggingface/example_flow.py) defines **HuggingFaceExampleFlow**: one `@huggingface` step using the **list** form of `models` (`["bert-base-uncased"]`), stores the downloaded path on `self`, and prints it.

```bash
PYTHONPATH=. python -m metaflow.plugins.huggingface.example_flow run
```

### Demo via CLI

The [demo directory](../demos/huggingface/) contains [run_huggingface_demo.py](../demos/huggingface/run_huggingface_demo.py): a `run` subcommand that runs a small flow and lets you toggle fetch mode, lazy vs prefetch, download directory, and built-in public vs private default repos. Hub models and aliases are defined in code (`_build_flow_class` / `_default_model_pairs`), not on the CLI.

```bash
python demos/huggingface/run_huggingface_demo.py run --help
```

Details: **[demos/huggingface/README.md](../demos/huggingface/README.md)**.
