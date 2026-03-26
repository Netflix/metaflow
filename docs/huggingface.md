# Hugging Face integration (`@huggingface`)

## Background

Metaflow did not previously define a standard place for setting [Hugging Face](https://huggingface.co) credentials.
Teams would need to reinvent auth for each flow. 
That is especially awkward when an organization must use vaults, cloud secret stores, IAM or workload identity, 
rotation, or batch workers that never expose a plain `HF_TOKEN`.

This integration addresses that in two ways:

- Pluggable authentication through `HuggingFaceAuthProvider`, so each organization can supply tokens from its own 
systems without changing flow code.
- The same `@huggingface` usage from local runs through production: OSS-compatible, and when infrastructure changes
you adjust configuration and extensions rather than editing the flow.

## Behavior

The `@huggingface` step decorator runs during `task_pre_step`, before your step function. 
It resolves the [Hugging Face](https://huggingface.co) repos you listed, asks the configured auth provider for a token, then either 
downloads each repo snapshot into the task temp directory or, if you set `metadata_only=True`, calls the Hub API for 
metadata only.
Your step reads the outcome from `current.huggingface`.
All access uses `huggingface_hub` (`snapshot_download`, `HfApi`).

You pass a required `models` argument: a non-empty list of strings (`org/model` or `org/model@revision`, with revision
defaulting to `main` when omitted), or a dict mapping an alias string to one of those spec strings. 
By default (`metadata_only=False`), each snapshot is written under `metaflow_huggingface/<repo>_<revision>/` beneath the
task temp directory (with `/` in the repo id normalized for the path).
Use `current.huggingface.models[key]` for the local directory path. 
If you set `metadata_only=True`, no files are downloaded; use `current.huggingface.model_info[key]` to get the Hub 
`ModelInfo` for each entry.

The default auth provider is `env`: it returns the first non-empty value among `HF_TOKEN`, `HUGGING_FACE_TOKEN`, and 
`HUGGING_FACE_HUB_TOKEN`.
If none are set, requests are unauthenticated, which is only appropriate for public repos. 

To use a cusom backend, configure `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` and register a provider through 
`HF_AUTH_PROVIDERS_DESC` in a `metaflow_extensions` package; see [Register a custom auth provider](#register-a-custom-auth-provider). 

If the Hub HTTP API is not hosted at `https://huggingface.co`, set `METAFLOW_HUGGINGFACE_ENDPOINT`.

`current.huggingface` is populated only on steps that carry `@huggingface`. 
Failures from this path are raised as `MetaflowException` with an `@huggingface:` prefix (for example missing `huggingface_hub`, auth failure, or Hub errors).

## Installation

Install the optional extra so `huggingface_hub` is available:

```bash
pip install "metaflow[huggingface]"
```

## Using the decorator

The decorator stacks above `@step`. Declare which models the step needs, then read paths or metadata from `current.huggingface` inside that step only.

```python
from metaflow import FlowSpec, step, huggingface, current

class MyFlow(FlowSpec):
    @huggingface(
        models={"llama": "meta-llama/Llama-2-7b@main"},
        metadata_only=False,
    )
    @step
    def train(self):
        path = current.huggingface.models["llama"]
        self.next(self.end)

    @step
    def end(self):
        pass
```

The `models` shape and semantics match [Behavior](#behavior). Set `metadata_only=True` when you only need
`current.huggingface.model_info` and no download.

A minimal example flow lives at `metaflow/plugins/huggingface/example_flow.py`.

## How it works

Code lives under `metaflow/plugins/huggingface/` and registers like other step decorators
(`metaflow/plugins/__init__.py`).

At flow load, `step_init` validates `models` and builds an internal map from each user-facing key (repo id or alias) to `(repo_id, revision)`. When the task runs, `task_pre_step` runs before your code. It resolves the `hf_auth_provider` plugin (default `env`), calls `get_token()` once for the task, reads optional `HUGGINGFACE_ENDPOINT` from config, then for each model either invokes `snapshot_download` into the task temp tree described above or `HfApi.model_info` when `metadata_only=True`. It attaches a `HuggingFaceContext` to `current.huggingface`. Custom providers subclass `HuggingFaceAuthProvider` in `auth.py`; the stock env implementation is `EnvHuggingFaceAuthProvider` in `env_auth_provider.py`.

The step body runs only after that setup. You need `huggingface_hub` at runtime (`metaflow[huggingface]`).

## Configuration

The table below is the authoritative mapping between Metaflow config keys and environment variables for this decorator.

| Config key | Environment variable | Purpose |
|------------|----------------------|---------|
| `HUGGINGFACE_AUTH_PROVIDER` | `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` | Auth provider id (default: `env`). |
| `HUGGINGFACE_ENDPOINT` | `METAFLOW_HUGGINGFACE_ENDPOINT` | API base if not `https://huggingface.co`. |

### Register a custom auth provider

Use the built-in `env` provider when you can set `HF_TOKEN` or the alternate variable names before
`python myflow.py run`. 

Add a custom provider when the token cannot be a normal environment variable.
For example, due to a policy, a vault or cloud secret store, IAM or workload identity, rotation through another API, or 
environments that never inject `HF_TOKEN`. 

The idea is that you implement a small class whose `get_token()` method returns either a string, `None` for 
unauthenticated access (public repos only), or raises an exception if the step must fail. 
Flow source does not import the provider; you select it with `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` and ship a 
`metaflow_extensions` package on the runtime where Metaflow executes.

Here are the steps:

1. Subclass `HuggingFaceAuthProvider` and set a short id on the class as `TYPE` (for example `acme-vault`).

   ```python
   from metaflow.plugins.huggingface import HuggingFaceAuthProvider

   class AcmeVaultProvider(HuggingFaceAuthProvider):
       TYPE = "acme-vault"

       def get_token(self):
           return fetch_from_your_backend()  # str, or None for public-only
   ```

2. Implement `get_token()`. Metaflow calls it once per task before any download or `model_info` call. Return a token
string, or `None` for no authentication. On unrecoverable errors, raise an exception; Metaflow wraps failures as
`MetaflowException` with a `@huggingface:` prefix.

3. Register the class from `metaflow_extensions.<name>/plugins/` by exporting `HF_AUTH_PROVIDERS_DESC` from a loaded 
module (for example `plugins/__init__.py` or `mfextinit_*.py`).
Each entry is `(name, class_path)`: `name` must match `TYPE`, and `class_path` is relative to that `plugins` package,
for example `".hf_auth.AcmeVaultProvider"` for module `hf_auth`, class `AcmeVaultProvider` (same pattern as 
`metaflow/plugins/__init__.py`).

   ```text
   metaflow_extensions/acme/
     plugins/
       __init__.py
       hf_auth.py
   ```

   ```python
   # plugins/__init__.py
   HF_AUTH_PROVIDERS_DESC = [
       ("acme-vault", ".hf_auth.AcmeVaultProvider"),
   ]
   ```

4. Install the extension everywhere the flow runs, like any other dependency.

5. Select the provider at runtime with `export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=acme-vault`, or set
`HUGGINGFACE_AUTH_PROVIDER` in Metaflow config.
Until you do, `env` remains the active provider.

6. (Optional.) If your deployment sets the allowlist `ENABLED_HF_AUTH_PROVIDER` in Metaflow config or
`METAFLOW_ENABLED_HF_AUTH_PROVIDER` in the environment for `hf_auth_provider` plugins, include your provider id (and
`env` if you rely on it). If the allowlist is unset, every registered provider is eligible to load.

Do not import the provider from flow code; only decorate the step with `@huggingface`. Further reference implementations
live in `metaflow/plugins/huggingface/auth.py` and the core `env` registration lives in `metaflow/plugins/__init__.py`.

## Repository examples

- `metaflow/plugins/huggingface/example_flow.py` — minimal flow using the decorator.
- [Multi-mode demo](../demos/huggingface/README.md) — `run_huggingface_demo.py` and `run_huggingface_demo.sh` for local smoke tests.

## Troubleshooting and FAQ

The sections below cover common Hub errors, configuration, and how this decorator interacts with the rest of Metaflow.

### HTTP 401 or 404 from Hugging Face

These usually mean the token is missing, not valid for that account, or not allowed to read a repo listed in `models`.
Check that the repo id matches a model your credentials can access; private and gated repos need a token with read
access.

### Private or gated repositories

With the default `env` provider, set `HF_TOKEN`, `HUGGING_FACE_TOKEN`, or `HUGGING_FACE_HUB_TOKEN` before the run.
If the token cannot live in the environment, use the steps in [Register a custom auth provider](#register-a-custom-auth-provider).

### API host is not `https://huggingface.co`

Set `METAFLOW_HUGGINGFACE_ENDPOINT` to your deployment’s base URL.
More details provided in [Configuration](#configuration).

### Frequently asked questions

#### Why does the step raise an import error for `huggingface_hub`?

Install the optional extra: `pip install "metaflow[huggingface]"` (see [Installation](#installation)).

#### Why is `current.huggingface` invalid in my step?

The binding exists only on steps decorated with `@huggingface`.
Other steps use the rest of Metaflow’s `current` API as usual.

#### Do I need `METAFLOW_HUGGINGFACE_ENDPOINT` for Hugging Face Enterprise?

Enterprise versus public access is determined by the token.
Set `METAFLOW_HUGGINGFACE_ENDPOINT` only when the HTTP API is hosted somewhere other than `https://huggingface.co` (for
example on-prem).

#### What is `METAFLOW_ENABLED_HF_AUTH_PROVIDER`?

It is the generic Metaflow allowlist for `hf_auth_provider` plugins, consistent with other plugin categories.
If it is unset, every registered auth provider can load.
If it is set, it must list your provider id (for example `env` and your custom `TYPE`) or that provider will not load.
See `process_plugins()` in `metaflow/extension_support/plugins.py` and step 6 under
[Register a custom auth provider](#register-a-custom-auth-provider).
