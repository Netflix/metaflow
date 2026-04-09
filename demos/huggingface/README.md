# `@huggingface` demo (CLI)

This directory contains **`run_huggingface_demo.py`**, an argparse-driven flow that exercises the decorator.

## Prerequisites (pick one)

**Repository root** = the directory that contains the `metaflow/` package (clone root).

1. **Editable install (simplest for repeated runs):** from the repository root, install Metaflow in your environment. After that, `python` finds `metaflow` without any path hack:

   ```bash
   pip install -e .
   pip install "metaflow[huggingface]"
   ```

2. **No install — set `PYTHONPATH` once per shell:** from the repository root:

   ```bash
   export PYTHONPATH="$PWD"
   ```

   Every command below assumes you already did **(1)** or **(2)** and your current directory is still the **repository root** (unless noted). You do **not** need to prefix each line with `PYTHONPATH=.`.

Quick check:

```bash
python demos/huggingface/run_huggingface_demo.py run --help
python demos/huggingface/run_huggingface_demo.py run
```

Pass flags after `run`. Run **`run --help`** for the epilog (built-in repo ids and behavior).

Integration reference (installation, API, configuration, custom auth): **[docs/huggingface.md](../../docs/huggingface.md)**.

**This branch** also registers **`vendor-token`** and the demo accepts **`--auth vendor`** for Netflix internal token retrieval. See **`docs/netflix/NETFLIX_HUGGINGFACE_VENDOR_TOKEN.md`**. Example:

```bash
python demos/huggingface/run_huggingface_demo.py run --auth vendor --fetch metadata
```

The OSS PR branch documents only **`public`** and **`env`** in this README; **`vendor`** exists only here.

## Defaults (no extra flags)

| Option | Default | Meaning |
|--------|---------|---------|
| `--auth` | `public` | Hugging Face Hub **public** repos only; no `HF_TOKEN` required for the built-in model. |
| `--fetch` | `metadata` | Hub **metadata only** (`metadata_only=True`); no snapshot download. |
| `@huggingface` `lazy` | `True` | Each model is resolved on **first access** to `current.huggingface.model_info[...]` (unless you pass `--prefetch`). |
| Models | Built-in | One public model: `openai-community/gpt2@main` under alias `gpt2`. |

So **`python demos/huggingface/run_huggingface_demo.py run`** is a safe, token-free smoke test for metadata on a small public model.

## How to test each use case

Commands use **repository root** as the working directory (and prerequisites above).

| # | What we are testing | Command (minimal) | Notes |
|---|---------------------|-------------------|-------|
| 1 | **Public model, metadata only** — `@huggingface` with `metadata_only=True`, default **`lazy=True`**, one Hub `model_info` fetch when you read the key. | `python demos/huggingface/run_huggingface_demo.py run` | Built-in: `gpt2` → `openai-community/gpt2@main`. No token. |
| 2 | **Lazy behavior with two models** — two repos on `@huggingface`, step **only** reads `model_info` for the **first** key so the second repo is never contacted while `lazy=True`. | `python demos/huggingface/run_huggingface_demo.py run --only-read-first-model` | Built-in pair: `used` / `not_accessed` (gpt2 + bert-base). Requires `--auth public --fetch metadata`. Conflicts with `--prefetch`. |
| 3 | **Private model, full download** — `snapshot_download` into task temp (or default `local_dir`), **`lazy=True`** unless `--prefetch`. | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download` | Built-in private example: `gpt2` → `netflix/my-gpt2@main` (may not exist in your Hub account—use `--model` with a repo your token can access). |
| 4 | **Prefetch before step** — `lazy=False`: every listed model resolved in **`task_pre_step`** before the step body. | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download --prefetch` | Same built-in private example as #3. |
| 5 | **Explicit download parent** — `@huggingface(local_dir=...)` via **`--use-demo-cache`** (gitignored `.demo_hf_cache` here). | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download --use-demo-cache` | Overrides default `<temp>/metaflow_huggingface` for the parent directory. |
| 6 | **Custom download directory** — same as #5 but any path. | `… run --auth env --fetch download --local-dir /path/to/parent` | `--local-dir` wins over `--use-demo-cache` if both are passed (see `--help`). |

**`--auth public` vs `--auth env` (demo only):** For those two flags, the CLI sets `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to **`env`**; the difference is which **built-in** repos are used when you omit `--model`. **`--auth vendor`** sets the provider to **`vendor-token`** (this branch only). **`--fetch {metadata,download}`** maps to decorator `metadata_only` vs full download. **`--prefetch`** sets **`lazy=False`**.

## Using your own models

Do **not** rely on the built-in `netflix/my-gpt2` or public defaults when you want your own repos.

1. Pass one or more **`--model KEY=SPEC`** arguments. **`SPEC`** is `org/model` or `org/model@revision` (same as `@huggingface` `models`).
2. Choose **`--fetch metadata`** or **`--fetch download`**.
3. Choose **`--auth public`** only if every repo is readable **without** a token. For private or gated models, use **`--auth env`** and set `HF_TOKEN` / `HUGGING_FACE_HUB_TOKEN`.
4. If you pass **`--model`**, the demo **does not** require `HF_TOKEN` for `--auth env` automatically—you are expected to supply credentials that match your repos.

```bash
# Your public model, metadata only, no token
python demos/huggingface/run_huggingface_demo.py run \
  --auth public --fetch metadata --model m=distilbert-base-uncased

# Your private org repo (token required)
export HF_TOKEN=hf_xxxxxxxx
python demos/huggingface/run_huggingface_demo.py run \
  --auth env --fetch download --model mine=my-org/my-model@main
```

**`--only-read-first-model`:** Same as **row #2** above: pass **two** `--model KEY=SPEC` lines (first = the one the step reads), or omit `--model` for the built-in two-model public pair.

## Optional: dedicated venv

If you use a virtualenv, point **`python`** at it (still from repository root, with prerequisites above):

```bash
.venv/bin/python demos/huggingface/run_huggingface_demo.py run --help
```

## Running unit tests (same checks as CI-style local runs)

Use the same [Prerequisites](#prerequisites-pick-one) as for the demo (`pip install -e .` is enough for imports). From **repository root**:

```bash
cd test/core
python -m unittest \
  tests.huggingface_decorator.TestHuggingFaceParsing \
  tests.huggingface_decorator.TestCurrentHuggingFaceSentinel \
  tests.huggingface_decorator.TestEnvHuggingFaceAuthProvider \
  tests.huggingface_decorator.TestLazyRepoMap \
  tests.huggingface_decorator.TestResolveLocalDirBase \
  -v
```

Integration test (requires Metaflow test harness):

```bash
cd test/core
python run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest
```

If imports fail and you are **not** using an editable install, `cd` back to the repository root, run `export PYTHONPATH="$PWD"`, then repeat the `cd test/core` block.

## Troubleshooting

- **401 / 404:** Confirm the token has read access to the repo. Enterprise vs open-source is determined by the token, not a separate endpoint (unless you use `METAFLOW_HUGGINGFACE_ENDPOINT` for a custom Hugging Face host).
- **Built-in `netflix/my-gpt2` missing:** That id is an example used in some forks. Point `--model` at a private repo your `HF_TOKEN` can read, or use `--auth public` with a public model.
- **`ModuleNotFoundError: metaflow`:** You skipped [Prerequisites](#prerequisites-pick-one)—run `pip install -e .` from the repository root or `export PYTHONPATH="$PWD"` there before calling `python`.
