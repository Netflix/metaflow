# `@huggingface` demo (CLI)

## Purpose

This demo is a small runnable flow so you can **try the `@huggingface` decorator** and **switch between behaviors** (metadata vs download, lazy vs prefetch, download location, the two-model lazy example, and the built-in public vs private default repos) without writing your own flow first.

What each **decorator** option means in general—`models`, `metadata_only`, `lazy`, `local_dir`, auth, and configuration—is documented in the main Hugging Face integration doc: **[docs/huggingface.md](../../docs/huggingface.md)**. This README focuses on **how to run the demo**, **what the CLI toggles**, and **where to edit code** (for example Hub repo ids).

The implementation lives entirely in `**[run_huggingface_demo.py](run_huggingface_demo.py)`**. Follow the functions `**_default_model_pairs**`, `**_build_flow_class**`, `**_configure_auth_env**`, and `_execute_hf_demo` there.

## Customizing Hub repos and the flow

**Hub repos and aliases are not configurable from the CLI.** They come from `**_default_model_pairs`** and the constants at the top of `[run_huggingface_demo.py](run_huggingface_demo.py)`. Change those helpers or `**_build_flow_class**` if you want different `models` or step logic.

## Prerequisites

From the repo root: `pip install -e .` and `pip install "metaflow[huggingface]"`. Then:

```bash
python demos/huggingface/run_huggingface_demo.py run --help
```

## What each demo CLI flag does

These flags only map demo arguments into the flow built in `[run_huggingface_demo.py](run_huggingface_demo.py)`. For the meaning of the underlying `@huggingface` parameters, see **[docs/huggingface.md](../../docs/huggingface.md)**.


| Flag                          | Effect in this demo                                                                                                                                         |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--auth {public,env}`         | Chooses which built-in pair `_default_model_pairs` returns (`public` vs private example). Sets `METAFLOW_HUGGINGFACE_AUTH_PROVIDER` to `env` in both cases. |
| `--fetch {metadata,download}` | Sets `metadata_only` on the decorator: Hub metadata only vs full snapshot to disk.                                                                          |
| `--prefetch`                  | Sets `lazy=False` (resolve every listed model in `task_pre_step` before the step body).                                                                     |
| `--use-demo-cache`            | For `--fetch download` only: sets `local_dir` to `demos/huggingface/.demo_hf_cache`.                                                                        |
| `--local-dir PATH`            | Sets `local_dir` for downloads; overrides `--use-demo-cache` when both are set.                                                                             |
| `--only-read-first-model`     | Uses the two-model public built-in pair; step only reads `model_info` for the first alias (`--auth public` and `--fetch metadata` required).                |


## How to test each use case

Run from the **repository root**.


| #   | What we are testing                                                                                                                                                              | Command (minimal)                                                                                                        | Notes                                                                                                                                                                   |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Public model, metadata only** — `@huggingface` with `metadata_only=True`, default `lazy=True`, one Hub `model_info` fetch when you read the key.                               | `python demos/huggingface/run_huggingface_demo.py run`                                                                   | Built-in: `gpt2` → `openai-community/gpt2@main`. No token.                                                                                                              |
| 2   | **Lazy behavior with two models** — two repos on `@huggingface`, step **only** reads `model_info` for the **first** key so the second repo is never contacted while `lazy=True`. | `python demos/huggingface/run_huggingface_demo.py run --only-read-first-model`                                           | Built-in pair: `used` / `not_accessed` (gpt2 + bert-base). Requires `--auth public --fetch metadata`. Conflicts with `--prefetch`.                                      |
| 3   | **Private model, full download** — `snapshot_download` into task temp (or default `local_dir`), `lazy=True` unless `--prefetch`.                                                 | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download`                  | Built-in private example: `gpt2` → `netflix/my-gpt2@main` (may not exist in your Hub account—edit `_default_model_pairs` / constants for a repo your token can access). |
| 4   | **Prefetch before step** — `lazy=False`: every listed model resolved in `task_pre_step` before the step body.                                                                    | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download --prefetch`       | Same built-in private example as #3.                                                                                                                                    |
| 5   | **Explicit download parent** — `@huggingface(local_dir=...)` via `--use-demo-cache` (gitignored `.demo_hf_cache` here).                                                          | `export HF_TOKEN=… && python demos/huggingface/run_huggingface_demo.py run --auth env --fetch download --use-demo-cache` | Overrides default `<temp>/metaflow_huggingface` for the parent directory.                                                                                               |
| 6   | **Custom download directory** — same as #5 but any path.                                                                                                                         | `… run --auth env --fetch download --local-dir /path/to/parent`                                                          | `--local-dir` wins over `--use-demo-cache` if both are passed (see `--help`).                                                                                           |


`**--auth public` vs `--auth env` (demo only):** Both select the same auth provider id; they only change which **built-in** repos `_default_model_pairs` uses and whether the shipped private default expects a token.

## Troubleshooting

- **401 / 404:** Confirm the token has read access to the repo. Enterprise vs open-source is determined by the token, not a separate endpoint (unless you use `METAFLOW_HUGGINGFACE_ENDPOINT` for a custom Hugging Face host).
- **Built-in `netflix/my-gpt2` missing:** That id is an example used in some forks. Edit `_default_model_pairs` / constants in `[run_huggingface_demo.py](run_huggingface_demo.py)`, or use `--auth public` with the shipped public defaults.
- `**ModuleNotFoundError: metaflow`:** Fix [Prerequisites](#prerequisites) (install editable or set `PYTHONPATH`).

