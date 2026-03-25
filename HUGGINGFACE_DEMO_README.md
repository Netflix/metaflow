# HuggingFace decorator demo

This demo runs the `@huggingface` decorator with three modes: public metadata, private download, or private metadata-only. See [docs/huggingface.md](docs/huggingface.md) for full documentation.

**CLI:** `./run_huggingface_demo.sh run [none|download|env]`. Default is `none`.

## Prerequisites

```bash
pip install -e .
pip install "metaflow[huggingface]"
```

Optional venv:

```bash
PYTHON_PATH=.venv/bin/python ./run_huggingface_demo.sh run none
```

---

## Case 1: Public model (`none`, default)

Metadata only for `openai-community/gpt2@main`. No token.

```bash
./run_huggingface_demo.sh run
```

---

## Case 2: Private model – full download (`download`)

Downloads `netflix/my-gpt2@main`. Requires `HF_TOKEN` (or `HUGGING_FACE_HUB_TOKEN`) with repo access.

```bash
export HF_TOKEN=hf_xxxxxxxx
./run_huggingface_demo.sh run download
```

---

## Case 3: Private model – metadata only (`env`)

Metadata only for `netflix/my-gpt2@main`. Requires `HF_TOKEN`.

```bash
export HF_TOKEN=hf_xxxxxxxx
./run_huggingface_demo.sh run env
```

---

## Summary

| Mode     | Model                 | Auth        | Command                          |
|----------|----------------------|-------------|----------------------------------|
| none     | openai-community/gpt2 | none        | `./run_huggingface_demo.sh run`  |
| download | netflix/my-gpt2      | HF_TOKEN    | `./run_huggingface_demo.sh run download` |
| env      | netflix/my-gpt2      | HF_TOKEN    | `./run_huggingface_demo.sh run env` |

---

## Troubleshooting

- **401 / 404:** Confirm the token has read access to the repo. Enterprise vs open-source is determined by the token, not a separate endpoint (unless you use `METAFLOW_HUGGINGFACE_ENDPOINT` for a custom Hub host).
