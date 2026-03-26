# Hugging Face decorator demo

This directory runs a small flow that exercises `@huggingface` in a few configurations.
Use it to play around with install, auth, and download paths without writing a new flow.
Full details are documented in [docs/huggingface.md](../../docs/huggingface.md).

The shell wrapper `run_huggingface_demo.sh` exposes three modes: `none`, `download`, and `env`.

**Note**: this demo is configured to fetch/download Netflix-internal models.
Update the name of the models set in `run_huggingface_demo.py` for testing your models.


## Commands (repo root)

```bash
./demos/huggingface/run_huggingface_demo.sh run              # public, metadata only (default)
./demos/huggingface/run_huggingface_demo.sh run download   # private full download (token)
./demos/huggingface/run_huggingface_demo.sh run env          # private metadata only (token)
./demos/huggingface/run_huggingface_demo.sh test             # unit tests
```

## Modes

| Mode | Model in script | Behavior |
|------|-----------------|----------|
| `none` (default) | `openai-community/gpt2` | Metadata only, no token |
| `download` | `netflix/my-gpt2` | Full snapshot |
| `env` | `netflix/my-gpt2` | Metadata only (private) |
