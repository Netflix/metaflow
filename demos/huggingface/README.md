# Hugging Face decorator demo

Runnable flows and **`run_huggingface_demo.py`** (argparse CLI). The **[docs/huggingface.md](../../docs/huggingface.md)** **Demo** section documents **defaults**, the **“How to test each use case”** table (commands and what they exercise), **`--auth public`** vs **`--auth env`** (both use the standard `env` auth provider in Metaflow; the flag only changes built-in repos when you omit `--model`), and **using your own models**.

Quick start from repo root (defaults: public Hub metadata, no token):

```bash
./demos/huggingface/run_huggingface_demo.sh run --help
./demos/huggingface/run_huggingface_demo.sh run
```

Pass flags after `run`. Run **`run --help`** for the epilog (built-in repo ids and behavior).
