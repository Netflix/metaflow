#!/usr/bin/env python
"""
CLI demo for the @huggingface decorator (argparse).

From the repository root, after ``pip install -e .`` or ``export PYTHONPATH="$PWD"``
(see demos/huggingface/README.md):

  python demos/huggingface/run_huggingface_demo.py run --help
  python demos/huggingface/run_huggingface_demo.py run

See demos/huggingface/README.md for defaults, the testing table, and "Using your own models".
"""
from __future__ import annotations

import argparse
import os
import sys
from argparse import Namespace
from typing import Dict, List, Optional, Sequence, Tuple

_DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DEMO_CACHE = os.path.abspath(os.path.join(_DEMO_DIR, ".demo_hf_cache"))

# Built-in Netflix private repo (requires HF_TOKEN for download/metadata in practice).
_DEFAULT_PRIVATE_SPEC = "netflix/my-gpt2@main"
# Built-in public models for --auth public.
_DEFAULT_PUBLIC_SPEC = "openai-community/gpt2@main"
_DEFAULT_SECOND_PUBLIC = "google-bert/bert-base-uncased"


def _parse_model_arg(raw: str) -> Tuple[str, str]:
    """Parse 'alias=org/model' or 'alias=org/model@rev'."""
    raw = (raw or "").strip()
    if "=" not in raw:
        raise argparse.ArgumentTypeError(
            "Expected KEY=repo_spec (e.g. myllama=meta-llama/Llama-2-7b@main), got %r"
            % raw
        )
    key, spec = raw.split("=", 1)
    key, spec = key.strip(), spec.strip()
    if not key or not spec:
        raise argparse.ArgumentTypeError(
            "Invalid --model %r: need non-empty key and spec" % raw
        )
    return key, spec


def _models_from_cli(model_args: Sequence[str]) -> List[Tuple[str, str]]:
    return [_parse_model_arg(m) for m in model_args]


def _default_model_pairs(
    auth: str,
    fetch: str,
    only_read_first_model: bool,
) -> List[Tuple[str, str]]:
    if auth == "public":
        if only_read_first_model:
            return [
                ("used", _DEFAULT_PUBLIC_SPEC),
                ("not_accessed", _DEFAULT_SECOND_PUBLIC),
            ]
        return [("gpt2", _DEFAULT_PUBLIC_SPEC)]
    # env or vendor: private demo repo
    if only_read_first_model:
        raise argparse.ArgumentTypeError(
            "--only-read-first-model is only supported with --auth public and --fetch metadata"
        )
    return [("gpt2", _DEFAULT_PRIVATE_SPEC)]


def _resolve_model_list(
    auth: str,
    fetch: str,
    only_read_first_model: bool,
    model_args: Sequence[str],
) -> List[Tuple[str, str]]:
    if model_args:
        pairs = _models_from_cli(model_args)
        if only_read_first_model and len(pairs) != 2:
            raise SystemExit(
                "error: --only-read-first-model requires exactly two --model KEY=SPEC entries "
                "(or omit --model to use the built-in public pair)."
            )
        return pairs
    return _default_model_pairs(auth, fetch, only_read_first_model)


def _validate_env_token_if_needed(
    args: Namespace, model_pairs: List[Tuple[str, str]]
) -> None:
    """Require HF token only for --auth env when using built-in private demo repos."""
    if args.auth != "env":
        return
    if args.model:
        return
    if not (os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")):
        sys.stderr.write(
            "error: --auth env with built-in demo models requires HF_TOKEN or "
            "HUGGING_FACE_HUB_TOKEN. Use --auth public, pass --model with a public repo, "
            "or set a token.\n"
        )
        sys.exit(2)


def _validate_vendor(auth: str) -> None:
    if auth != "vendor":
        return
    url = os.environ.get("METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL") or os.environ.get(
        "HUGGINGFACE_VENDOR_TOKEN_URL"
    )
    if url and str(url).strip():
        return
    try:
        from metaflow.metaflow_config import HUGGINGFACE_VENDOR_TOKEN_URL

        os.environ.setdefault(
            "METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL", HUGGINGFACE_VENDOR_TOKEN_URL
        )
    except Exception:
        pass
    url = os.environ.get("METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL") or os.environ.get(
        "HUGGINGFACE_VENDOR_TOKEN_URL"
    )
    if not url or not str(url).strip():
        sys.stderr.write(
            "error: --auth vendor requires METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL or "
            "HUGGINGFACE_VENDOR_TOKEN_URL (hf-token endpoint).\n"
        )
        sys.exit(2)


def _configure_auth_env(auth: str) -> None:
    if auth == "vendor":
        os.environ["METAFLOW_HUGGINGFACE_AUTH_PROVIDER"] = "vendor-token"
    else:
        os.environ["METAFLOW_HUGGINGFACE_AUTH_PROVIDER"] = "env"


def _build_flow_class(
    model_pairs: List[Tuple[str, str]],
    metadata_only: bool,
    lazy: bool,
    local_dir: Optional[str],
    only_read_first_model: bool,
):
    from metaflow import FlowSpec, step, huggingface, current

    models: Dict[str, str] = {}
    for k, v in model_pairs:
        models[k] = v

    hf_kw = {
        "models": models,
        "metadata_only": metadata_only,
        "lazy": lazy,
    }
    if local_dir is not None:
        hf_kw["local_dir"] = local_dir

    class HuggingFaceDemoFlow(FlowSpec):
        @huggingface(**hf_kw)
        @step
        def start(self):
            if metadata_only:
                if only_read_first_model:
                    keys = list(models.keys())
                    k0 = keys[0]
                    info = current.huggingface.model_info[k0]
                    _print_model_metadata(info)
                    print(
                        "(lazy=True) Listed two models but only read model_info for %r; "
                        "%r was never accessed so it was not fetched." % (k0, keys[1])
                    )
                else:
                    for k in models:
                        print("--- key: %s ---" % k)
                        info = current.huggingface.model_info[k]
                        _print_model_metadata(info)
            else:
                if only_read_first_model:
                    raise AssertionError(
                        "only_read_first_model applies only to metadata fetch"
                    )
                for k in models:
                    path = current.huggingface.models[k]
                    print("%s -> %s" % (k, path))
                if not lazy:
                    print(
                        "(lazy=False) All listed models were resolved in task_pre_step "
                        "before this step."
                    )
                if local_dir is not None:
                    print("local_dir parent:", local_dir)
            self.next(self.end)

        @step
        def end(self):
            print("Done.")

    HuggingFaceDemoFlow.__doc__ = (
        "Dynamic @huggingface demo (see run_huggingface_demo.py --help)."
    )
    return HuggingFaceDemoFlow


def _print_model_metadata(info) -> None:
    print("Model id:", info.id)
    print("Revision (sha):", getattr(info, "sha", "N/A"))
    print("Files (siblings):", len(info.siblings) if info.siblings else 0)


def _run_cmd(args: Namespace) -> None:
    only_read_first_model = args.only_read_first_model
    if only_read_first_model and args.fetch != "metadata":
        sys.stderr.write("error: --only-read-first-model requires --fetch metadata.\n")
        sys.exit(2)
    if only_read_first_model and args.auth != "public":
        sys.stderr.write("error: --only-read-first-model requires --auth public.\n")
        sys.exit(2)
    if only_read_first_model and args.prefetch:
        sys.stderr.write(
            "error: --only-read-first-model demonstrates lazy=True; omit --prefetch.\n"
        )
        sys.exit(2)

    model_pairs = _resolve_model_list(
        args.auth, args.fetch, only_read_first_model, args.model
    )

    _validate_env_token_if_needed(args, model_pairs)
    _validate_vendor(args.auth)
    _configure_auth_env(args.auth)

    metadata_only = args.fetch == "metadata"
    lazy = not args.prefetch

    local_dir: Optional[str] = None
    if args.local_dir is not None:
        local_dir = os.path.abspath(os.path.expanduser(args.local_dir))
    elif args.use_demo_cache:
        if args.fetch != "download":
            sys.stderr.write(
                "error: --use-demo-cache only applies to --fetch download.\n"
            )
            sys.exit(2)
        local_dir = _DEFAULT_DEMO_CACHE

    if args.fetch == "download" and not metadata_only:
        # downloads only: local_dir optional
        pass

    # Import Metaflow after environment is configured for auth plugins.
    flow_cls = _build_flow_class(
        model_pairs,
        metadata_only=metadata_only,
        lazy=lazy,
        local_dir=local_dir,
        only_read_first_model=only_read_first_model,
    )
    flow_cls()


def _build_parser() -> argparse.ArgumentParser:
    epilog = """
defaults (if you run: %(prog)s run with no flags)
  --auth public
  --fetch metadata
  lazy=True for @huggingface (each repo resolved on first access unless --prefetch)
  Models: built-in public gpt2 metadata-only demo (no token).

Using your own models
  Pass one or more --model KEY=SPEC where SPEC is org/model or org/model@revision.
  Examples:
    %(prog)s run --auth public --fetch metadata --model m=distilbert-base-uncased
    %(prog)s run --auth env --fetch download --model my=org/private-model@main
  For a gated/private repo, use --auth env and set HF_TOKEN / HUGGING_FACE_HUB_TOKEN, or
  --auth vendor with METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL (Netflix internal).

Built-in demo repos (when --model is omitted)
  --auth public: openai-community/gpt2@main (with --only-read-first-model, a second public model is also listed).
  --auth env or vendor: %(private)s

Both --auth public and --auth env set METAFLOW_HUGGINGFACE_AUTH_PROVIDER to env; they only
differ in which built-in repos are used when --model is omitted. --auth vendor sets
METAFLOW_HUGGINGFACE_AUTH_PROVIDER to vendor-token (see docs/netflix/NETFLIX_HUGGINGFACE_VENDOR_TOKEN.md).
""" % {
        "prog": os.path.basename(sys.argv[0]),
        "private": _DEFAULT_PRIVATE_SPEC,
    }

    p = argparse.ArgumentParser(
        description="Run the @huggingface decorator demo flow.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run",
        help="Run a one-step demo flow with @huggingface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    run.add_argument(
        "--auth",
        choices=("public", "env", "vendor"),
        default="public",
        help="Built-in demo selection when --model is omitted. public and env use the env "
        "auth provider (HF_TOKEN / HUGGING_FACE_HUB_TOKEN for private defaults); vendor uses "
        "vendor-token and METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL (Netflix internal). "
        "Default: %(default)s.",
    )
    run.add_argument(
        "--fetch",
        choices=("metadata", "download"),
        default="metadata",
        help="metadata = Hub API only; download = snapshot_download to disk. Default: %(default)s.",
    )
    run.add_argument(
        "--prefetch",
        action="store_true",
        help="Set lazy=False so every listed model is resolved in task_pre_step "
        "before the step body (metadata API calls or snapshot downloads).",
    )
    run.add_argument(
        "--use-demo-cache",
        action="store_true",
        help="For --fetch download, set local_dir to demos/huggingface/.demo_hf_cache "
        "(overridden by --local-dir).",
    )
    run.add_argument(
        "--local-dir",
        metavar="PATH",
        default=None,
        help="Parent directory for downloads (same as @huggingface(local_dir=...)). "
        "Default: Metaflow task temp / metaflow_huggingface unless --use-demo-cache.",
    )
    run.add_argument(
        "--only-read-first-model",
        action="store_true",
        dest="only_read_first_model",
        help="List two Hub models on @huggingface but only read model_info for the first "
        "alias (second repo is never fetched while lazy=True). Requires --auth public "
        "--fetch metadata; pass two --model KEY=SPEC or use built-in public pair.",
    )
    run.add_argument(
        "--model",
        action="append",
        dest="model",
        default=[],
        metavar="KEY=SPEC",
        help="Declare a model (repeatable). Example: --model llama=meta-llama/Llama-2-7b@main. "
        "If omitted, built-in demo repos are used (see --help epilog).",
    )
    run.set_defaults(func=_run_cmd)

    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
