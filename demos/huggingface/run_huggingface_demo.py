#!/usr/bin/env python
"""
CLI demo for the @huggingface decorator (argparse).

From the repository root, after ``pip install -e .`` or ``export PYTHONPATH="$PWD"``
(see demos/huggingface/README.md):

  python demos/huggingface/run_huggingface_demo.py run --help
  python demos/huggingface/run_huggingface_demo.py run

See demos/huggingface/README.md for how to run the demo and the test matrix; see docs/huggingface.md for
decorator options (`metadata_only`, `lazy`, `local_dir`, auth, etc.).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from argparse import Namespace
from typing import Dict, List, Optional, Sequence, Tuple

_DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DEMO_CACHE = os.path.abspath(os.path.join(_DEMO_DIR, ".demo_hf_cache"))
# Parent sets this before starting the flow so worker subprocesses rebuild the same @huggingface config.
_MF_HF_DEMO_ENV = "METAFLOW_HF_DEMO_CONFIG"

# Built-in Netflix private repo (requires HF_TOKEN for download/metadata in practice).
_DEFAULT_PRIVATE_SPEC = "netflix/my-gpt2@main"
# Built-in public models for --auth public.
_DEFAULT_PUBLIC_SPEC = "openai-community/gpt2@main"
_DEFAULT_SECOND_PUBLIC = "google-bert/bert-base-uncased"


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
    # env: private demo repo
    if only_read_first_model:
        raise argparse.ArgumentTypeError(
            "--only-read-first-model is only supported with --auth public and --fetch metadata"
        )
    return [("gpt2", _DEFAULT_PRIVATE_SPEC)]


def _resolve_model_list(
    auth: str,
    fetch: str,
    only_read_first_model: bool,
) -> List[Tuple[str, str]]:
    return _default_model_pairs(auth, fetch, only_read_first_model)


def _validate_env_token_if_needed(
    args: Namespace, model_pairs: List[Tuple[str, str]]
) -> None:
    """Require HF token for --auth env when pairs match the shipped private demo default."""
    if args.auth != "env":
        return
    expected = _default_model_pairs("env", args.fetch, False)
    if model_pairs != expected:
        return
    if not (os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")):
        sys.stderr.write(
            "error: --auth env with the built-in private demo repo requires HF_TOKEN or "
            "HUGGING_FACE_HUB_TOKEN. Use --auth public for the public default, set a token, "
            "or edit Hub repo ids in run_huggingface_demo.py.\n"
        )
        sys.exit(2)


def _configure_auth_env(auth: str) -> None:
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


def _persist_demo_env(args: Namespace) -> None:
    payload = {
        "auth": args.auth,
        "fetch": args.fetch,
        "only_read_first_model": args.only_read_first_model,
        "prefetch": args.prefetch,
        "use_demo_cache": args.use_demo_cache,
        "local_dir": args.local_dir,
    }
    os.environ[_MF_HF_DEMO_ENV] = json.dumps(payload)


def _load_demo_args_from_env() -> Namespace:
    raw = os.environ.get(_MF_HF_DEMO_ENV)
    if not raw:
        return Namespace(
            auth="public",
            fetch="metadata",
            only_read_first_model=False,
            prefetch=False,
            use_demo_cache=False,
            local_dir=None,
        )
    cfg = json.loads(raw)
    return Namespace(**cfg)


def _execute_hf_demo(args: Namespace) -> None:
    only_read_first_model = args.only_read_first_model

    model_pairs = _resolve_model_list(args.auth, args.fetch, only_read_first_model)

    _validate_env_token_if_needed(args, model_pairs)
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

    _persist_demo_env(args)
    _execute_hf_demo(args)


def _build_parser() -> argparse.ArgumentParser:
    epilog = """
defaults (if you run: %(prog)s run with no flags)
  --auth public
  --fetch metadata
  lazy=True for @huggingface (each repo resolved on first access unless --prefetch)
  Models: built-in public gpt2 metadata-only demo (no token).

To use different Hub repos or aliases, edit _default_model_pairs / constants in this file
(see demos/huggingface/README.md).

Built-in demo repos (selected by --auth)
  --auth public: openai-community/gpt2@main (with --only-read-first-model, a second public model is also listed).
  --auth env: %(private)s

Both --auth public and --auth env set METAFLOW_HUGGINGFACE_AUTH_PROVIDER to env; they only
differ in which built-in repos are used (see above).
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
        choices=("public", "env"),
        default="public",
        help="Selects built-in demo repos (see epilog). Both choices use the "
        "env auth provider (METAFLOW_HUGGINGFACE_AUTH_PROVIDER=env). public: public Hub "
        "defaults; env: private example repo (HF_TOKEN / HUGGING_FACE_HUB_TOKEN). "
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
        "--fetch metadata; uses the built-in two-model public pair.",
    )
    run.set_defaults(func=_run_cmd)

    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    args = parser.parse_args(argv)
    args.func(args)


def _metaflow_worker_entry() -> None:
    """Reconstruct the flow when Metaflow re-invokes this file (e.g. ``local step ...``)."""
    _execute_hf_demo(_load_demo_args_from_env())


if __name__ == "__main__":
    argv = sys.argv[1:]
    if argv and argv[0] == "run":
        main()
    elif not argv:
        sys.stderr.write(
            "usage: %s run [--help | ...]\n" % os.path.basename(sys.argv[0])
        )
        sys.stderr.write(
            "This script is also re-invoked by Metaflow for worker tasks; start with "
            "`run` for the demo CLI.\n"
        )
        sys.exit(2)
    else:
        _metaflow_worker_entry()
