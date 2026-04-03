#!/usr/bin/env bash
# Run the @huggingface decorator demo. Invoke from anywhere; resolves repo root automatically.
#
# Usage: demos/huggingface/run_huggingface_demo.sh run [none|download|env|vendor|vendor-download]
#   run              - default: public model, metadata only
#   run none         - public model (openai-community/gpt2), metadata only
#   run download     - private netflix/my-gpt2, full download (HF_TOKEN or HUGGING_FACE_HUB_TOKEN)
#   run env          - private netflix/my-gpt2, metadata only (same token env vars)
#   run vendor       - private model, metadata only (vendor token retrieval + Metatron)
#   run vendor-download - private model, full download (vendor token retrieval + Metatron)
#
# Usage: demos/huggingface/run_huggingface_demo.sh test
#   - run unit + integration tests for huggingface decorator
#
# Token modes (env, download) require HF_TOKEN or HUGGING_FACE_HUB_TOKEN.
# Vendor modes require METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL or HUGGINGFACE_VENDOR_TOKEN_URL
# (defaults from Metaflow config are applied by this script if unset). See docs/huggingface.md.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export PYTHONPATH="$ROOT"

case "${1:-run}" in
  run)
    MODE="${2:-none}"
    PYTHON="${PYTHON_PATH:-python}"
    case "$MODE" in
      vendor|vendor-download)
        export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=vendor-token
        if [[ -z "${METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL:-}" && -z "${HUGGINGFACE_VENDOR_TOKEN_URL:-}" ]]; then
          export METAFLOW_HUGGINGFACE_VENDOR_TOKEN_URL="$("$PYTHON" -c "from metaflow.metaflow_config import HUGGINGFACE_VENDOR_TOKEN_URL; print(HUGGINGFACE_VENDOR_TOKEN_URL)")"
        fi
        ;;
      none|download|env)
        export METAFLOW_HUGGINGFACE_AUTH_PROVIDER=env
        ;;
      *)
        echo "Unknown mode: $MODE" >&2
        echo "Usage: $0 run [none|download|env|vendor|vendor-download]" >&2
        exit 1
        ;;
    esac
    export HUGGINGFACE_DEMO_MODE="$MODE"
    echo "Running HuggingFace demo flow (mode=$MODE)..."
    "$PYTHON" "$SCRIPT_DIR/run_huggingface_demo.py" run
    ;;
  test)
    echo "Running HuggingFace decorator tests..."
    cd "$ROOT/test/core"
    export PYTHONPATH="$ROOT"
    echo "--- Unit tests (parsing + sentinel + env auth) ---"
    python -m unittest tests.huggingface_decorator.TestHuggingFaceParsing tests.huggingface_decorator.TestCurrentHuggingFaceSentinel tests.huggingface_decorator.TestEnvHuggingFaceAuthProvider -v || true
    echo "--- Integration test ---"
    python run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest || true
    ;;
  *)
    echo "Usage: $0 run [none|download|env|vendor|vendor-download]"
    echo "       $0 test"
    echo "  run              - public model, metadata only (default)"
    echo "  run none         - public model, metadata only"
    echo "  run download     - private netflix model, full download (HF_TOKEN or HUGGING_FACE_HUB_TOKEN)"
    echo "  run env          - private netflix model, metadata only (same token env vars)"
    echo "  run vendor       - private model, metadata (vendor token retrieval service)"
    echo "  run vendor-download - private model, full download (vendor token retrieval service)"
    exit 1
    ;;
esac
