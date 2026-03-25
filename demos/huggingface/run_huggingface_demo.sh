#!/usr/bin/env bash
# Run the @huggingface decorator demo. Invoke from anywhere; resolves repo root automatically.
#
# Usage: demos/huggingface/run_huggingface_demo.sh run [none|download|env]
#   run         - default: public model, metadata only
#   run none    - public model (openai-community/gpt2), metadata only
#   run download - private netflix/my-gpt2, full download (token env)
#   run env     - private netflix/my-gpt2, metadata only, token in env
#
# Usage: demos/huggingface/run_huggingface_demo.sh test
#   - run unit + integration tests for huggingface decorator
#
# For private models: set HF_TOKEN, HUGGING_FACE_TOKEN, or HUGGING_FACE_HUB_TOKEN. See docs/huggingface.md.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export PYTHONPATH="$ROOT"

case "${1:-run}" in
  run)
    MODE="${2:-none}"
    export HUGGINGFACE_DEMO_MODE="$MODE"
    PYTHON="${PYTHON_PATH:-python}"
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
    echo "Usage: $0 run [none|download|env]"
    echo "       $0 test"
    echo "  run         - public model, metadata only (default)"
    echo "  run none    - public model, metadata only"
    echo "  run download - private netflix model, full download (token env)"
    echo "  run env     - private netflix model, metadata only (token in env)"
    exit 1
    ;;
esac
