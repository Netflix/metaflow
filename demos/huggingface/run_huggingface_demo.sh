#!/usr/bin/env bash
# Run or test the @huggingface demo. Repo root is put on PYTHONPATH.
#
# Usage:
#   ./demos/huggingface/run_huggingface_demo.sh run [-- ARGS...]
#   ./demos/huggingface/run_huggingface_demo.sh test
#
# All arguments after "run" are passed to the Python CLI (argparse). Examples:
#   ./demos/huggingface/run_huggingface_demo.sh run --help
#   ./demos/huggingface/run_huggingface_demo.sh run
#   ./demos/huggingface/run_huggingface_demo.sh run --fetch download --auth env
#   ./demos/huggingface/run_huggingface_demo.sh run --only-read-first-model
#
# See docs/huggingface.md § Demo.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export PYTHONPATH="$ROOT"
PYTHON="${PYTHON_PATH:-python}"

case "${1:-run}" in
  run)
    shift
    echo "Running HuggingFace demo (python ${PYTHON} ... run \"\$@\")..."
    exec "$PYTHON" "$SCRIPT_DIR/run_huggingface_demo.py" run "$@"
    ;;
  test)
    echo "Running HuggingFace decorator tests..."
    cd "$ROOT/test/core"
    export PYTHONPATH="$ROOT"
    echo "--- Unit tests (parsing + sentinel + env auth + lazy map) ---"
    "$PYTHON" -m unittest \
      tests.huggingface_decorator.TestHuggingFaceParsing \
      tests.huggingface_decorator.TestCurrentHuggingFaceSentinel \
      tests.huggingface_decorator.TestEnvHuggingFaceAuthProvider \
      tests.huggingface_decorator.TestLazyRepoMap \
      tests.huggingface_decorator.TestResolveLocalDirBase \
      -v || true
    echo "--- Integration test ---"
    "$PYTHON" run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest || true
    ;;
  *)
    echo "Usage: $0 run [-- ARGS passed to run_huggingface_demo.py]"
    echo "       $0 test"
    echo ""
    echo "Examples:"
    echo "  $0 run --help"
    echo "  $0 run                                    # defaults: public metadata"
    echo "  $0 run --only-read-first-model           # two models listed; step reads only the first (lazy)"
    echo "  $0 run --fetch download --auth env        # needs HF_TOKEN for built-in private repo"
    echo "  $0 run --model m=org/model@main --auth public --fetch metadata"
    exit 1
    ;;
esac
