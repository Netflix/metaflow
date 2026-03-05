#!/usr/bin/env bash
# Run the @huggingface decorator demo from repo root.
# Usage: ./run_huggingface_demo.sh [run|test]
#   run  - run the demo flow (default)
#   test - run unit + integration tests for huggingface decorator

set -e
cd "$(dirname "$0")"
ROOT="$(pwd)"
export PYTHONPATH="$ROOT"

case "${1:-run}" in
  run)
    echo "Running HuggingFace demo flow..."
    python run_huggingface_demo.py run --no-pylint
    ;;
  test)
    echo "Running HuggingFace decorator tests..."
    cd "$ROOT/test/core"
    export PYTHONPATH="$ROOT"
    echo "--- Unit tests (parsing + sentinel) ---"
    python -m unittest tests.huggingface_decorator.TestHuggingFaceParsing tests.huggingface_decorator.TestCurrentHuggingFaceSentinel -v || true
    echo "--- Integration test ---"
    python run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest || true
    ;;
  *)
    echo "Usage: $0 [run|test]"
    echo "  run  - run the demo flow (default)"
    echo "  test - run unit + integration tests"
    exit 1
    ;;
esac
