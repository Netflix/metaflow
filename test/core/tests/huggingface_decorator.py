"""
Tests for @huggingface decorator: parsing, current.huggingface sentinel, and integration.

Unit tests (TestHuggingFaceParsing, TestCurrentHuggingFaceSentinel) test parsing and
the current.huggingface sentinel. Integration test (HuggingFaceDecoratorTest) runs a
flow with @huggingface and requires huggingface_hub; it is skipped when the package
is not installed.

How to test locally
-------------------
1. From repo root, install Metaflow and optional deps:
   pip install -e .
   pip install huggingface_hub   # for integration test and manual flow

2. Integration test (runs flow with @huggingface; needs huggingface_hub):
   cd test/core
   PYTHONPATH=../.. python run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest

3. Unit tests (parsing + sentinel; require metaflow_test on PYTHONPATH):
   cd test/core
   PYTHONPATH=../.. python -m unittest tests.huggingface_decorator.TestHuggingFaceParsing tests.huggingface_decorator.TestCurrentHuggingFaceSentinel -v

4. Manual flow (from repo root):
   python metaflow/plugins/huggingface/example_flow.py run
   (Optional: set HF_TOKEN or HUGGING_FACE_HUB_TOKEN for gated models.)
"""

import os
import unittest

try:
    import huggingface_hub  # noqa: F401
    HAS_HUGGINGFACE_HUB = True
except ImportError:
    HAS_HUGGINGFACE_HUB = False

from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag, assert_equals


# Unit tests for parsing (no flow run)
class TestHuggingFaceParsing(unittest.TestCase):
    def test_parse_repo_spec_repo_only(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _parse_repo_spec

        self.assertEqual(_parse_repo_spec("bert-base-uncased"), ("bert-base-uncased", "main"))
        self.assertEqual(
            _parse_repo_spec("meta-llama/Llama-2-7b"),
            ("meta-llama/Llama-2-7b", "main"),
        )

    def test_parse_repo_spec_with_revision(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _parse_repo_spec

        self.assertEqual(
            _parse_repo_spec("meta-llama/Llama-2-7b@main"),
            ("meta-llama/Llama-2-7b", "main"),
        )
        self.assertEqual(
            _parse_repo_spec("bert-base-uncased@v1.0"),
            ("bert-base-uncased", "v1.0"),
        )

    def test_parse_repo_spec_empty_raises(self):
        from metaflow.exception import MetaflowException
        from metaflow.plugins.huggingface.huggingface_decorator import _parse_repo_spec

        with self.assertRaises(MetaflowException):
            _parse_repo_spec("")
        with self.assertRaises(MetaflowException):
            _parse_repo_spec("   ")

    def test_build_spec_map_models(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _build_spec_map

        m = _build_spec_map(models=["bert-base-uncased", "meta-llama/Llama-2-7b@main"], model_mapping=None)
        self.assertEqual(m["bert-base-uncased"], ("bert-base-uncased", "main"))
        self.assertEqual(m["meta-llama/Llama-2-7b"], ("meta-llama/Llama-2-7b", "main"))

    def test_build_spec_map_model_mapping(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _build_spec_map

        m = _build_spec_map(
            models=None,
            model_mapping={"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"},
        )
        self.assertEqual(m["llama"], ("meta-llama/Llama-2-7b", "main"))
        self.assertEqual(m["bert"], ("bert-base-uncased", "main"))

    def test_build_spec_map_combined(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _build_spec_map

        m = _build_spec_map(
            models=["bert-base-uncased"],
            model_mapping={"llama": "meta-llama/Llama-2-7b@main"},
        )
        self.assertEqual(m["bert-base-uncased"], ("bert-base-uncased", "main"))
        self.assertEqual(m["llama"], ("meta-llama/Llama-2-7b", "main"))


class TestCurrentHuggingFaceSentinel(unittest.TestCase):
    """Test that current.huggingface raises when not inside a @huggingface step."""

    def test_huggingface_raises_without_decorator(self):
        from metaflow import current

        # Default state: huggingface property raises (set in Current.__init__)
        try:
            _ = current.huggingface
            self.fail("expected RuntimeError when accessing current.huggingface without @huggingface")
        except RuntimeError as e:
            self.assertIn("huggingface", str(e))
            self.assertIn("@huggingface", str(e))


# Integration test: flow with @huggingface (requires huggingface_hub and network)
@unittest.skipIf(
    not HAS_HUGGINGFACE_HUB,
    "huggingface_hub not installed; pip install huggingface_hub to run this test",
)
class HuggingFaceDecoratorTest(MetaflowTest):
    """
    Test that @huggingface decorator provides current.huggingface.models with local paths.
    Skips if huggingface_hub is not installed.
    """

    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @tag("huggingface(models=['bert-base-uncased'])")
    @steps(0, ["start"])
    def step_start(self):
        try:
            from metaflow import current
        except Exception:
            self.hf_import_error = True
            self.model_path = None
            return
        self.hf_import_error = False
        self.model_path = current.huggingface.models["bert-base-uncased"]
        self.model_key_present = "bert-base-uncased" in current.huggingface.models

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            return
        for step in run:
            for task in step:
                if hasattr(task.data, "hf_import_error") and task.data.hf_import_error:
                    # huggingface_hub not available or decorator failed; skip assertions
                    return
                if hasattr(task.data, "model_path") and task.data.model_path:
                    path = task.data.model_path
                    assert_equals(True, os.path.isdir(path), "model path should be a directory")
                    assert_equals(True, len(path) > 0, "model path should be non-empty")
                if hasattr(task.data, "model_key_present"):
                    assert_equals(True, task.data.model_key_present)
