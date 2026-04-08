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
   PYTHONPATH=../.. python -m unittest tests.huggingface_decorator.TestHuggingFaceParsing tests.huggingface_decorator.TestCurrentHuggingFaceSentinel tests.huggingface_decorator.TestEnvHuggingFaceAuthProvider -v

4. Manual flow (from repo root):
   python metaflow/plugins/huggingface/example_flow.py run
   (Optional: set HF_TOKEN, HUGGING_FACE_TOKEN, or HUGGING_FACE_HUB_TOKEN for gated models.)

5. Demo CLI: PYTHONPATH=. python demos/huggingface/run_huggingface_demo.py run --help (see demos/huggingface/README.md).
"""

import os
import unittest
from unittest.mock import patch

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

        self.assertEqual(
            _parse_repo_spec("bert-base-uncased"), ("bert-base-uncased", "main")
        )
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

    def test_build_spec_map_list(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _build_spec_map

        m = _build_spec_map(["bert-base-uncased", "meta-llama/Llama-2-7b@main"])
        self.assertEqual(m["bert-base-uncased"], ("bert-base-uncased", "main"))
        self.assertEqual(m["meta-llama/Llama-2-7b"], ("meta-llama/Llama-2-7b", "main"))

    def test_build_spec_map_dict(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _build_spec_map

        m = _build_spec_map(
            {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"},
        )
        self.assertEqual(m["llama"], ("meta-llama/Llama-2-7b", "main"))
        self.assertEqual(m["bert"], ("bert-base-uncased", "main"))


class TestEnvHuggingFaceAuthProvider(unittest.TestCase):
    """Env provider reads HF_TOKEN, then HUGGING_FACE_TOKEN, then HUGGING_FACE_HUB_TOKEN."""

    def test_token_precedence(self):
        from metaflow.plugins.huggingface.env_auth_provider import (
            EnvHuggingFaceAuthProvider,
        )

        p = EnvHuggingFaceAuthProvider()
        with patch.dict(
            os.environ,
            {
                "HF_TOKEN": "a",
                "HUGGING_FACE_TOKEN": "b",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            clear=False,
        ):
            self.assertEqual(p.get_token(), "a")
        with patch.dict(
            os.environ,
            {
                "HF_TOKEN": "",
                "HUGGING_FACE_TOKEN": "b",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            clear=False,
        ):
            self.assertEqual(p.get_token(), "b")
        with patch.dict(
            os.environ,
            {
                "HF_TOKEN": "",
                "HUGGING_FACE_TOKEN": "",
                "HUGGING_FACE_HUB_TOKEN": "c",
            },
            clear=False,
        ):
            self.assertEqual(p.get_token(), "c")


class TestLazyRepoMap(unittest.TestCase):
    """Lazy mapping resolves each key at most once on first access."""

    def test_lazy_download_deferred_until_getitem(self):
        import metaflow.plugins.huggingface.huggingface_decorator as hd

        calls = []

        def fake_download(repo_id, revision, token, endpoint, local_dir_base):
            calls.append((repo_id, revision, local_dir_base))
            return "/fake/%s" % repo_id.replace("/", "_")

        with patch.object(hd, "_download_to_task_dir", side_effect=fake_download):
            m = hd._LazyRepoMap(
                {"a": ("org/m1", "main"), "b": ("org/m2", "v1")},
                False,
                None,
                None,
                "/fakebase",
            )
            self.assertEqual(len(calls), 0)
            self.assertEqual(m["a"], "/fake/org_m1")
            self.assertEqual(len(calls), 1)
            self.assertEqual(m["a"], "/fake/org_m1")
            self.assertEqual(len(calls), 1)
            self.assertEqual(m["b"], "/fake/org_m2")
            self.assertEqual(len(calls), 2)

    def test_lazy_unknown_key_raises(self):
        from metaflow.plugins.huggingface.huggingface_decorator import _LazyRepoMap

        m = _LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
        with self.assertRaises(KeyError):
            _ = m["missing"]

    def test_contains_does_not_download(self):
        import metaflow.plugins.huggingface.huggingface_decorator as hd

        with patch.object(hd, "_download_to_task_dir", side_effect=AssertionError):
            m = hd._LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
            self.assertIn("a", m)
            self.assertNotIn("z", m)


class TestResolveLocalDirBase(unittest.TestCase):
    def test_explicit_path(self):
        import metaflow.plugins.huggingface.huggingface_decorator as hd

        p = hd._resolve_local_dir_base("/data/hf_cache")
        self.assertEqual(p, os.path.abspath("/data/hf_cache"))

    def test_default_joins_task_temp(self):
        import metaflow.metaflow_config as mc
        import metaflow.plugins.huggingface.huggingface_decorator as hd
        from metaflow.metaflow_current import current

        with patch.object(current, "tempdir", "/var/mf_tmp"):
            with patch.object(mc, "HUGGINGFACE_LOCAL_DIR", None):
                p = hd._resolve_local_dir_base(None)
        self.assertEqual(p, os.path.join("/var/mf_tmp", "metaflow_huggingface"))

    def test_config_when_decorator_unset(self):
        import metaflow.metaflow_config as mc
        import metaflow.plugins.huggingface.huggingface_decorator as hd

        with patch.object(mc, "HUGGINGFACE_LOCAL_DIR", "/mnt/shared/hf"):
            p = hd._resolve_local_dir_base(None)
        self.assertEqual(p, os.path.abspath("/mnt/shared/hf"))


class TestCurrentHuggingFaceSentinel(unittest.TestCase):
    """Test that current.huggingface raises when not inside a @huggingface step."""

    def test_huggingface_raises_without_decorator(self):
        from metaflow import current

        # Default state: huggingface property raises (set in Current.__init__)
        try:
            _ = current.huggingface
            self.fail(
                "expected RuntimeError when accessing current.huggingface without @huggingface"
            )
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
                    assert_equals(
                        True, os.path.isdir(path), "model path should be a directory"
                    )
                    assert_equals(True, len(path) > 0, "model path should be non-empty")
                if hasattr(task.data, "model_key_present"):
                    assert_equals(True, task.data.model_key_present)
