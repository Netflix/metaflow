"""
Tests for @huggingface decorator: parsing, current.huggingface sentinel, and integration.

Unit tests cover parsing, auth env precedence, lazy maps, and local dir resolution.
Integration test (HuggingFaceDecoratorTest) runs a flow with @huggingface and
requires huggingface_hub; the class is skipped when the package is not installed.

How to test locally
-------------------
1. From repo root, install Metaflow and optional deps:
   pip install -e .
   pip install huggingface_hub   # for integration test and manual flow

2. Integration test (runs flow with @huggingface; needs huggingface_hub):
   cd test/core
   PYTHONPATH=../.. python run_tests.py --debug --contexts dev-local --tests HuggingFaceDecoratorTest

3. Unit tests (repo root + this directory on PYTHONPATH for ``tests.*`` imports):
   cd test/core
   PYTHONPATH=`pwd`/../../:`pwd` python -m unittest tests.huggingface_decorator -v

4. Manual flow (from repo root):
   python metaflow/plugins/huggingface/example_flow.py run
   (Optional: set HF_TOKEN, HUGGING_FACE_TOKEN, or HUGGING_FACE_HUB_TOKEN for gated models.)

5. Demo CLI: python demos/huggingface/run_huggingface_demo.py run --help after pip install -e . or export PYTHONPATH (see demos/huggingface/README.md).
"""

import os
import unittest
from unittest.mock import patch

try:
    import huggingface_hub  # noqa: F401

    HAS_HUGGINGFACE_HUB = True
except ImportError:
    HAS_HUGGINGFACE_HUB = False

from metaflow import current
from metaflow.exception import MetaflowException
import metaflow.metaflow_config as mf_config
import metaflow.plugins.huggingface.huggingface_decorator as hf_dec
from metaflow.plugins.huggingface.huggingface_decorator import (
    HuggingFaceDecorator,
    _LazyRepoMap,
    _build_spec_map,
    _fill_huggingface_maps,
    _parse_repo_spec,
)
from metaflow.plugins.huggingface.env_auth_provider import EnvHuggingFaceAuthProvider

from metaflow_test import MetaflowTest, assert_equals, steps, tag

# Env vars read by EnvHuggingFaceAuthProvider (order matters in production code).
_HF_TOKEN_ENV_KEYS = ("HF_TOKEN", "HUGGING_FACE_TOKEN", "HUGGING_FACE_HUB_TOKEN")

# Same SKIP_GRAPHS list as secrets_decorator / timeout_decorator integration tests.
_SKIP_SWITCH_HEAVY_GRAPHS = [
    "simple_switch",
    "nested_switch",
    "branch_in_switch",
    "foreach_in_switch",
    "switch_in_branch",
    "switch_in_foreach",
    "recursive_switch",
    "recursive_switch_inside_foreach",
]

# Arguments for HuggingFaceDecorator.step_init when graph/flow are irrelevant.
_NO_FLOW_STEP_INIT = (None, None, "start", [], None, None, None)


def _fake_hf_api_class(error_message):
    """Return a minimal HfApi-like class whose model_info always raises."""

    class _FakeApi:
        def __init__(self, token=None, endpoint=None):
            pass

        def model_info(self, repo_id, revision=None, token=None):
            raise RuntimeError(error_message)

    return _FakeApi


class TestHuggingFaceParsing(unittest.TestCase):
    def test_parse_repo_spec_repo_only(self):
        self.assertEqual(
            _parse_repo_spec("bert-base-uncased"), ("bert-base-uncased", "main")
        )
        self.assertEqual(
            _parse_repo_spec("meta-llama/Llama-2-7b"),
            ("meta-llama/Llama-2-7b", "main"),
        )

    def test_parse_repo_spec_with_revision(self):
        self.assertEqual(
            _parse_repo_spec("meta-llama/Llama-2-7b@main"),
            ("meta-llama/Llama-2-7b", "main"),
        )
        self.assertEqual(
            _parse_repo_spec("bert-base-uncased@v1.0"),
            ("bert-base-uncased", "v1.0"),
        )

    def test_parse_repo_spec_empty_raises(self):
        with self.assertRaises(MetaflowException):
            _parse_repo_spec("")
        with self.assertRaises(MetaflowException):
            _parse_repo_spec("   ")

    def test_parse_repo_spec_malformed_at_raises(self):
        with self.assertRaises(MetaflowException):
            _parse_repo_spec("repo_only@")
        with self.assertRaises(MetaflowException):
            _parse_repo_spec("@revision_only")

    def test_build_spec_map_list(self):
        m = _build_spec_map(["bert-base-uncased", "meta-llama/Llama-2-7b@main"])
        self.assertEqual(m["bert-base-uncased"], ("bert-base-uncased", "main"))
        self.assertEqual(m["meta-llama/Llama-2-7b"], ("meta-llama/Llama-2-7b", "main"))

    def test_build_spec_map_dict(self):
        m = _build_spec_map(
            {"llama": "meta-llama/Llama-2-7b@main", "bert": "bert-base-uncased"},
        )
        self.assertEqual(m["llama"], ("meta-llama/Llama-2-7b", "main"))
        self.assertEqual(m["bert"], ("bert-base-uncased", "main"))

    def test_build_spec_map_wrong_type_raises(self):
        with self.assertRaises(MetaflowException):
            _build_spec_map("not-a-list-or-dict")

    def test_build_spec_map_list_non_string_raises(self):
        with self.assertRaises(MetaflowException):
            _build_spec_map(["ok", 123])


class TestHuggingFaceDecoratorStepInit(unittest.TestCase):
    def test_step_init_no_models_raises(self):
        dec = HuggingFaceDecorator(attributes={"models": None})
        with self.assertRaises(MetaflowException) as ctx:
            dec.step_init(*_NO_FLOW_STEP_INIT)
        self.assertIn("models", str(ctx.exception).lower())

    def test_step_init_empty_models_raises(self):
        dec = HuggingFaceDecorator(attributes={"models": []})
        with self.assertRaises(MetaflowException):
            dec.step_init(*_NO_FLOW_STEP_INIT)

    def test_step_init_local_dir_not_string_raises(self):
        dec = HuggingFaceDecorator(
            attributes={"models": ["bert-base-uncased"], "local_dir": 99}
        )
        with self.assertRaises(MetaflowException) as ctx:
            dec.step_init(*_NO_FLOW_STEP_INIT)
        self.assertIn("local_dir", str(ctx.exception).lower())


class TestEnvHuggingFaceAuthProvider(unittest.TestCase):
    """Env provider reads HF_TOKEN, then HUGGING_FACE_TOKEN, then HUGGING_FACE_HUB_TOKEN."""

    def test_token_precedence(self):
        provider = EnvHuggingFaceAuthProvider()
        cases = (
            (
                {
                    "HF_TOKEN": "a",
                    "HUGGING_FACE_TOKEN": "b",
                    "HUGGING_FACE_HUB_TOKEN": "c",
                },
                "a",
            ),
            (
                {
                    "HF_TOKEN": "",
                    "HUGGING_FACE_TOKEN": "b",
                    "HUGGING_FACE_HUB_TOKEN": "c",
                },
                "b",
            ),
            (
                {
                    "HF_TOKEN": "",
                    "HUGGING_FACE_TOKEN": "",
                    "HUGGING_FACE_HUB_TOKEN": "c",
                },
                "c",
            ),
        )
        for env_overlay, expected in cases:
            with self.subTest(expected=expected):
                with patch.dict(os.environ, env_overlay, clear=False):
                    self.assertEqual(provider.get_token(), expected)

    def test_token_all_keys_absent_returns_none(self):
        with patch.dict(os.environ, clear=False):
            for key in _HF_TOKEN_ENV_KEYS:
                os.environ.pop(key, None)
            self.assertIsNone(EnvHuggingFaceAuthProvider().get_token())


class TestGetAuthProvider(unittest.TestCase):
    def test_unknown_provider_falls_back_to_env(self):
        with patch.object(mf_config, "HUGGINGFACE_AUTH_PROVIDER", "no-such-id"):
            with patch("metaflow.plugins.HF_AUTH_PROVIDERS", []):
                provider = hf_dec._get_auth_provider()
        self.assertIsInstance(provider, EnvHuggingFaceAuthProvider)


class TestGetModelInfo(unittest.TestCase):
    def test_wraps_not_found_when_token_set(self):
        fake = _fake_hf_api_class("404 repository not found")
        with patch.object(hf_dec, "_import_hf_api", return_value=fake):
            with self.assertRaises(MetaflowException) as ctx:
                hf_dec._get_model_info("acme/model", "main", "secret-token")
        msg = str(ctx.exception)
        self.assertIn("acme/model", msg)
        self.assertIn("Token was obtained", msg)

    def test_propagates_when_no_token(self):
        fake = _fake_hf_api_class("404 not found")
        with patch.object(hf_dec, "_import_hf_api", return_value=fake):
            with self.assertRaises(RuntimeError):
                hf_dec._get_model_info("acme/model", "main", None)


class TestFillHuggingfaceMaps(unittest.TestCase):
    def test_metadata_only_uses_get_model_info(self):
        sentinel = object()
        with patch.object(hf_dec, "_get_model_info", return_value=sentinel) as m_info:
            path_map, info_map = _fill_huggingface_maps(
                {"a": ("r1", "v1"), "b": ("r2", "v2")},
                True,
                "tok",
                "https://hub.example",
                "/base",
            )
        self.assertEqual(path_map, {})
        self.assertIs(info_map["a"], sentinel)
        self.assertIs(info_map["b"], sentinel)
        self.assertEqual(m_info.call_count, 2)
        m_info.assert_any_call("r1", "v1", "tok", endpoint="https://hub.example")
        m_info.assert_any_call("r2", "v2", "tok", endpoint="https://hub.example")

    def test_download_uses_download_to_task_dir(self):
        with patch.object(
            hf_dec, "_download_to_task_dir", return_value="/snap/path"
        ) as m_dl:
            path_map, info_map = _fill_huggingface_maps(
                {"alias": ("org/repo", "rev")},
                False,
                "tok",
                None,
                "/parent",
            )
        self.assertEqual(info_map, {})
        self.assertEqual(path_map["alias"], "/snap/path")
        m_dl.assert_called_once_with("org/repo", "rev", "tok", None, "/parent")


class TestLazyRepoMap(unittest.TestCase):
    """Lazy mapping resolves each key at most once on first access."""

    def test_lazy_download_deferred_until_getitem(self):
        calls = []

        def record_download(repo_id, revision, token, endpoint, local_dir_base):
            calls.append((repo_id, revision, local_dir_base))
            return "/fake/%s" % repo_id.replace("/", "--")

        with patch.object(hf_dec, "_download_to_task_dir", side_effect=record_download):
            m = _LazyRepoMap(
                {"a": ("org/m1", "main"), "b": ("org/m2", "v1")},
                False,
                None,
                None,
                "/fakebase",
            )
            self.assertEqual(calls, [])
            self.assertEqual(m["a"], "/fake/org--m1")
            self.assertEqual(calls, [("org/m1", "main", "/fakebase")])
            self.assertEqual(m["a"], "/fake/org--m1")
            self.assertEqual(calls, [("org/m1", "main", "/fakebase")])
            self.assertEqual(m["b"], "/fake/org--m2")
            self.assertEqual(
                calls,
                [
                    ("org/m1", "main", "/fakebase"),
                    ("org/m2", "v1", "/fakebase"),
                ],
            )

    def test_lazy_metadata_deferred_until_getitem(self):
        calls = []

        def record_info(repo_id, revision, token, endpoint=None):
            calls.append((repo_id, revision, endpoint))
            return object()

        with patch.object(hf_dec, "_get_model_info", side_effect=record_info):
            m = _LazyRepoMap(
                {"k": ("org/m", "rev")},
                True,
                "tok",
                "https://hf.example",
                "/base",
            )
            self.assertEqual(calls, [])
            _ = m["k"]
            self.assertEqual(calls, [("org/m", "rev", "https://hf.example")])

    def test_lazy_unknown_key_raises(self):
        m = _LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
        with self.assertRaises(KeyError):
            _ = m["missing"]

    def test_contains_does_not_download(self):
        with patch.object(hf_dec, "_download_to_task_dir", side_effect=AssertionError):
            m = _LazyRepoMap({"a": ("x", "main")}, False, None, None, "/tmp")
            self.assertIn("a", m)
            self.assertNotIn("z", m)

    def test_get_returns_value_or_default(self):
        info = {"id": "m"}
        with patch.object(hf_dec, "_get_model_info", return_value=info):
            m = _LazyRepoMap({"k": ("x/y", "main")}, True, None, None, "/tmp")
            self.assertIs(m.get("k"), info)
            self.assertIsNone(m.get("missing"))
            self.assertEqual(m.get("missing", "default"), "default")


class TestResolveLocalDirBase(unittest.TestCase):
    def test_explicit_path(self):
        p = hf_dec._resolve_local_dir_base("/data/hf_cache")
        self.assertEqual(p, os.path.abspath("/data/hf_cache"))

    def test_explicit_path_strips_whitespace(self):
        p = hf_dec._resolve_local_dir_base("  /data/hf_cache  ")
        self.assertEqual(p, os.path.abspath("/data/hf_cache"))

    def test_default_joins_task_temp(self):
        # ``current.tempdir`` is read-only; patch backing field (required on Py 3.14+).
        with patch.object(current, "_tempdir", "/var/mf_tmp"):
            with patch.object(mf_config, "HUGGINGFACE_LOCAL_DIR", None):
                p = hf_dec._resolve_local_dir_base(None)
        self.assertEqual(p, os.path.join("/var/mf_tmp", "metaflow_huggingface"))

    def test_config_when_decorator_unset(self):
        with patch.object(mf_config, "HUGGINGFACE_LOCAL_DIR", "/mnt/shared/hf"):
            p = hf_dec._resolve_local_dir_base(None)
        self.assertEqual(p, os.path.abspath("/mnt/shared/hf"))


class TestCurrentHuggingFaceSentinel(unittest.TestCase):
    """``current.huggingface`` raises outside a @huggingface step."""

    def test_huggingface_raises_without_decorator(self):
        with self.assertRaises(RuntimeError) as ctx:
            _ = current.huggingface
        self.assertIn("huggingface", str(ctx.exception))
        self.assertIn("@huggingface", str(ctx.exception))


@unittest.skipIf(
    not HAS_HUGGINGFACE_HUB,
    "huggingface_hub not installed; pip install huggingface_hub to run this test",
)
class HuggingFaceDecoratorTest(MetaflowTest):
    """@huggingface exposes ``current.huggingface.models`` with local paths."""

    PRIORITY = 2
    SKIP_GRAPHS = _SKIP_SWITCH_HEAVY_GRAPHS

    @tag("huggingface(models=['bert-base-uncased'])")
    @steps(0, ["start"])
    def step_start(self):
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
                if hasattr(task.data, "model_path") and task.data.model_path:
                    path = task.data.model_path
                    assert_equals(
                        True, os.path.isdir(path), "model path should be a directory"
                    )
                    assert_equals(True, len(path) > 0, "model path should be non-empty")
                if hasattr(task.data, "model_key_present"):
                    assert_equals(True, task.data.model_key_present)
