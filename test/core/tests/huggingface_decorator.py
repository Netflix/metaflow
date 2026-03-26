"""
Tests for the ``@huggingface`` decorator.

Unit tests cover repo spec parsing, the env auth provider, and the
``current.huggingface`` sentinel when the decorator is not used. For semantics and
auth, see ``docs/huggingface.md``. The demo under ``demos/huggingface/`` exercises
``@huggingface`` in a live flow; ``run_huggingface_demo.sh test`` runs this unit suite.
General test instructions are in ``CONTRIBUTING.md``.
"""

import os
import unittest
from unittest.mock import patch


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


class TestGetAuthProvider(unittest.TestCase):
    """_get_auth_provider must not silently fall back to env for a missing custom id."""

    def test_unknown_provider_raises(self):
        from metaflow.exception import MetaflowException
        import metaflow.metaflow_config as mc
        import metaflow.plugins as plugins
        from metaflow.plugins.huggingface.huggingface_decorator import (
            _get_auth_provider,
        )

        with patch.object(mc, "HUGGINGFACE_AUTH_PROVIDER", "custom-vault"):
            with patch.object(plugins, "HF_AUTH_PROVIDERS", []):
                with self.assertRaises(MetaflowException) as ctx:
                    _get_auth_provider()
        self.assertIn("custom-vault", str(ctx.exception))
        self.assertIn("@huggingface:", str(ctx.exception))

    def test_env_fallback_when_env_not_in_plugin_list(self):
        """Core env provider is used when TYPE env is missing from HF_AUTH_PROVIDERS."""
        import metaflow.metaflow_config as mc
        import metaflow.plugins as plugins
        from metaflow.plugins.huggingface.env_auth_provider import (
            EnvHuggingFaceAuthProvider,
        )
        from metaflow.plugins.huggingface.huggingface_decorator import (
            _get_auth_provider,
        )

        with patch.object(mc, "HUGGINGFACE_AUTH_PROVIDER", "env"):
            with patch.object(plugins, "HF_AUTH_PROVIDERS", []):
                p = _get_auth_provider()
        self.assertIsInstance(p, EnvHuggingFaceAuthProvider)


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
