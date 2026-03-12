"""
Tests for metaflow.cmd.configure_cmd – specifically the persist_env guard
that warns and prompts before writing an empty configuration.
"""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

# We test persist_env in isolation by patching the module-level helpers
# (echo, click.confirm, get_config_path) so we never touch real files or TTYs.
from metaflow.cmd.configure_cmd import persist_env, get_config_path


class TestPersistEnvEmptyGuard(unittest.TestCase):
    """Validate the empty-env_dict guard inside persist_env."""

    def setUp(self):
        """Create a temporary directory to act as the configuration folder."""
        self.tmpdir = tempfile.mkdtemp()
        self.profile = ""
        self.config_path = os.path.join(self.tmpdir, "config.json")
        # Seed with an existing config so we can verify it isn't wiped.
        self.original_config = {"METAFLOW_DEFAULT_DATASTORE": "s3"}
        with open(self.config_path, "w") as f:
            json.dump(self.original_config, f)

    def tearDown(self):
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        os.rmdir(self.tmpdir)

    # ------------------------------------------------------------------
    # Helper – patch get_config_path to use our temp path
    # ------------------------------------------------------------------
    def _patch_config_path(self):
        return patch(
            "metaflow.cmd.configure_cmd.get_config_path",
            return_value=self.config_path,
        )

    # ------------------------------------------------------------------
    # 1. Non-empty dict is persisted without any prompt
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.echo")
    def test_nonempty_dict_persisted_without_prompt(self, mock_echo):
        """A non-empty env_dict should be written directly – no warning."""
        env = {"METAFLOW_DEFAULT_DATASTORE": "local"}
        with self._patch_config_path():
            result = persist_env(env, self.profile)

        self.assertTrue(result)
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, env)

    # ------------------------------------------------------------------
    # 2. Empty dict + interactive TTY + user confirms → writes empty config
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_empty_dict_interactive_user_confirms(
        self, mock_stdin, mock_echo, mock_click
    ):
        mock_stdin.isatty.return_value = True
        mock_click.confirm.return_value = True  # user says "yes"
        mock_click.style = lambda s, **kw: s  # pass-through

        with self._patch_config_path():
            result = persist_env({}, self.profile)

        self.assertTrue(result)
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, {})

    # ------------------------------------------------------------------
    # 3. Empty dict + interactive TTY + user declines → config unchanged
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_empty_dict_interactive_user_declines(
        self, mock_stdin, mock_echo, mock_click
    ):
        mock_stdin.isatty.return_value = True
        mock_click.confirm.return_value = False  # user says "no"
        mock_click.style = lambda s, **kw: s

        with self._patch_config_path():
            result = persist_env({}, self.profile)

        self.assertFalse(result)
        # Original config must be untouched.
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, self.original_config)

    # ------------------------------------------------------------------
    # 4. Empty dict + non-interactive (piped stdin) → auto-abort
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_empty_dict_non_interactive_aborts(
        self, mock_stdin, mock_echo, mock_click
    ):
        mock_stdin.isatty.return_value = False  # non-interactive

        with self._patch_config_path():
            result = persist_env({}, self.profile)

        self.assertFalse(result)
        # confirm should never be called in non-interactive mode
        mock_click.confirm.assert_not_called()
        # Original config must be untouched.
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, self.original_config)

    # ------------------------------------------------------------------
    # 5. None treated as empty (falsy) → triggers the guard
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_none_treated_as_empty(self, mock_stdin, mock_echo, mock_click):
        mock_stdin.isatty.return_value = False

        with self._patch_config_path():
            result = persist_env(None, self.profile)

        self.assertFalse(result)
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, self.original_config)

    # ------------------------------------------------------------------
    # 6. Named profile path is respected
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.echo")
    def test_named_profile_path(self, mock_echo):
        """persist_env with a named profile should write to config_<profile>.json"""
        profile = "production"
        profile_path = os.path.join(self.tmpdir, "config_production.json")
        env = {"METAFLOW_DEFAULT_DATASTORE": "s3"}

        with patch(
            "metaflow.cmd.configure_cmd.get_config_path",
            return_value=profile_path,
        ):
            result = persist_env(env, profile)

        self.assertTrue(result)
        with open(profile_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, env)

        # Cleanup extra file
        os.remove(profile_path)

    # ------------------------------------------------------------------
    # 7. Empty dict warning message is emitted
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_empty_dict_emits_warning(self, mock_stdin, mock_echo, mock_click):
        mock_stdin.isatty.return_value = False

        with self._patch_config_path():
            persist_env({}, self.profile)

        # Check that the warning message was emitted
        warning_calls = [
            call
            for call in mock_echo.call_args_list
            if "empty" in str(call).lower() and "warning" in str(call).lower()
        ]
        self.assertTrue(
            len(warning_calls) > 0,
            "Expected a warning message about empty configuration",
        )

    # ------------------------------------------------------------------
    # 8. Non-interactive abort message is emitted
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_non_interactive_abort_message(self, mock_stdin, mock_echo, mock_click):
        mock_stdin.isatty.return_value = False

        with self._patch_config_path():
            persist_env({}, self.profile)

        abort_calls = [
            call
            for call in mock_echo.call_args_list
            if "aborting" in str(call).lower()
        ]
        self.assertTrue(
            len(abort_calls) > 0,
            "Expected an abort message in non-interactive mode",
        )

    # ------------------------------------------------------------------
    # 9. Return value is True for successful non-empty write
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.echo")
    def test_return_true_on_success(self, mock_echo):
        env = {"KEY": "value"}
        with self._patch_config_path():
            result = persist_env(env, self.profile)
        self.assertTrue(result)

    # ------------------------------------------------------------------
    # 10. Config file is created fresh when it does not exist
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.echo")
    def test_creates_config_when_missing(self, mock_echo):
        os.remove(self.config_path)  # remove seeded config
        env = {"NEW_KEY": "new_value"}
        with self._patch_config_path():
            result = persist_env(env, self.profile)
        self.assertTrue(result)
        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, env)

    # ------------------------------------------------------------------
    # 11. Empty dict + interactive + confirms → file actually empty
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    @patch("sys.stdin")
    def test_empty_confirmed_file_is_empty_json(
        self, mock_stdin, mock_echo, mock_click
    ):
        mock_stdin.isatty.return_value = True
        mock_click.confirm.return_value = True

        with self._patch_config_path():
            persist_env({}, self.profile)

        with open(self.config_path) as f:
            saved = json.load(f)
        self.assertEqual(saved, {})

    # ------------------------------------------------------------------
    # 12. Non-empty dict does NOT call click.confirm
    # ------------------------------------------------------------------
    @patch("metaflow.cmd.configure_cmd.click")
    @patch("metaflow.cmd.configure_cmd.echo")
    def test_nonempty_dict_no_confirm_call(self, mock_echo, mock_click):
        env = {"KEY": "val"}
        with self._patch_config_path():
            persist_env(env, self.profile)
        mock_click.confirm.assert_not_called()


class TestGetConfigPath(unittest.TestCase):
    """Test the config path generation helper."""

    @patch(
        "metaflow.cmd.configure_cmd.METAFLOW_CONFIGURATION_DIR",
        "/fake/config/dir",
    )
    def test_default_profile_path(self):
        path = get_config_path("")
        self.assertEqual(path, "/fake/config/dir/config.json")

    @patch(
        "metaflow.cmd.configure_cmd.METAFLOW_CONFIGURATION_DIR",
        "/fake/config/dir",
    )
    def test_named_profile_path(self):
        path = get_config_path("staging")
        self.assertEqual(path, "/fake/config/dir/config_staging.json")


if __name__ == "__main__":
    unittest.main()
