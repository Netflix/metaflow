import os
import pytest
from unittest.mock import MagicMock, patch

from metaflow.plugins.uv.uv_environment import UVEnvironment, UVException


@pytest.fixture
def uv_env(tmp_path):
    """UVEnvironment with minimal required files present."""
    (tmp_path / "uv.lock").touch()
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
    flow = MagicMock()
    with patch("os.getcwd", return_value=str(tmp_path)):
        yield UVEnvironment(flow)


# ---------------------------------------------------------------------------
# _get_credential_export_cmds
# ---------------------------------------------------------------------------


class TestGetCredentialExportCmds:
    def test_uv_index_vars_are_forwarded(self, uv_env):
        with patch.dict(os.environ, {"UV_INDEX_MYPYPI_PASSWORD": "secret"}, clear=True):
            cmds = uv_env._get_credential_export_cmds()
        assert any("UV_INDEX_MYPYPI_PASSWORD" in c for c in cmds)

    def test_non_uv_index_vars_are_not_auto_forwarded(self, uv_env):
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "key"}, clear=True):
            cmds = uv_env._get_credential_export_cmds()
        assert not any("AWS_ACCESS_KEY_ID" in c for c in cmds)

    def test_values_are_shell_escaped(self, uv_env):
        with patch.dict(
            os.environ, {"UV_INDEX_TOKEN": "val with spaces & special"}, clear=True
        ):
            cmds = uv_env._get_credential_export_cmds()
        assert any("'val with spaces & special'" in c for c in cmds)

    def test_forward_env_vars_config_forwards_named_var(self, uv_env):
        with patch.dict(os.environ, {"MY_TOKEN": "tok123"}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", "MY_TOKEN"
            ):
                cmds = uv_env._get_credential_export_cmds()
        assert any("MY_TOKEN" in c for c in cmds)

    def test_forward_env_vars_config_skips_missing_var(self, uv_env):
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", "MISSING_VAR"
            ):
                cmds = uv_env._get_credential_export_cmds()
        assert not any("MISSING_VAR" in c for c in cmds)

    def test_forward_env_vars_config_handles_whitespace(self, uv_env):
        with patch.dict(os.environ, {"VAR_A": "a", "VAR_B": "b"}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS",
                " VAR_A , VAR_B ",
            ):
                cmds = uv_env._get_credential_export_cmds()
        assert any("VAR_A" in c for c in cmds)
        assert any("VAR_B" in c for c in cmds)

    def test_no_relevant_vars_returns_empty_list(self, uv_env):
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", None
            ):
                cmds = uv_env._get_credential_export_cmds()
        assert cmds == []

    def test_uv_index_var_not_duplicated_when_in_forward_env_vars(self, uv_env):
        with patch.dict(os.environ, {"UV_INDEX_TOKEN": "tok"}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS",
                "UV_INDEX_TOKEN",
            ):
                cmds = uv_env._get_credential_export_cmds()
        exports = [c for c in cmds if "UV_INDEX_TOKEN" in c]
        assert len(exports) == 1


# ---------------------------------------------------------------------------
# add_to_package — .netrc forwarding
# ---------------------------------------------------------------------------


class TestAddToPackageNetrc:
    def test_netrc_included_when_flag_set_and_file_exists(self, uv_env, tmp_path):
        fake_netrc = tmp_path / ".netrc"
        fake_netrc.write_text("machine example.com login user password pass")
        with patch("metaflow.plugins.uv.uv_environment.UV_FORWARD_NETRC", True):
            with patch("os.path.expanduser", return_value=str(fake_netrc)):
                files = uv_env.add_to_package()
        arcnames = [f[1] for f in files]
        assert "netrc" in arcnames

    def test_netrc_not_included_when_flag_is_false(self, uv_env):
        with patch("metaflow.plugins.uv.uv_environment.UV_FORWARD_NETRC", False):
            files = uv_env.add_to_package()
        arcnames = [f[1] for f in files]
        assert "netrc" not in arcnames

    def test_missing_netrc_does_not_raise(self, uv_env):
        with patch("metaflow.plugins.uv.uv_environment.UV_FORWARD_NETRC", True):
            with patch("os.path.expanduser", return_value="/nonexistent/.netrc"):
                files = uv_env.add_to_package()  # must not raise
        arcnames = [f[1] for f in files]
        assert "netrc" not in arcnames

    def test_uv_lock_and_pyproject_always_present(self, uv_env):
        with patch("metaflow.plugins.uv.uv_environment.UV_FORWARD_NETRC", False):
            files = uv_env.add_to_package()
        arcnames = [f[1] for f in files]
        assert "uv.lock" in arcnames
        assert "pyproject.toml" in arcnames


# ---------------------------------------------------------------------------
# validate_environment — UV_FORWARD_ENV_VARS name validation
# ---------------------------------------------------------------------------


class TestValidateEnvironment:
    def test_invalid_var_name_in_forward_env_vars_raises(self, uv_env):
        with patch(
            "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", "my-invalid-token"
        ):
            with pytest.raises(UVException, match="invalid environment variable name"):
                uv_env.validate_environment(MagicMock(), "s3")

    def test_valid_var_names_do_not_raise(self, uv_env):
        with patch(
            "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", "MY_TOKEN,ANOTHER_VAR"
        ):
            uv_env.validate_environment(MagicMock(), "s3")  # must not raise


# ---------------------------------------------------------------------------
# bootstrap_commands — ordering
# ---------------------------------------------------------------------------


class TestBootstrapCommands:
    def test_export_cmds_appear_before_bootstrap_call(self, uv_env):
        with patch.dict(os.environ, {"UV_INDEX_TOKEN": "tok"}, clear=True):
            cmds = uv_env.bootstrap_commands("start", "s3")
        # export must come first
        assert cmds[0].startswith("export UV_INDEX_TOKEN=")
        # bootstrap call must follow
        bootstrap_idx = next(
            i for i, c in enumerate(cmds) if "metaflow.plugins.uv.bootstrap" in c
        )
        assert bootstrap_idx > 0

    def test_no_credential_export_cmds_when_no_uv_index_vars(self, uv_env):
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "metaflow.plugins.uv.uv_environment.UV_FORWARD_ENV_VARS", None
            ):
                cmds = uv_env.bootstrap_commands("start", "s3")
        # The PATH export is always present; no additional credential exports expected
        credential_exports = [
            c for c in cmds if c.startswith("export ") and "PATH" not in c
        ]
        assert credential_exports == []

    def test_datastore_type_passed_to_bootstrap(self, uv_env):
        with patch.dict(os.environ, {}, clear=True):
            cmds = uv_env.bootstrap_commands("start", "azure")
        assert any('"azure"' in c for c in cmds)
