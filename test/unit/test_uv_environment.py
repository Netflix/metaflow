from metaflow.metaflow_config import UV_VERSION
from metaflow.plugins.uv.uv_environment import UVEnvironment


def test_bootstrap_commands_embeds_datastore_type_and_uv_version(mocker):
    """The remote task doesn't inherit this process's environment, so the
    locally-resolved UV_VERSION (via METAFLOW_UV_VERSION/config overrides)
    must be baked into the bootstrap command rather than passed via env var."""
    mocker.patch("metaflow.plugins.uv.uv_environment.UV_VERSION", "9.9.9")

    env = UVEnvironment(flow=None)
    commands = env.bootstrap_commands(step_name="start", datastore_type="s3")

    bootstrap_cmd = next(cmd for cmd in commands if "uv.bootstrap" in cmd)
    assert '"s3"' in bootstrap_cmd
    assert '"9.9.9"' in bootstrap_cmd


def test_bootstrap_commands_defaults_to_configured_uv_version():
    env = UVEnvironment(flow=None)
    commands = env.bootstrap_commands(step_name="start", datastore_type="s3")

    bootstrap_cmd = next(cmd for cmd in commands if "uv.bootstrap" in cmd)
    assert '"s3"' in bootstrap_cmd
    assert f'"{UV_VERSION}"' in bootstrap_cmd
