from types import SimpleNamespace

from metaflow._vendor import click
from metaflow._vendor.click.testing import CliRunner
from metaflow.plugins.logs_cli import CustomGroup


def _make_logs_group(calls=None):
    @click.group(name="logs", cls=CustomGroup, default_cmd="show")
    def logs_group():
        pass

    @logs_group.command()
    @click.argument("input-path")
    @click.option("--stdout/--no-stdout", default=False, show_default=True)
    @click.option("--stderr/--no-stderr", default=False, show_default=True)
    @click.option("--both/--no-both", default=True, show_default=True)
    @click.option("--timestamps/--no-timestamps", default=False, show_default=True)
    @click.option("--attempt", default=None, type=int)
    @click.pass_obj
    def show(obj, input_path, **kwargs):
        if calls is not None:
            calls.append((input_path, kwargs))

    @logs_group.command()
    def scrub():
        pass

    return logs_group


def test_logs_help_does_not_include_show_help():
    logs_group = _make_logs_group()
    result = CliRunner().invoke(logs_group, ["--help"])

    assert result.exit_code == 0
    assert "Usage: " in result.output
    assert "logs [OPTIONS] COMMAND [ARGS]" in result.output
    assert "logs show [OPTIONS] INPUT_PATH" not in result.output


def test_logs_show_help_still_works():
    logs_group = _make_logs_group()
    obj = SimpleNamespace(echo=lambda *args, **kwargs: None)
    result = CliRunner().invoke(logs_group, ["show", "--help"], obj=obj)

    assert result.exit_code == 0
    assert "logs show [OPTIONS] INPUT_PATH" in result.output


def test_logs_legacy_pathspec_routes_to_default_show():
    calls = []
    logs_group = _make_logs_group(calls)

    obj = SimpleNamespace(
        echo=lambda *args, **kwargs: None,
        flow_datastore=object(),
    )
    result = CliRunner().invoke(logs_group, ["123/start/1"], obj=obj)

    assert result.exit_code == 0
    assert calls == [
        (
            "123/start/1",
            {
                "stdout": False,
                "stderr": False,
                "both": True,
                "timestamps": False,
                "attempt": None,
            },
        )
    ]


def test_logs_legacy_show_option_routes_to_default_show():
    calls = []
    logs_group = _make_logs_group(calls)

    obj = SimpleNamespace(
        echo=lambda *args, **kwargs: None,
        flow_datastore=object(),
    )
    result = CliRunner().invoke(logs_group, ["--stdout", "123/start/1"], obj=obj)

    assert result.exit_code == 0
    assert calls == [
        (
            "123/start/1",
            {
                "stdout": True,
                "stderr": False,
                "both": True,
                "timestamps": False,
                "attempt": None,
            },
        )
    ]
