from types import SimpleNamespace

import pytest

from metaflow._vendor import click
from metaflow._vendor.click.testing import CliRunner
from metaflow.plugins.logs_cli import CustomGroup


INPUT_PATH = "123/start/1"


@pytest.fixture
def make_logs_group():
    def _make_logs_group(show_callback=None):
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
            if show_callback is not None:
                show_callback(input_path, **kwargs)

        @logs_group.command()
        def scrub():
            pass

        return logs_group

    return _make_logs_group


@pytest.fixture
def logs_obj():
    return SimpleNamespace(
        echo=lambda *args, **kwargs: None,
        flow_datastore=object(),
    )


def test_logs_help_does_not_include_show_help(make_logs_group):
    logs_group = make_logs_group()
    result = CliRunner().invoke(logs_group, ["--help"])

    assert result.exit_code == 0
    assert "Usage: " in result.output
    assert "logs [OPTIONS] COMMAND [ARGS]" in result.output
    assert "logs show [OPTIONS] INPUT_PATH" not in result.output


def test_logs_show_help_still_works(make_logs_group, logs_obj):
    logs_group = make_logs_group()
    result = CliRunner().invoke(logs_group, ["show", "--help"], obj=logs_obj)

    assert result.exit_code == 0
    assert "logs show [OPTIONS] INPUT_PATH" in result.output


@pytest.mark.parametrize(
    "args, expected_options",
    [
        (
            [INPUT_PATH],
            {
                "stdout": False,
                "stderr": False,
                "both": True,
                "timestamps": False,
                "attempt": None,
            },
        ),
        (
            ["--stdout", INPUT_PATH],
            {
                "stdout": True,
                "stderr": False,
                "both": True,
                "timestamps": False,
                "attempt": None,
            },
        ),
    ],
    ids=["pathspec", "show-option"],
)
def test_logs_legacy_args_route_to_default_show(
    make_logs_group, logs_obj, mocker, args, expected_options
):
    show_callback = mocker.Mock()
    logs_group = make_logs_group(show_callback)

    result = CliRunner().invoke(logs_group, args, obj=logs_obj)

    assert result.exit_code == 0
    show_callback.assert_called_once_with(INPUT_PATH, **expected_options)
