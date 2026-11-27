"""Parameter values exported for Argo Workflows, Airflow and Step Functions
must reach the `METAFLOW_INIT_*` environment variables unchanged when the
generated export file is sourced by bash."""

import base64
import json
import shutil
import subprocess

import pytest

from metaflow.plugins.airflow.plumbing import set_parameters as airflow_params
from metaflow.plugins.argo import set_parameters as argo_params
from metaflow.plugins.aws.step_functions import set_batch_environment as sfn_params

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

# (parameter value, value the step must see in METAFLOW_INIT_P)
VALUES = [
    ("hello world", "hello world"),
    (["a", "b"], '["a", "b"]'),
    ({"k": 1, "j": "x"}, '{"k": 1, "j": "x"}'),
    ("$(pwd)", "$(pwd)"),
    ("`pwd`", "`pwd`"),
    ("${HOME}", "${HOME}"),
    ("it's", "it's"),
    ("line1\nline2", "line1\nline2"),
    ("café", "café"),
    ('["a", "b"]', '["a", "b"]'),
    (5, "5"),
    (True, "true"),
]
VALUE_IDS = [
    "string-with-space",
    "list",
    "dict",
    "command-substitution",
    "backticks",
    "variable-expansion",
    "single-quote",
    "newline",
    "non-ascii",
    "json-string",
    "int",
    "bool",
]


def _sourced_env(export_file, name):
    """Source `export_file` in bash and return the value of env var `name`."""
    out = subprocess.run(
        ["bash", "-c", '. "$0" && printf "%s" "${!1}"', str(export_file), name],
        capture_output=True,
        check=True,
    )
    assert out.stderr == b""
    return out.stdout.decode("utf-8")


def _export_argo(path, value, monkeypatch):
    encoded = base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")
    argo_params.export_parameters(str(path), {"p": (encoded, False)})


def _export_airflow(path, value, monkeypatch):
    monkeypatch.setenv("METAFLOW_PARAMETERS", json.dumps({"p": value}))
    airflow_params.export_parameters(str(path))


def _export_sfn(path, value, monkeypatch):
    monkeypatch.setenv("METAFLOW_PARAMETERS", json.dumps({"p": value}))
    monkeypatch.delenv("METAFLOW_DEFAULT_PARAMETERS", raising=False)
    sfn_params.export_parameters(str(path))


@pytest.mark.parametrize(
    "export",
    [_export_argo, _export_airflow, _export_sfn],
    ids=["argo", "airflow", "step-functions"],
)
@pytest.mark.parametrize("value, expected", VALUES, ids=VALUE_IDS)
def test_exported_value_reaches_env_unchanged(
    tmp_path, monkeypatch, export, value, expected
):
    export_file = tmp_path / "params.sh"
    export(export_file, value, monkeypatch)
    assert _sourced_env(export_file, "METAFLOW_INIT_P") == expected


def test_sourcing_does_not_execute_parameter_values(tmp_path, monkeypatch):
    marker = tmp_path / "pwned"
    export_file = tmp_path / "params.sh"
    _export_airflow(export_file, ["$(touch %s)" % marker, "b"], monkeypatch)
    _sourced_env(export_file, "METAFLOW_INIT_P")
    assert not marker.exists()


def test_argo_json_string_default_is_exported_as_its_json(tmp_path):
    # Argo stores JSONType defaults as a JSON-encoded string, so the step
    # must see the inner JSON document.
    stored = json.dumps(json.dumps(["a", "b"]))
    encoded = base64.b64encode(stored.encode("utf-8")).decode("ascii")
    export_file = tmp_path / "params.sh"
    argo_params.export_parameters(str(export_file), {"p": (encoded, False)})
    assert json.loads(_sourced_env(export_file, "METAFLOW_INIT_P")) == ["a", "b"]


@pytest.mark.parametrize(
    "stored, none_default, expected",
    [
        ("null", True, None),
        ("null", False, "null"),
        ('"null"', True, "null"),
    ],
    ids=["none-default-skipped", "null-value-exported", "string-null-exported"],
)
def test_argo_none_default_handling(tmp_path, stored, none_default, expected):
    encoded = base64.b64encode(stored.encode("utf-8")).decode("ascii")
    export_file = tmp_path / "params.sh"
    argo_params.export_parameters(str(export_file), {"p": (encoded, none_default)})
    content = export_file.read_text(encoding="utf-8")
    if expected is None:
        assert content == ""
    else:
        assert _sourced_env(export_file, "METAFLOW_INIT_P") == expected
