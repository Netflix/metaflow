import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from xml.sax.saxutils import escape

import pytest

from metaflow._vendor import click
from metaflow._vendor.click.testing import CliRunner
from metaflow.exception import MetaflowException
from metaflow.plugins.datatools.s3 import s3 as s3_module
from metaflow.plugins.datatools.s3 import s3op, s3util
from metaflow.plugins.datatools.s3.s3 import S3, MetaflowS3NotFound


@pytest.fixture
def secret_params():
    return {
        "aws_access_key_id": "dummy-access-key",
        "aws_secret_access_key": "dummy-secret-key",
        "aws_session_token": "dummy-session-token",
    }


@pytest.fixture
def capture_cli():
    received = []

    @click.command()
    @s3op.common_options
    def command(**kwargs):
        received.append(json.loads(kwargs["s3clientparams"]))

    return command, received


@pytest.fixture
def local_s3(monkeypatch):
    """Exercise real boto3 requests and transfer subprocesses without AWS."""
    objects = {"/bucket/one": b"one", "/bucket/two": b"two"}
    requests = []
    monkeypatch.setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, body, status=200):
            requests.append(dict(self.headers))
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            self.send_header("ETag", '"test-etag"')
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):
            self.respond(
                objects.get(self.path, b""), 200 if self.path in objects else 404
            )

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if "list-type" in query:
                prefix = query.get("prefix", [""])[0]
                entries = "".join(
                    "<Contents><Key>%s</Key><Size>%d</Size></Contents>"
                    % (escape(path[len("/bucket/") :]), len(body))
                    for path, body in sorted(objects.items())
                    if path.startswith("/bucket/" + prefix)
                )
                self.respond(
                    (
                        "<ListBucketResult><IsTruncated>false</IsTruncated>%s"
                        "</ListBucketResult>" % entries
                    ).encode()
                )
            else:
                self.respond(
                    objects.get(parsed.path, b""),
                    200 if parsed.path in objects else 404,
                )

        def do_PUT(self):
            objects[urlparse(self.path).path] = self.rfile.read(
                int(self.headers["Content-Length"])
            )
            self.respond(b"")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "endpoint_url": "http://127.0.0.1:%d" % server.server_port,
            "region_name": "us-east-1",
            "config": {
                "s3": {"addressing_style": "path"},
                "connect_timeout": 2,
                "read_timeout": 2,
                "retries": {"max_attempts": 0},
            },
        }, objects, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("mode", ["list", "get", "put", "info"])
def test_secret_parameters_use_stdin_and_stay_out_of_debug_logs(
    mode, tmp_path, secret_params, mocker
):
    run = mocker.patch.object(s3_module.subprocess, "check_output", return_value=b"")
    debug_log = mocker.patch.object(s3_module.debug, "s3client_exec")
    with S3(
        tmproot=str(tmp_path),
        client_params={"region_name": "us-east-1"},
        secret_client_params=secret_params,
    ) as client:
        client._s3op_with_retries(mode, inputs="unused")
    command = run.call_args.args[0]
    assert "--secret-client-params-stdin" in command
    assert json.loads(run.call_args.kwargs["input"]) == secret_params
    assert "us-east-1" in " ".join(command)
    for value in secret_params.values():
        assert value not in " ".join(command)
        assert value not in str(debug_log.call_args_list)


def test_callers_without_secrets_keep_the_existing_subprocess_transport(
    tmp_path, mocker
):
    run = mocker.patch.object(s3_module.subprocess, "check_output", return_value=b"")
    with S3(
        tmproot=str(tmp_path), client_params={"region_name": "us-east-1"}
    ) as client:
        client._s3op_with_retries("list", inputs="unused")
    assert "input" not in run.call_args.kwargs
    assert "--secret-client-params-stdin" not in run.call_args.args[0]
    assert "--s3clientparams" in run.call_args.args[0]


def test_secret_transport_support_can_be_detected_by_extensions():
    assert S3.SUPPORTS_SECRET_CLIENT_PARAMS is True


def test_legacy_aws_credentials_are_redacted_in_debug_logs(
    tmp_path, secret_params, mocker
):
    run = mocker.patch.object(s3_module.subprocess, "check_output", return_value=b"")
    log = mocker.patch.object(s3_module.debug, "s3client_exec")
    with S3(tmproot=str(tmp_path), client_params=secret_params) as client:
        client._s3op_with_retries("list", inputs="unused")
    for value in secret_params.values():
        assert value not in str(log.call_args_list)
        # Legacy callers retain their existing argv transport.
        assert value in " ".join(run.call_args.args[0])


def test_secret_parameters_are_resent_when_a_transfer_is_retried(
    tmp_path, secret_params, mocker
):
    def transfer(command, **kwargs):
        if run.call_count == 1:
            kwargs["stderr"].write(
                (s3util.TRANSIENT_RETRY_START_LINE + "\n").encode() + b'{"idx": 0}\n'
            )
            kwargs["stderr"].flush()
            raise subprocess.CalledProcessError(
                s3op.ERROR_TRANSIENT, command, output=b"0 <none>\n"
            )
        return b"0 s3://bucket/one local-file\n"

    run = mocker.patch.object(
        s3_module.subprocess, "check_output", side_effect=transfer
    )
    with S3(tmproot=str(tmp_path), secret_client_params=secret_params) as client:
        mocker.patch.object(client, "_jitter_sleep")
        lines, error, _ = client._s3op_with_retries("get", inputs="unused")
    assert error is None
    assert lines == [b"s3://bucket/one local-file"]
    assert run.call_count == 2
    assert all(
        json.loads(call.kwargs["input"]) == secret_params for call in run.call_args_list
    )


@pytest.mark.parametrize(
    "value", ["not-json-dummy-secret", '["dummy-secret"]', '"dummy-secret"', "null"]
)
def test_invalid_stdin_is_rejected_without_echoing_its_contents(value, capture_cli):
    command, received = capture_cli
    result = CliRunner().invoke(command, ["--secret-client-params-stdin"], input=value)
    assert result.exit_code != 0
    assert "Invalid or overlapping secret client parameters" in result.output
    assert "dummy-secret" not in result.output
    assert received == []


def test_cli_merges_secret_parameters_without_mutating_inputs(
    secret_params, capture_cli
):
    command, received = capture_cli
    ordinary = {"region_name": "us-east-1"}
    result = CliRunner().invoke(
        command,
        ["--s3clientparams", json.dumps(ordinary), "--secret-client-params-stdin"],
        input=json.dumps(secret_params),
    )
    assert result.exit_code == 0, result.output
    assert received == [dict(ordinary, **secret_params)]
    assert ordinary == {"region_name": "us-east-1"}


def test_overlapping_parameters_are_rejected_before_launching_a_child(tmp_path, mocker):
    run = mocker.patch.object(s3_module.subprocess, "check_output")
    with pytest.raises(MetaflowException, match="overlapping keys"):
        S3(
            tmproot=str(tmp_path),
            client_params={"aws_session_token": "ordinary-token"},
            secret_client_params={"aws_session_token": "secret-token"},
        )
    run.assert_not_called()


@pytest.mark.parametrize("secret_params", [[], "token", 1])
def test_secret_parameters_must_be_a_dictionary(tmp_path, secret_params):
    with pytest.raises(MetaflowException, match="must be a dictionary"):
        S3(tmproot=str(tmp_path), secret_client_params=secret_params)


def test_overlapping_stdin_parameters_are_rejected_without_echoing_values(capture_cli):
    command, received = capture_cli
    result = CliRunner().invoke(
        command,
        [
            "--s3clientparams",
            '{"aws_session_token": "ordinary-token"}',
            "--secret-client-params-stdin",
        ],
        input='{"aws_session_token": "secret-token"}',
    )
    assert result.exit_code != 0
    assert "ordinary-token" not in result.output
    assert "secret-token" not in result.output
    assert received == []


def test_configured_defaults_are_preserved_and_cannot_override_secrets(monkeypatch):
    monkeypatch.setattr(s3util, "DATATOOLS_CLIENT_PARAMS", {"region_name": "us-east-1"})
    assert s3util.merge_client_params(None, {"aws_session_token": "token"}) == {
        "region_name": "us-east-1",
        "aws_session_token": "token",
    }
    with pytest.raises(MetaflowException, match="overlapping keys"):
        s3util.merge_client_params(None, {"region_name": "secret"})


def test_secrets_work_for_single_and_bulk_transfers_without_appearing_in_logs(
    tmp_path, local_s3, secret_params, mocker, capfd
):
    params, objects, requests = local_s3
    check_output = subprocess.check_output

    def bounded_transfer(*args, **kwargs):
        return check_output(*args, timeout=20, **kwargs)

    run = mocker.patch.object(
        s3_module.subprocess, "check_output", side_effect=bounded_transfer
    )
    mocker.patch.object(s3_module.debug, "s3client", True)
    mocker.patch.object(
        s3_module.debug,
        "s3client_exec",
        side_effect=lambda args: print("debug:", args),
    )
    local = tmp_path / "upload"
    local.write_bytes(b"uploaded")
    with S3(
        tmproot=str(tmp_path), client_params=params, secret_client_params=secret_params
    ) as client:
        assert client.get("s3://bucket/one").blob == b"one"
        assert [
            obj.blob for obj in client.get_many(["s3://bucket/one", "s3://bucket/two"])
        ] == [b"one", b"two"]
        client.put_files([("s3://bucket/upload", str(local))])
        assert objects["/bucket/upload"] == b"uploaded"
        assert {obj.url for obj in client.list_paths(["s3://bucket/"])} == {
            "s3://bucket/one",
            "s3://bucket/two",
            "s3://bucket/upload",
        }
        assert client.info_many(["s3://bucket/one"])[0].size == 3
        with pytest.raises(MetaflowS3NotFound):
            client.get_many(["s3://bucket/missing"])
    assert run.call_count == 5
    assert requests
    for headers in requests:
        assert secret_params["aws_access_key_id"] in headers["Authorization"]
        assert headers["X-Amz-Security-Token"] == secret_params["aws_session_token"]
    output = capfd.readouterr()
    for value in secret_params.values():
        assert value not in output.out + output.err
        assert all(value not in " ".join(call.args[0]) for call in run.call_args_list)


def test_existing_bulk_transfers_still_accept_client_params(
    tmp_path, local_s3, secret_params
):
    params, _, _ = local_s3
    with S3(
        tmproot=str(tmp_path), client_params=dict(params, **secret_params)
    ) as client:
        assert [obj.blob for obj in client.get_many(["s3://bucket/one"])] == [b"one"]


def test_secret_payload_larger_than_a_pipe_buffer_does_not_hang(tmp_path, mocker):
    check_output = subprocess.check_output

    def bounded_transfer(*args, **kwargs):
        return check_output(*args, timeout=20, **kwargs)

    mocker.patch.object(
        s3_module.subprocess, "check_output", side_effect=bounded_transfer
    )
    inputs = tmp_path / "empty-inputs"
    inputs.write_text("")
    with S3(
        tmproot=str(tmp_path),
        secret_client_params={"custom_secret": "x" * (256 * 1024)},
    ) as client:
        lines, error, _ = client._s3op_with_retries("list", inputs=str(inputs))
    assert lines == []
    assert error is None
