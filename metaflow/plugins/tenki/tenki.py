import atexit
import base64
import hashlib
import json
import os
import re
import shlex
import sys
import threading
from uuid import uuid4

from metaflow import util
from metaflow.metaflow_config import (
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    AZURE_KEY_VAULT_PREFIX,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    CARD_AZUREROOT,
    CARD_GSROOT,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_AZUREROOT,
    DATATOOLS_GSROOT,
    DATATOOLS_S3ROOT,
    DEFAULT_AWS_CLIENT_PROVIDER,
    DEFAULT_AZURE_CLIENT_PROVIDER,
    DEFAULT_GCP_CLIENT_PROVIDER,
    DEFAULT_METADATA,
    DEFAULT_SECRETS_BACKEND_TYPE,
    GCP_SECRET_MANAGER_PREFIX,
    OTEL_ENDPOINT,
    OTEL_SERVICE_NAME,
    S3_ENDPOINT_URL,
    S3_SERVER_SIDE_ENCRYPTION,
    SERVICE_HEADERS,
    SERVICE_URL,
    TENKI_API_KEY,
    TENKI_BASE_URL,
    TENKI_PROJECT_ID,
    TENKI_SANDBOX_INIT_SCRIPT,
    TENKI_WORKSPACE_ID,
)
from metaflow.metaflow_config_funcs import config_values
from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
    get_log_tailer,
    tail_logs,
)

from .tenki_client import TenkiClient, TenkiException, TenkiKilledException

# Redirect structured logs to $PWD/.logs/ (identical contract to @kubernetes).
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

# Datastore credentials to forward from the launching process into the sandbox,
# keyed by datastore type. A Tenki microVM has no ambient cloud identity (unlike
# a Kubernetes pod with an IRSA service account / workload identity), so the
# datastore credentials must be passed explicitly. Prefer short-lived
# credentials whose lifetime exceeds the step run-time limit.
#
# GS additionally uses a service-account JSON *file* (GOOGLE_APPLICATION_
# CREDENTIALS); its contents are materialized inside the sandbox — see
# `_env_vars` and `_command`.
_FORWARDED_CREDENTIAL_ENV_VARS = {
    "s3": [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        # boto3-native S3 endpoint override (used by S3-compatible stores such
        # as MinIO / the Metaflow devstack).
        "AWS_ENDPOINT_URL_S3",
    ],
    "azure": [
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_SUBSCRIPTION_ID",
    ],
    "gs": [
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
        # Endpoint override for a GCS-compatible emulator (e.g. fake-gcs-server).
        "STORAGE_EMULATOR_HOST",
    ],
}
# Env var carrying the base64-encoded GCP service-account JSON into the sandbox.
_GCP_CREDENTIALS_ENV = "METAFLOW_GCP_CREDENTIALS_JSON_B64"

# Extra lifetime granted to the sandbox beyond the task's run-time limit, to
# cover bootstrap (image pull, dependency install, code-package download) and
# teardown. This is a server-side leak backstop: if the local orchestrator dies
# without cleaning up, the microVM is force-expired instead of leaking forever.
# Metaflow still enforces the real per-step timeout independently.
_SANDBOX_TTL_GRACE_SECONDS = 600

# Make the microVM a viable Metaflow runtime before the standard bootstrap runs.
# Metaflow's remote bootstrap invokes the interpreter as `python` (for the
# dependency install, the code-package download, and the step itself), but Tenki
# base images (including the platform default, a minimal non-root Ubuntu) ship
# only `python3`. Without a `python` on PATH the bootstrap fails before it can
# even download the code package. This shim, run ahead of the bootstrap:
#   * puts a writable dir on PATH and symlinks python -> python3 if `python` is
#     absent (non-root: cannot write /usr/local/bin, so use $HOME/.local/bin);
#     if neither python nor python3 exists it exits with a clear error rather
#     than letting the bootstrap fail with a confusing one later,
#   * sets PIP_BREAK_SYSTEM_PACKAGES so pip can install into a PEP-668
#     "externally-managed" system interpreter,
#   * bootstraps pip via ensurepip if it is missing.
# It is a no-op on images that already provide a usable `python`/pip. Users can
# still supply their own image via @tenki(image=) / METAFLOW_TENKI_CONTAINER_IMAGE.
_RUNTIME_BOOTSTRAP_SHIM = (
    "mkdir -p $HOME/.local/bin && export PATH=$HOME/.local/bin:$PATH && "
    "{ command -v python >/dev/null 2>&1 || "
    "{ command -v python3 >/dev/null 2>&1 && "
    "ln -sf $(command -v python3) $HOME/.local/bin/python; } || "
    "{ echo '@tenki: image provides no python runtime; set "
    "METAFLOW_TENKI_CONTAINER_IMAGE to an image with python3' >&2; exit 1; }; } && "
    "export PIP_BREAK_SYSTEM_PACKAGES=1 && "
    "{ python -m pip --version >/dev/null 2>&1 || "
    "python -m ensurepip --upgrade >/dev/null 2>&1 || true; }"
)


# Tags applied to every sandbox so `tenki list` / `tenki kill` can find them.
# The flow tag scopes cleanup to the current flow (mirrors how @kubernetes /
# @batch always filter by flow name) so an unscoped `tenki kill` can never reap
# another flow's or user's sandboxes.
TENKI_TAG = "metaflow"
TENKI_TAG_FLOW = "metaflow-flow"
TENKI_TAG_RUN = "metaflow-run"
TENKI_TAG_USER = "metaflow-user"

# Tenki tags must be <= 32 chars from [a-z0-9_:.-]. run_id and user are
# free-form (user is often an email with '@', IDs can be long), so an unescaped
# value can reject sandbox creation.
_MAX_TAG_LEN = 32
_TAG_HASH_LEN = 8


def _sanitize_name(name):
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:40] or "step"


def _describe_failure(exit_code, signal, reason, errno):
    # Human-readable diagnostic including everything the SDK reports about a
    # failed command, not just the exit code.
    parts = []
    if exit_code is not None:
        parts.append("exit code %s" % exit_code)
    if signal:
        parts.append("signal %s" % signal)
    if reason:
        parts.append("reason %s" % reason)
    if errno is not None:
        parts.append("errno %s" % errno)
    return ", ".join(parts) or "unknown status"


def _tag(key, value):
    # Build a "<key>:<value>" tag whose value is normalized to the Tenki tag
    # charset/length. When the normalized value would overflow the 32-char
    # budget it is truncated and suffixed with a short stable hash of the
    # original so distinct values stay distinct. Deterministic, so create
    # (tenki.py) and cleanup lookup (tenki_cli.py) always agree.
    budget = _MAX_TAG_LEN - len(key) - 1  # room after "<key>:"
    v = re.sub(r"[^a-z0-9_.-]", "-", str(value).lower()).strip("-")
    if len(v) > budget:
        digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:_TAG_HASH_LEN]
        head = v[: max(0, budget - _TAG_HASH_LEN - 1)].strip("-")
        v = "%s-%s" % (head, digest) if head else digest[:budget]
    return "%s:%s" % (key, v)


class Tenki(object):
    def __init__(self, datastore, metadata, environment):
        self._datastore = datastore
        self._metadata = metadata
        self._environment = environment

        self._client = None
        self._sandbox = None
        self._name = None

        # Populated by the background exec thread.
        self._exec_thread = None
        self._result = None
        self._exec_error = None
        self._output_final_logs = False

    def _command(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        code_package_metadata,
        code_package_url,
        step_cmds,
    ):
        mflog_expr = export_mflog_env_vars(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=attempt,
            datastore_type=self._datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
        )
        init_cmds = self._environment.get_package_commands(
            code_package_url, self._datastore.TYPE, code_package_metadata
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self._environment.bootstrap_commands(step_name, self._datastore.TYPE)
                + step_cmds
            )
        )

        # Construct an entrypoint that
        # 1) provisions a usable `python`/pip runtime (_RUNTIME_BOOTSTRAP_SHIM)
        # 2) initializes the mflog environment (mflog_expr)
        # 3) downloads + bootstraps the code package (init_expr)
        # 4) executes the task (step_expr)
        # then captures the exit code and persists the final logs, exiting with
        # the task's real exit code so the local orchestrator sees the right
        # status.
        cmd_str = "true && %s && mkdir -p %s && %s && %s && %s; " % (
            _RUNTIME_BOOTSTRAP_SHIM,
            LOGS_DIR,
            mflog_expr,
            init_expr,
            step_expr,
        )
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # Optional init script hook (parity with KUBERNETES_SANDBOX_INIT_SCRIPT).
        # Runs before the runtime shim so a user image that sets up its own
        # python is respected (the shim then no-ops).
        cmd_str = (
            '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"} && %s'
            % cmd_str
        )
        # For the GS datastore, recreate the service-account JSON file from the
        # forwarded base64 blob (see _env_vars) and point
        # GOOGLE_APPLICATION_CREDENTIALS at it BEFORE the code-package download
        # runs. Only emitted for gs so the s3/azure command strings are unchanged.
        if self._datastore.TYPE == "gs":
            cmd_str = (
                'if [ -n \\"$%s\\" ]; then '
                "printf %%s $%s | base64 -d > $HOME/mf_gcp_creds.json; "
                "export GOOGLE_APPLICATION_CREDENTIALS=$HOME/mf_gcp_creds.json; fi "
                "&& %s"
            ) % (_GCP_CREDENTIALS_ENV, _GCP_CREDENTIALS_ENV, cmd_str)
        # The inner commands (get_package_commands, mflog, bash_capture_logs)
        # emit \\"-escaped quotes that only resolve correctly when the whole
        # string is unwrapped from a `bash -c "..."` invocation. We reproduce
        # @kubernetes' exact transformation and hand the resulting argv list to
        # `sb.exec(*argv)`.
        return shlex.split('bash -c "%s"' % cmd_str)

    def _env_vars(
        self,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        code_package_ds,
        user,
        user_env,
    ):
        env = {
            "METAFLOW_CODE_METADATA": code_package_metadata,
            "METAFLOW_CODE_SHA": code_package_sha,
            "METAFLOW_CODE_URL": code_package_url,
            "METAFLOW_CODE_DS": code_package_ds,
            "METAFLOW_USER": user,
            # A Tenki microVM lives outside the cluster network, so it must use
            # the externally-reachable service URL rather than the cluster
            # internal one (@kubernetes uses SERVICE_INTERNAL_URL).
            "METAFLOW_SERVICE_URL": SERVICE_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_DEFAULT_DATASTORE": self._datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
            "METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE": DEFAULT_SECRETS_BACKEND_TYPE,
            # Secrets-backend config so @secrets resolves the same way it does
            # under @kubernetes (per-provider region/prefix).
            "METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION": (
                AWS_SECRETS_MANAGER_DEFAULT_REGION
            ),
            "METAFLOW_GCP_SECRET_MANAGER_PREFIX": GCP_SECRET_MANAGER_PREFIX,
            "METAFLOW_AZURE_KEY_VAULT_PREFIX": AZURE_KEY_VAULT_PREFIX,
            "METAFLOW_RUNTIME_ENVIRONMENT": "tenki",
            # Marker used by TenkiDecorator.task_pre_step / task_finished to
            # detect that it is running inside the remote workload.
            "METAFLOW_TENKI_WORKLOAD": "1",
            "METAFLOW_INIT_SCRIPT": TENKI_SANDBOX_INIT_SCRIPT,
            # S3 datastore config.
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
            "METAFLOW_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "METAFLOW_DEFAULT_AWS_CLIENT_PROVIDER": DEFAULT_AWS_CLIENT_PROVIDER,
            # Server-side encryption for S3 datastore writes (dropped if unset).
            "METAFLOW_S3_SERVER_SIDE_ENCRYPTION": S3_SERVER_SIDE_ENCRYPTION,
            # Azure datastore config.
            "METAFLOW_DATASTORE_SYSROOT_AZURE": DATASTORE_SYSROOT_AZURE,
            "METAFLOW_DATATOOLS_AZUREROOT": DATATOOLS_AZUREROOT,
            "METAFLOW_CARD_AZUREROOT": CARD_AZUREROOT,
            "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT": (
                AZURE_STORAGE_BLOB_SERVICE_ENDPOINT
            ),
            "METAFLOW_DEFAULT_AZURE_CLIENT_PROVIDER": DEFAULT_AZURE_CLIENT_PROVIDER,
            # GS datastore config.
            "METAFLOW_DATASTORE_SYSROOT_GS": DATASTORE_SYSROOT_GS,
            "METAFLOW_DATATOOLS_GSROOT": DATATOOLS_GSROOT,
            "METAFLOW_CARD_GSROOT": CARD_GSROOT,
            "METAFLOW_DEFAULT_GCP_CLIENT_PROVIDER": DEFAULT_GCP_CLIENT_PROVIDER,
            # Telemetry/tracing config (parity with @kubernetes).
            "METAFLOW_OTEL_ENDPOINT": OTEL_ENDPOINT,
            "METAFLOW_OTEL_SERVICE_NAME": OTEL_SERVICE_NAME,
        }

        # Temporary passing of *some* environment variables. Do not rely on this
        # mechanism as it will be removed in the near future (copied verbatim
        # from @kubernetes).
        for k, v in config_values():
            if k.startswith("METAFLOW_CONDA_") or k.startswith("METAFLOW_DEBUG_"):
                env[k] = v

        # Forward the datastore's credentials into the microVM.
        for k in _FORWARDED_CREDENTIAL_ENV_VARS.get(self._datastore.TYPE, []):
            if os.environ.get(k) is not None:
                env[k] = os.environ[k]

        # GS authenticates via a service-account JSON *file*
        # (GOOGLE_APPLICATION_CREDENTIALS). Forward its contents (base64) so the
        # sandbox can recreate the file before the code-package download runs.
        if self._datastore.TYPE == "gs":
            gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if gac and os.path.isfile(gac):
                with open(gac, "rb") as f:
                    env[_GCP_CREDENTIALS_ENV] = base64.b64encode(f.read()).decode()

        # User-specified environment (from @environment).
        for k, v in (user_env or {}).items():
            env[k] = v

        # Drop unset values so we never hand `None` to the SDK.
        return {k: str(v) for k, v in env.items() if v is not None}

    def launch_job(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        user,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        code_package_ds,
        step_cli,
        image=None,
        cpu=None,
        memory=None,
        run_time_limit=None,
        env=None,
        **kwargs
    ):
        bash_argv = self._command(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            attempt=attempt,
            code_package_metadata=code_package_metadata,
            code_package_url=code_package_url,
            step_cmds=[step_cli],
        )
        # Unique name per attempt so a retry never collides with a still
        # terminating prior sandbox.
        self._name = "mf-%s-%s-a%s-%s" % (
            _sanitize_name("%s-%s" % (run_id, step_name)),
            _sanitize_name(str(task_id)),
            attempt,
            str(uuid4())[:8],
        )

        full_env = self._env_vars(
            code_package_metadata,
            code_package_sha,
            code_package_url,
            code_package_ds,
            user,
            env,
        )
        # Expose the sandbox name to the in-VM workload so task_pre_step can
        # register it as task metadata for debugging.
        full_env["METAFLOW_TENKI_SANDBOX_NAME"] = self._name

        self._client = TenkiClient(api_key=TENKI_API_KEY, base_url=TENKI_BASE_URL)

        # The Tenki API requires a project to create a sandbox in. Use the
        # configured project, else auto-resolve the token's first project.
        project_id = TENKI_PROJECT_ID or self._client.default_project_id(
            TENKI_WORKSPACE_ID
        )

        # Outbound access is REQUIRED so the microVM can reach the datastore
        # (S3) and the metadata service.
        create_kwargs = dict(
            name=self._name,
            # Resource values arrive as strings and may be float-formatted
            # (e.g. "1.0") when merged with @resources, so parse via float.
            cpu_cores=int(float(cpu)),
            memory_mb=int(float(memory)),
            allow_outbound=True,
            # Tag/annotate the sandbox so orphans can be found and terminated
            # by `tenki kill` (the deterministic cleanup path for crashes where
            # in-process teardown never ran).
            tags=[
                TENKI_TAG,
                _tag(TENKI_TAG_FLOW, flow_name),
                _tag(TENKI_TAG_RUN, run_id),
                _tag(TENKI_TAG_USER, user),
            ],
            metadata={
                "metaflow.flow_name": flow_name,
                "metaflow.run_id": str(run_id),
                "metaflow.step_name": step_name,
                "metaflow.task_id": str(task_id),
                "metaflow.attempt": str(attempt),
                "metaflow.user": user,
            },
        )
        if project_id:
            create_kwargs["project_id"] = project_id
        if TENKI_WORKSPACE_ID:
            create_kwargs["workspace_id"] = TENKI_WORKSPACE_ID
        if image:
            create_kwargs["image"] = image
        if run_time_limit:
            # Server-side hard cap (seconds); see the constant.
            create_kwargs["max_duration"] = (
                int(float(run_time_limit)) + _SANDBOX_TTL_GRACE_SECONDS
            )
        self._sandbox = self._client.create_sandbox(**create_kwargs)
        # Best-effort teardown of the disposable microVM even if the local
        # orchestrator crashes hard (mirrors @kubernetes best_effort_kill).
        # This is the last-chance pass, so let it surface a warning if teardown
        # still fails after wait()'s finally already retried.
        atexit.register(self._cleanup, final=True)

        def _run():
            try:
                self._result = self._sandbox.exec(
                    *bash_argv,
                    env=full_env,
                    timeout=int(float(run_time_limit)) if run_time_limit else None,
                )
            except BaseException as e:  # noqa: B902 - capture to re-raise in wait()
                self._exec_error = e

        self._exec_thread = threading.Thread(target=_run)
        self._exec_thread.daemon = True
        self._exec_thread.start()

    def wait(self, stdout_location, stderr_location, echo=None):
        # Tail structured logs from the datastore (S3) on the main thread while
        # the blocking `sb.exec` runs in the background thread. Without this the
        # user would see no output until the (possibly hours-long) step ends.
        prefix = b"[%s] " % util.to_bytes(self._name)
        stdout_tail = get_log_tailer(stdout_location, self._datastore.TYPE)
        stderr_tail = get_log_tailer(stderr_location, self._datastore.TYPE)

        self._output_final_logs = False

        def _has_updates():
            if self._exec_thread.is_alive():
                return True
            # Emit a final tail once the exec has finished.
            if not self._output_final_logs:
                self._output_final_logs = True
                return True
            return False

        try:
            tail_logs(
                prefix=prefix,
                stdout_tail=stdout_tail,
                stderr_tail=stderr_tail,
                echo=echo,
                has_log_updates=_has_updates,
            )
            self._exec_thread.join()
            self._interpret_result(echo)
        finally:
            self._cleanup()

    def _interpret_result(self, echo):
        # Infra failures raised by the SDK during exec. Classify retryable vs
        # non-retryable. A non-retryable failure is surfaced as
        # TenkiKilledException, which the CLI maps to METAFLOW_EXIT_DISALLOW_RETRY.
        err = self._exec_error
        if err is not None:
            if isinstance(err, self._client.exception("PermissionDeniedError")):
                raise TenkiKilledException(
                    "Tenki denied the request (%s). This is a configuration or "
                    "credential problem and will not be retried." % err
                )
            # A run-time-limit breach surfaces as the SDK's CommandTimeoutError
            # or, for the client-side exec deadline, a builtin TimeoutError.
            timeout_types = (TimeoutError,)
            sdk_timeout = self._client.exception("CommandTimeoutError")
            if sdk_timeout:
                timeout_types = timeout_types + (sdk_timeout,)
            if isinstance(err, timeout_types):
                raise TenkiException(
                    "Task timed out. This could be a transient error. "
                    "Use @retry to retry."
                )
            if isinstance(err, self._client.exception("SessionNotFoundError")):
                raise TenkiException(
                    "The Tenki sandbox disappeared before the task finished. "
                    "This could be a transient error. Use @retry to retry."
                )
            raise TenkiException(
                "Failed to execute the task in the Tenki sandbox: %s" % err
            )

        result = self._result
        exit_code = getattr(result, "exit_code", None)
        signal = getattr(result, "signal", None)
        reason = getattr(result, "reason", None)
        errno = getattr(result, "errno", None)
        # The SDK treats a command as successful only when it exited 0 AND was
        # not killed by a signal (result.ok). Fall back to that definition if
        # the property is unavailable so a signalled task is never read as OK.
        ok = getattr(result, "ok", None)
        if ok is None:
            ok = exit_code == 0 and not signal

        # Surface captured stderr on failure even if the in-VM `save_logs` never
        # ran (e.g. a hard crash before the tail flush).
        stderr_text = getattr(result, "stderr_text", None)
        if not ok and stderr_text:
            echo(stderr_text, "stderr")

        if ok:
            echo("Task finished with exit code %s." % exit_code, "stderr")
            return

        if exit_code is None and not signal:
            # Unknown status (no code and no signal). Treat as a retryable
            # failure, not success.
            raise TenkiException(
                "Task ended with an unknown status. This could be a "
                "transient error. Use @retry to retry."
            )

        details = _describe_failure(exit_code, signal, reason, errno)
        if exit_code == 137:
            raise TenkiException(
                "Task ran out of memory (%s). Increase the available memory by "
                "specifying @resources(memory=...) for the step." % details
            )
        if exit_code == 139:
            raise TenkiException(
                "Task failed with a segmentation fault (%s)." % details
            )
        raise TenkiException(
            "This could be a transient error (%s). Use @retry to retry." % details
        )

    def _cleanup(self, final=False):
        sb = self._sandbox
        if sb is not None:
            # Tear down the disposable microVM. Try close first, then fall back
            # to terminate (aliases in the SDK). Keep self._sandbox set until a
            # teardown call actually succeeds: if this pass fails (e.g. a
            # transient API error in wait()'s finally), the atexit hook can
            # still retry against a live handle.
            terminated = False
            last_error = None
            for method in ("close", "terminate", "delete"):
                fn = getattr(sb, method, None)
                if fn is None:
                    continue
                try:
                    fn()
                    terminated = True
                    break
                except Exception as e:
                    last_error = e
            if not terminated:
                # Keep both the sandbox and client handles so a later pass can
                # retry (sb teardown needs the still-open client). Only warn on
                # the final (atexit) pass, once no retry remains, so a transient
                # failure that the retry resolves doesn't raise a false alarm.
                if final:
                    sys.stderr.write(
                        "[@tenki] WARNING: could not terminate sandbox %s: %s\n"
                        "It may keep running until its server-side max_duration "
                        "expires; run `python <flow> tenki kill` to remove "
                        "orphans.\n" % (getattr(sb, "id", "<unknown>"), last_error)
                    )
                return
            self._sandbox = None
        client = self._client
        if client is not None:
            self._client = None
            client.close()
