"""
debugger_step_decorator.py — VSCode/debugpy integration for Metaflow tasks.

Enables interactive debugging of every task subprocess. Two modes depending on
whether the flow runs locally or on remote compute (Kubernetes/Batch):

LOCAL MODE (mode="connect")
===========================

    Developer machine
    ┌──────────────────────────────────────────────┐
    │                                              │
    │  VSCode  ◄──DAP──►  debugpy adapter (:5678)  │
    │                       ▲          ▲           │
    │                       │          │           │
    │                    (pydevd     (pydevd       │
    │                   protocol)   protocol)      │
    │                       │          │           │
    │                   task (pid1)  task (pid2)   │
    │                                              │
    └──────────────────────────────────────────────┘

  1. runtime_init: starts the debugpy adapter via debugpy.listen().
  2. runtime_step_cli: passes adapter host/port to each task via env vars.
  3. task_pre_step (_task_connect): each task calls debugpy.connect() back to
     the adapter, which notifies VSCode to auto-attach a new debug session.


REMOTE MODE (mode="listen")
===========================

    Developer machine                          Remote container
    ┌─────────────────────────────┐            ┌─────────────────┐
    │                             │            │                 │
    │  VSCode ◄─DAP─► adapter    │            │   pydevd (:5678)│
    │                  ▲          │            │     ▲           │
    │               (pydevd       │            │     │           │
    │              protocol)      │            │   task code     │
    │                  │          │            │     │           │
    │              bridge ◄───────╂── TCP ─────╂─────┘           │
    │                  ▲          │            │                 │
    │                  │          │            │                 │
    │          callback server    │◄── TCP ────╂── callback      │
    │          (ephemeral port)   │  (endpoint │  (host:port)    │
    │                             │    JSON)   │                 │
    └─────────────────────────────┘            └─────────────────┘

  1. runtime_init: starts debugpy.listen() + a callback server on an
     ephemeral port. Passes callback host/port to tasks via env vars.

  2. task_pre_step (_task_listen): the remote task:
     a. Pre-binds a server socket on base_port.
     b. Sends its {host, port} as JSON to the callback server.
     c. Starts pydevd.settrace() in server mode (reusing the pre-bound socket).

  3. _handle_callback (on the developer machine, per task):
     a. Reads the task's {host, port} from the callback connection.
     b. Opens a "bridge" socket to the adapter's internal pydevd server.
     c. Completes the 2-message DAP handshake that the adapter expects
        (pydevdAuthorize + pydevdSystemInfo) so the adapter registers
        a new debug session.
     d. Connects to the remote task's pydevd and pipes traffic in both
        directions:  adapter <--bridge--> remote pydevd.

  This makes each remote task appear as a local child process to the adapter,
  so VSCode auto-attaches seamlessly.


LIFECYCLE (StepDecorator hooks)
===============================

  step_init        → verify debugpy is importable
  runtime_init     → start adapter + callback server, print banner
  runtime_step_cli → inject env vars for the task subprocess
  task_pre_step    → connect or listen depending on mode
  runtime_finished → reset class-level state
"""

import json
import os
import socket
import sys
import threading

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

_ENV_ADAPTER_HOST = "METAFLOW_DEBUGPY_ADAPTER_HOST"
_ENV_ADAPTER_PORT = "METAFLOW_DEBUGPY_ADAPTER_PORT"
_ENV_PARENT_PID = "METAFLOW_DEBUGPY_PARENT_PID"
_ENV_WAIT_FOR_CLIENT = "METAFLOW_DEBUGPY_WAIT_FOR_CLIENT"
_ENV_DEBUG_MODE = "METAFLOW_DEBUGPY_MODE"
_ENV_BASE_PORT = "METAFLOW_DEBUGPY_BASE_PORT"
_ENV_CALLBACK_HOST = "METAFLOW_DEBUGPY_CALLBACK_HOST"
_ENV_CALLBACK_PORT = "METAFLOW_DEBUGPY_CALLBACK_PORT"
_ENV_ACCESS_TOKEN = "METAFLOW_DEBUGPY_ACCESS_TOKEN"

_LOG_PREFIX = "[DEBUGGER]"


def _log(msg, *args):
    if args:
        msg = msg % args
    sys.stderr.write("%s %s\n" % (_LOG_PREFIX, msg))
    sys.stderr.flush()


def _read_dap_message(sock):
    """Read one DAP-framed JSON message from *sock*."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("socket closed while reading DAP header")
        buf += chunk
    header, _ = buf.split(b"\r\n\r\n", 1)
    content_length = None
    for line in header.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            content_length = int(line.split(b":", 1)[1].strip())
            break
    if content_length is None:
        raise ValueError("DAP message missing Content-Length header")
    body = b""
    while len(body) < content_length:
        chunk = sock.recv(content_length - len(body))
        if not chunk:
            raise ConnectionError("socket closed while reading DAP body")
        body += chunk
    return json.loads(body)


def _write_dap_message(sock, body):
    """Write a DAP-framed JSON message to *sock*."""
    payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
    header = ("Content-Length: %d\r\n\r\n" % len(payload)).encode("utf-8")
    sock.sendall(header + payload)


def _pipe(src, dst):
    """Copy bytes from *src* to *dst* until EOF or error."""
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


_fake_pid_counter = 100000
_fake_pid_lock = threading.Lock()


def _next_fake_pid():
    global _fake_pid_counter
    with _fake_pid_lock:
        _fake_pid_counter += 1
        return _fake_pid_counter


def _handle_callback(conn, adapter_info):
    """Handle one callback from a remote task.

    Reads the task's endpoint, connects a bridge to the local adapter's
    internal pydevd server, completes the 2-message DAP handshake
    (pydevdAuthorize + pydevdSystemInfo), then forwards all traffic
    between the adapter and the remote task's pydevd.
    """
    try:
        # Read endpoint JSON from the task.
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        conn.close()
        endpoint = json.loads(data)
        task_host = endpoint["host"]
        task_port = int(endpoint["port"])

        # Connect bridge to adapter's internal pydevd server.
        bridge = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bridge.connect((adapter_info["host"], adapter_info["port"]))

        # DAP handshake — respond to pydevdAuthorize + pydevdSystemInfo.
        seq = 0

        def _next_seq():
            nonlocal seq
            seq += 1
            return seq

        msg = _read_dap_message(bridge)
        if msg.get("command") != "pydevdAuthorize":
            raise RuntimeError("expected pydevdAuthorize, got %s" % msg.get("command"))
        # adapter.access_token is always None when spawned via debugpy.listen().
        _write_dap_message(
            bridge,
            {
                "seq": _next_seq(),
                "type": "response",
                "request_seq": msg["seq"],
                "success": True,
                "command": "pydevdAuthorize",
                "body": {"clientAccessToken": None},
            },
        )

        msg = _read_dap_message(bridge)
        if msg.get("command") != "pydevdSystemInfo":
            raise RuntimeError("expected pydevdSystemInfo, got %s" % msg.get("command"))
        _write_dap_message(
            bridge,
            {
                "seq": _next_seq(),
                "type": "response",
                "request_seq": msg["seq"],
                "success": True,
                "command": "pydevdSystemInfo",
                "body": {
                    "python": {
                        "version": "3.11.0",
                        "implementation": {"name": "cpython", "version": "3.11.0"},
                    },
                    "platform": {"name": "linux"},
                    "process": {
                        "pid": _next_fake_pid(),
                        "ppid": adapter_info["parent_pid"],
                        "executable": "python",
                        "bitness": 64,
                    },
                },
            },
        )

        # Forward all subsequent traffic: adapter <-> remote task's pydevd.
        remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote.connect((task_host, task_port))
        threading.Thread(target=_pipe, args=(bridge, remote), daemon=True).start()
        threading.Thread(target=_pipe, args=(remote, bridge), daemon=True).start()

    except Exception as exc:
        _log("Bridge setup failed: %s", exc)


def _start_callback_server(adapter_info):
    """Start a TCP server that accepts callback connections from remote tasks.

    Returns the port of the listening server.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 0))
    server.listen(16)
    _, port = server.getsockname()

    def _accept_loop():
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                break
            threading.Thread(
                target=_handle_callback,
                args=(conn, adapter_info),
                daemon=True,
            ).start()

    threading.Thread(target=_accept_loop, daemon=True).start()
    return port


def _create_listen_socket(host, port):
    """Create a TCP server socket bound to *host*:*port*."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(1)
    return sock


class DebuggerStepDecorator(StepDecorator):
    """
    Step decorator that enables interactive debugging of Metaflow tasks via debugpy.

    **Local tasks** connect back to the runtime's debugpy adapter, triggering
    VSCode to auto-attach a new debug session for each task.

    **Remote tasks** (Kubernetes/Batch) start their own pydevd listener
    and call back to the runtime, which bridges the connection through the
    local adapter so VSCode auto-attaches seamlessly.

    Usage::

        python flow.py run --with debugger
        python flow.py run --with debugger:base_port=9000
        python flow.py run --with debugger:wait_for_client=False

    Or as a step annotation::

        @debugger
        @step
        def my_step(self):
            ...
    """

    name = "debugger"
    defaults = {
        "base_port": "5678",
        "wait_for_client": "True",
        "enabled": "True",
        "host": "auto",
    }

    _REMOTE_COMPUTE_DECORATORS = {"titus", "kubernetes", "batch"}

    _adapter_info = None
    _banner_printed = False
    _is_remote = False
    _callback_host = None
    _callback_port = None

    @property
    def _is_enabled(self):
        return self.attributes["enabled"].lower() == "true"

    @property
    def _wait_for_client(self):
        return self.attributes["wait_for_client"].lower() == "true"

    def _has_remote_compute(self, graph):
        return any(
            deco.name in self._REMOTE_COMPUTE_DECORATORS
            for node in graph
            for deco in node.decorators
        )

    @staticmethod
    def _get_routable_ip():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if not self._is_enabled:
            return
        try:
            import debugpy  # noqa: F401
        except ImportError:
            raise MetaflowException(
                "The @debugger decorator requires the 'debugpy' package. "
                "Install it with: pip install debugpy or, add it using @pypi or @conda"
            )

    def runtime_init(self, flow, graph, package, run_id):
        if not self._is_enabled:
            return

        if self.__class__._adapter_info is not None:
            self._print_banner()
            return

        import debugpy

        base_port = int(self.attributes["base_port"])
        host_attr = self.attributes["host"]

        is_remote = self._has_remote_compute(graph)
        self.__class__._is_remote = is_remote

        if host_attr == "auto":
            listen_host = "0.0.0.0" if is_remote else "127.0.0.1"
        else:
            listen_host = host_attr

        debugpy.listen((listen_host, base_port))

        if host_attr != "auto":
            connect_host = host_attr
        elif is_remote:
            connect_host = self._get_routable_ip()
        else:
            connect_host = "127.0.0.1"

        # Read adapter internal server info (available after debugpy.listen).
        from pydevd import SetupHolder

        setup = SetupHolder.setup
        if setup is None:
            raise MetaflowException(
                "Failed to initialize debugpy: could not read adapter info"
            )

        adapter_info = {
            "mode": "listen" if is_remote else "connect",
            "host": setup["client"],
            "port": int(setup["port"]),
            "parent_pid": os.getpid(),
            "connect_host": connect_host,
        }

        if is_remote:
            adapter_info["access_token"] = setup.get("access-token")
            cb_port = _start_callback_server(adapter_info)
            self.__class__._callback_host = connect_host
            self.__class__._callback_port = cb_port

        self.__class__._adapter_info = adapter_info
        self._print_banner()

        if self._wait_for_client:
            _log("Waiting for VSCode to attach on port %d ...", base_port)
            debugpy.wait_for_client()

    def _print_banner(self):
        if self.__class__._banner_printed:
            return
        self.__class__._banner_printed = True

        is_remote = self.__class__._is_remote
        base_port = int(self.attributes["base_port"])
        lines = [
            "",
            "=" * 60,
            "%s debugpy is enabled for this run." % _LOG_PREFIX,
        ]
        if is_remote:
            lines.extend(
                [
                    "%s Remote compute detected -- seamless bridge mode." % _LOG_PREFIX,
                    "%s Ensure debugpy is available in the container"
                    " (e.g. @pypi(packages={'debugpy': '...'}))." % _LOG_PREFIX,
                ]
            )
        else:
            lines.extend(
                [
                    "%s Attach VSCode to localhost:%d to start debugging."
                    % (_LOG_PREFIX, base_port),
                ]
            )
        if self._wait_for_client:
            lines.append(
                "%s Tasks will WAIT for a debugger client to attach." % _LOG_PREFIX
            )
        lines.append("=" * 60)
        sys.stderr.write("\n".join(lines) + "\n")

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if not self._is_enabled:
            return

        info = self.__class__._adapter_info
        if info is None:
            return

        mode = info["mode"]
        cli_args.env[_ENV_DEBUG_MODE] = mode

        if mode == "connect":
            cli_args.env[_ENV_ADAPTER_HOST] = str(info["connect_host"])
            cli_args.env[_ENV_ADAPTER_PORT] = str(info["port"])
            cli_args.env[_ENV_PARENT_PID] = str(info["parent_pid"])
        elif mode == "listen":
            cli_args.env[_ENV_BASE_PORT] = self.attributes["base_port"]
            cli_args.env[_ENV_CALLBACK_HOST] = str(self.__class__._callback_host)
            cli_args.env[_ENV_CALLBACK_PORT] = str(self.__class__._callback_port)
            if info.get("access_token"):
                cli_args.env[_ENV_ACCESS_TOKEN] = info["access_token"]

        if self._wait_for_client:
            cli_args.env[_ENV_WAIT_FOR_CLIENT] = "1"

        task = cli_args.task

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        if not self._is_enabled:
            return

        mode = os.environ.get(_ENV_DEBUG_MODE, "")
        if not mode:
            return

        wait = os.environ.get(_ENV_WAIT_FOR_CLIENT) == "1"
        task_key = "%s/%s/%s" % (flow.name, step_name, task_id)

        try:
            if mode == "listen":
                self._task_listen(task_key, wait)
            elif mode == "connect":
                self._task_connect(task_key, wait)
        except Exception as e:
            _log("%s debugger setup failed: %s", task_key, e)
            import traceback

            traceback.print_exc(file=sys.stderr)

    def _task_listen(self, task_key, wait):
        """Remote mode: start raw pydevd in server mode.

        We use pydevd directly (not debugpy.listen) because the local adapter
        connects through a bridge speaking the internal pydevd protocol.
        """
        base_port = int(os.environ[_ENV_BASE_PORT])

        import debugpy._vendored.force_pydevd  # noqa: F401
        import pydevd

        # Pre-create the listening socket before sending the callback to
        # eliminate the race where the runtime tries to connect before
        # pydevd.settrace() has bound the port.
        server_sock = _create_listen_socket("", base_port)

        # Report our endpoint back to the runtime.
        callback_host = os.environ[_ENV_CALLBACK_HOST]
        callback_port = int(os.environ[_ENV_CALLBACK_PORT])
        container_ip = socket.gethostbyname(socket.gethostname())
        payload = json.dumps({"host": container_ip, "port": base_port})
        cb = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cb.settimeout(30)
        cb.connect((callback_host, callback_port))
        cb.sendall(payload.encode("utf-8"))
        cb.shutdown(socket.SHUT_WR)
        cb.close()

        # Monkey-patch pydevd.start_server to reuse our pre-bound socket.
        # pydevd.py imports start_server at module level, so we must patch
        # the reference in the pydevd module, not in pydevd_comm.
        _orig = pydevd.start_server

        def _patched(port):
            conn, _ = server_sock.accept()
            server_sock.close()
            return conn

        pydevd.start_server = _patched
        try:
            kwargs = dict(
                host="",
                port=base_port,
                suspend=False,
                block_until_connected=True,
                wait_for_ready_to_run=wait,
                protocol="dap",
            )
            access_token = os.environ.get(_ENV_ACCESS_TOKEN)
            if access_token:
                kwargs["access_token"] = access_token
            pydevd.settrace(**kwargs)
        finally:
            pydevd.start_server = _orig

    @staticmethod
    def _task_connect(task_key, wait):
        """Local mode: connect back to the parent adapter."""
        import debugpy

        adapter_host = os.environ[_ENV_ADAPTER_HOST]
        adapter_port = int(os.environ[_ENV_ADAPTER_PORT])
        parent_pid = int(os.environ[_ENV_PARENT_PID])

        debugpy.connect(
            (adapter_host, adapter_port),
            parent_session_pid=parent_pid,
        )

        if wait:
            debugpy.wait_for_client()

    def runtime_finished(self, exception):
        self.__class__._adapter_info = None
        self.__class__._banner_printed = False
        self.__class__._is_remote = False
        self.__class__._callback_host = None
        self.__class__._callback_port = None
