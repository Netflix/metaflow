import datetime
import time

try:
    from armada_client.binoculars_client import BinocularsClient
except ModuleNotFoundError:
    print("Binoculars client not available!")

import grpc


def _get_client(host, port, use_ssl=True):
    grpc_target = f"{host}:{port}"
    if use_ssl:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(grpc_target, channel_credentials)
    else:
        channel = grpc.insecure_channel(grpc_target)

    return BinocularsClient(channel)


def logs(
    host,
    port,
    job_id,
    pod_namespace,
    since_time,
    pod_number=0,
    log_options=None,
    use_ssl=True,
):
    bino_client = _get_client(host, port, use_ssl)

    return bino_client.logs(job_id, pod_namespace, since_time, pod_number, log_options)


def cordon(host, port, node_name, use_ssl=True):
    bino_client = _get_client(host, port, use_ssl)

    return bino_client.cordon(node_name)


def log_thread(
    host,
    port,
    job_id,
    pod_namespace,
    done_signal,
    log_func,
    pod_number=0,
    log_options=None,
    use_ssl=True,
):
    bino_client = _get_client(host, port, use_ssl)
    since_time = ""

    log_lines = {}

    log_func(f"Log begins for job '{job_id}'")
    while True:
        log_response = bino_client.logs(job_id, pod_namespace, since_time)
        for line in log_response.log:
            if line.timestamp not in log_lines:
                log_lines[line.timestamp] = line.line
                log_func(f"{line.timestamp} {line.line}")

        if done_signal.wait(timeout=1.0):
            log_func("Log ends for job '{job_id}'")
            return

        if log_response is not None and len(log_response.log) > 0:
            since_time = log_response.log[-1].timestamp
        else:
            since_time = ""
