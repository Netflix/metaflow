try:
    from armada_client.log_client import JobLogClient
except ModuleNotFoundError:
    pass


def logs(
    host,
    port,
    job_id,
    since_time,
    use_ssl=True,
):
    log_client = JobLogClient(f"{host}:{port}", job_id, not use_ssl)

    return log_client.logs(since_time)


def log_thread(
    host,
    port,
    job_id,
    done_signal,
    log_func,
    use_ssl=True,
):
    log_client = JobLogClient(f"{host}:{port}", job_id, not use_ssl)
    since_time = ""

    log_lines_cache = {}

    log_func(f"Log begins for job '{job_id}'")
    while True:
        log_lines = log_client.logs(since_time)
        for line in log_lines:
            if line.timestamp not in log_lines_cache:
                log_lines_cache[line.timestamp] = line.line
                log_func(f"{line.timestamp} {line.line}")

        if done_signal.wait(timeout=1.0):
            log_func(f"Log ends for job '{job_id}'")
            return

        if log_lines is not None and len(log_lines) > 0:
            since_time = log_lines[-1].timestamp
        else:
            since_time = ""
