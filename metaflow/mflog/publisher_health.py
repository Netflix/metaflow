import json
import os
import time
from datetime import datetime


HEALTH_ENABLED_ENV_VAR = "METAFLOW_DEBUG_LOG_PUBLISHER"
HEALTH_PATH_ENV_VAR = "METAFLOW_LOG_PUBLISHER_HEALTH_PATH"
HEALTH_INTERVAL_ENV_VAR = "METAFLOW_LOG_PUBLISHER_HEALTH_INTERVAL_SECONDS"
DEFAULT_HEALTH_FILENAME = "metaflow_log_publisher_health.jsonl"
DEFAULT_HEALTH_INTERVAL_SECONDS = 30.0
PROCESS_POLL_INTERVAL_SECONDS = 1.0


def health_logging_enabled():
    return os.environ.get(HEALTH_ENABLED_ENV_VAR, "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def health_interval_seconds():
    try:
        return max(
            0.1,
            float(
                os.environ.get(HEALTH_INTERVAL_ENV_VAR, DEFAULT_HEALTH_INTERVAL_SECONDS)
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_HEALTH_INTERVAL_SECONDS


def health_log_path():
    configured_path = os.environ.get(HEALTH_PATH_ENV_VAR)
    if configured_path:
        return configured_path

    task_stderr = os.environ.get("MFLOG_STDERR")
    if task_stderr:
        return os.path.join(os.path.dirname(task_stderr), DEFAULT_HEALTH_FILENAME)
    return os.path.join("/tmp", DEFAULT_HEALTH_FILENAME)


def last_event_age(event_time, now=None):
    if event_time is None:
        return None
    return max(0, (now or time.time()) - event_time)


def write_health_event(event, **fields):
    if not health_logging_enabled():
        return

    record = {
        "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "event": event,
        "pathspec": os.environ.get("MF_PATHSPEC"),
        "attempt": os.environ.get("MF_ATTEMPT"),
        "pid": os.getpid(),
    }
    record.update(fields)

    try:
        path = health_log_path()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "a", encoding="utf-8") as health_log:
            health_log.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    except Exception:
        # Health reporting must never affect task execution or log publishing.
        pass
