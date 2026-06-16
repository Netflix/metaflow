import math
import time

from .mflog import refine, set_should_persist

from metaflow.util import to_unicode
from metaflow.exception import MetaflowInternalError

# Log source indicates the system that *minted the timestamp*
# for the logline. This means that for a single task we can
# assume that timestamps originating from the same source are
# monotonically increasing. Clocks are not synchronized between
# log sources, so if a file contains multiple log sources, the
# lines may not be in the ascending timestamp order.

# Note that a logfile prefixed with a log source, e.g. runtime,
# may contain lines from multiple sources below it (e.g. task).
#
# Note that these file names don't match to any previous log files
# (e.g. `0.stdout.log`). Older Metaflow versions will return None
# or an empty string when trying to access these new-style files.
# This is deliberate, so the users won't see partial files with older
# clients.
RUNTIME_LOG_SOURCE = "runtime"
TASK_LOG_SOURCE = "task"

# Loglines from all sources need to be merged together to
# produce a complete view of logs. Hence, keep this list short
# since each item takes a DataStore access.
LOG_SOURCES = [RUNTIME_LOG_SOURCE, TASK_LOG_SOURCE]

# BASH_MFLOG defines a bash function that outputs valid mflog
# structured loglines. We use this to output properly timestamped
# loglined prior to Metaflow package has been downloaded.
# Note that MFLOG_STDOUT is defined by mflog_export_env_vars() function.
BASH_MFLOG = (
    "mflog(){ "
    "T=$(date -u -Ins|tr , .); "
    'echo \\"[MFLOG|0|${T:0:26}Z|%s|$T]$1\\"'
    " >> $MFLOG_STDOUT; echo $1; "
    " }" % TASK_LOG_SOURCE
)

BASH_SAVE_LOGS_ARGS = ["python", "-m", "metaflow.mflog.save_logs"]
BASH_SAVE_LOGS = " ".join(BASH_SAVE_LOGS_ARGS)

BASH_FLUSH_LOGS = "flush_mflogs(){ " f"{BASH_SAVE_LOGS}; " "}"


# this function returns a bash expression that redirects stdout
# and stderr of the given bash expression to mflog.tee
def bash_capture_logs(bash_expr, var_transform=None):
    if var_transform is None:
        var_transform = lambda s: "$%s" % s

    cmd = "python -m metaflow.mflog.tee %s %s"
    parts = (
        bash_expr,
        cmd % (TASK_LOG_SOURCE, var_transform("MFLOG_STDOUT")),
        cmd % (TASK_LOG_SOURCE, var_transform("MFLOG_STDERR")),
    )
    return "(%s) 1>> >(%s) 2>> >(%s >&2)" % parts


# update_delay determines how often logs should be uploaded to S3
# as a function of the task execution time

MIN_UPDATE_DELAY = 0.25  # the most frequent update interval
MAX_UPDATE_DELAY = 30.0  # the least frequent update interval


def update_delay(secs_since_start):
    # this sigmoid function reaches
    # - 0.1 after 11 minutes
    # - 0.5 after 15 minutes
    # - 1.0 after 23 minutes
    # in other words, the user will see very frequent updates
    # during the first 10 minutes
    sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
    return MIN_UPDATE_DELAY + sigmoid * MAX_UPDATE_DELAY


# this function is used to generate a Bash 'export' expression that
# sets environment variables that are used by 'tee' and 'save_logs'.
# Note that we can't set the env vars statically, as some of them
# may need to be evaluated during runtime
def export_mflog_env_vars(
    flow_name=None,
    run_id=None,
    step_name=None,
    task_id=None,
    retry_count=None,
    datastore_type=None,
    datastore_root=None,
    stdout_path=None,
    stderr_path=None,
):
    pathspec = "/".join((flow_name, str(run_id), step_name, str(task_id)))
    env_vars = {
        "PYTHONUNBUFFERED": "x",
        "MF_PATHSPEC": pathspec,
        "MF_DATASTORE": datastore_type,
        "MF_ATTEMPT": retry_count,
        "MFLOG_STDOUT": stdout_path,
        "MFLOG_STDERR": stderr_path,
    }
    if datastore_root is not None:
        env_vars["MF_DATASTORE_ROOT"] = datastore_root

    return "export " + " ".join("%s=%s" % kv for kv in env_vars.items())


def tail_logs(prefix, stdout_tail, stderr_tail, echo, has_log_updates):
    def _available_logs(tail, stream, echo, should_persist=False):
        try:
            for line in tail:
                if should_persist:
                    line = set_should_persist(line)
                else:
                    line = refine(line, prefix=prefix)
                echo(
                    line.strip().decode("utf-8", errors="replace"), stream, no_bold=True
                )
        except Exception as ex:
            echo(
                "%s[ temporary error in fetching logs: %s ]" % (to_unicode(prefix), ex),
                "stderr",
            )

    start_time = time.time()
    next_log_update = start_time
    log_update_delay = update_delay(0)
    while has_log_updates():
        if time.time() > next_log_update:
            _available_logs(stdout_tail, "stdout", echo)
            _available_logs(stderr_tail, "stderr", echo)
            now = time.time()
            log_update_delay = update_delay(now - start_time)
            next_log_update = now + log_update_delay

        # This sleep should never delay log updates. On the other hand,
        # we should exit this loop when the task has finished without
        # a long delay, regardless of the log tailing schedule
        time.sleep(min(log_update_delay, 5.0))
    # It is possible that we exit the loop above before all logs have been
    # tailed.
    _available_logs(stdout_tail, "stdout", echo)
    _available_logs(stderr_tail, "stderr", echo)


def get_log_tailer(log_url, datastore_type):
    if datastore_type == "s3":
        from metaflow.plugins.datatools.s3.s3tail import S3Tail

        return S3Tail(log_url)
    elif datastore_type == "azure":
        from metaflow.plugins.azure.azure_tail import AzureTail

        return AzureTail(log_url)
    elif datastore_type == "gs":
        from metaflow.plugins.gcp.gs_tail import GSTail

        return GSTail(log_url)
    else:
        raise MetaflowInternalError(
            "Log tailing implementation missing for datastore type %s"
            % (datastore_type,)
        )
