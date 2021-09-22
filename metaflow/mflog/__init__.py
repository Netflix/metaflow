import math

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
RUNTIME_LOG_SOURCE = 'runtime'
TASK_LOG_SOURCE = 'task'

# Loglines from all sources need to be merged together to
# produce a complete view of logs. Hence keep this list short
# since every items takes a DataStore access.
LOG_SOURCES = [
    RUNTIME_LOG_SOURCE,
    TASK_LOG_SOURCE
]

# BASH_MFLOG defines a bash function that outputs valid mflog
# structured loglines. We use this to output properly timestamped
# loglined prior to Metaflow package has been downloaded.
# Note that MFLOG_STDOUT is defined by mflog_export_env_vars() function.
BASH_MFLOG =\
    'mflog(){ '\
        'T=$(date -u -Ins|tr , .); '\
        'echo \\"[MFLOG|0|${T:0:26}Z|%s|$T]$1\\"'\
            ' >> $MFLOG_STDOUT; echo $1; '\
     ' }' % TASK_LOG_SOURCE

BASH_SAVE_LOGS_ARGS = ['python', '-m', 'metaflow.mflog.save_logs']
BASH_SAVE_LOGS = ' '.join(BASH_SAVE_LOGS_ARGS)

# this function returns a bash expression that redirects stdout
# and stderr of the given bash expression to mflog.tee
def bash_capture_logs(bash_expr):
    cmd = 'python -m metaflow.mflog.tee %s %s'
    parts = (bash_expr,
             cmd % (TASK_LOG_SOURCE, '$MFLOG_STDOUT'),
             cmd % (TASK_LOG_SOURCE, '$MFLOG_STDERR'))
    return '(%s) 1>> >(%s) 2>> >(%s >&2)' % parts

# update_delay determines how often logs should be uploaded to S3
# as a function of the task execution time

MIN_UPDATE_DELAY = 1.  # the most frequent update interval
MAX_UPDATE_DELAY = 30. # the least frequent update interval

def update_delay(secs_since_start):
    # this sigmoid function reaches
    # - 0.1 after 11 minutes
    # - 0.5 after 15 minutes
    # - 1.0 after 23 minutes
    # in other words, the user will see very frequent updates
    # during the first 10 minutes
    sigmoid = 1. / (1. + math.exp(-0.01 * secs_since_start + 9.))
    return MIN_UPDATE_DELAY + sigmoid * MAX_UPDATE_DELAY

# this function is used to generate a Bash 'export' expression that
# sets environment variables that are used by 'tee' and 'save_logs'.
# Note that we can't set the env vars statically, as some of them
# may need to be evaluated during runtime
def export_mflog_env_vars(flow_name=None,
                          run_id=None,
                          step_name=None,
                          task_id=None,
                          retry_count=None,
                          datastore_type=None,
                          datastore_root=None,
                          stdout_path=None,
                          stderr_path=None):

    pathspec = '/'.join((flow_name, str(run_id), step_name, str(task_id)))
    env_vars = {
        'PYTHONUNBUFFERED': 'x',
        'MF_PATHSPEC': pathspec,
        'MF_DATASTORE': datastore_type,
        'MF_ATTEMPT': retry_count,
        'MFLOG_STDOUT': stdout_path,
        'MFLOG_STDERR': stderr_path
    }
    if datastore_root is not None:
        env_vars['MF_DATASTORE_ROOT'] = datastore_root

    return 'export ' + ' '.join('%s=%s' % kv for kv in env_vars.items())