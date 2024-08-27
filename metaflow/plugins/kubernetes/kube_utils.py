from metaflow.exception import CommandException
from metaflow.util import get_username, get_latest_run_id


def parse_cli_options(flow_name, run_id, user, my_runs, echo):
    if user and my_runs:
        raise CommandException("--user and --my-runs are mutually exclusive.")

    if run_id and my_runs:
        raise CommandException("--run_id and --my-runs are mutually exclusive.")

    if my_runs:
        user = get_username()

    latest_run = True

    if user and not run_id:
        latest_run = False

    if not run_id and latest_run:
        run_id = get_latest_run_id(echo, flow_name)
        if run_id is None:
            raise CommandException("A previous run id was not found. Specify --run-id.")

    return flow_name, run_id, user
