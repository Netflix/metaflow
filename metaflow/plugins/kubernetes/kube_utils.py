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


def qos_requests_and_limits(qos: str, cpu: int, memory: int, storage: int):
    "return resource requests and limits for the kubernetes pod based on the given QoS Class"
    # case insensitive matching for QoS class
    qos = qos.lower()
    # Determine the requests and limits to define chosen QoS class
    qos_limits = {}
    qos_requests = {}
    if qos == "guaranteed":
        # Guaranteed - has both cpu/memory limits. requests not required, as these will be inferred.
        qos_limits = {
            "cpu": str(cpu),
            "memory": "%sM" % str(memory),
            "ephemeral-storage": "%sM" % str(storage),
        }
        # NOTE: Even though Kubernetes will produce matching requests for the specified limits, this happens late in the lifecycle.
        # We specify them explicitly here to make some K8S tooling happy, in case they rely on .resources.requests being present at time of submitting the job.
        qos_requests = qos_limits
    else:
        # Burstable - not Guaranteed, and has a memory/cpu limit or request
        qos_requests = {
            "cpu": str(cpu),
            "memory": "%sM" % str(memory),
            "ephemeral-storage": "%sM" % str(storage),
        }
    # TODO: Add support for BestEffort once there is a use case for it.
    # BestEffort - no limit or requests for cpu/memory
    return qos_requests, qos_limits
