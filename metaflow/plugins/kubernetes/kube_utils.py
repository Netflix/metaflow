import re
from typing import Dict, List, Optional
from metaflow.exception import CommandException, MetaflowException
from metaflow.util import get_username, get_latest_run_id


# avoid circular import by having the exception class contained here
class KubernetesException(MetaflowException):
    headline = "Kubernetes error"


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


def validate_kube_labels(
    labels: Optional[Dict[str, Optional[str]]],
) -> bool:
    """Validate label values.

    This validates the kubernetes label values.  It does not validate the keys.
    Ideally, keys should be static and also the validation rules for keys are
    more complex than those for values.  For full validation rules, see:

    https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
    """

    def validate_label(s: Optional[str]):
        regex_match = r"^(([A-Za-z0-9][-A-Za-z0-9_.]{0,61})?[A-Za-z0-9])?$"
        if not s:
            # allow empty label
            return True
        if not re.search(regex_match, s):
            raise KubernetesException(
                'Invalid value: "%s"\n'
                "A valid label must be an empty string or one that\n"
                "  - Consist of alphanumeric, '-', '_' or '.' characters\n"
                "  - Begins and ends with an alphanumeric character\n"
                "  - Is at most 63 characters" % s
            )
        return True

    return all([validate_label(v) for v in labels.values()]) if labels else True


def parse_kube_keyvalue_list(items: List[str], requires_both: bool = True):
    try:
        ret = {}
        for item_str in items:
            item = item_str.split("=", 1)
            if requires_both:
                item[1]  # raise IndexError
            if str(item[0]) in ret:
                raise KubernetesException("Duplicate key found: %s" % str(item[0]))
            ret[str(item[0])] = str(item[1]) if len(item) > 1 else None
        return ret
    except KubernetesException as e:
        raise e
    except (AttributeError, IndexError):
        raise KubernetesException("Unable to parse kubernetes list: %s" % items)
