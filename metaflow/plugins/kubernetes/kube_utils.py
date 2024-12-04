import re
from typing import Dict, List, Optional
from metaflow.exception import CommandException
from .kubernetes import KubernetesException
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


def validate_kube_labels_or_annotations(
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
