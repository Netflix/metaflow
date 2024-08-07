import json
import os
from datetime import datetime

###
# Algorithm to determine 1st error:
#   ignore the failures where message = ""
#   group the failures via templateName
#     sort each group by finishedAt
#   find the group for which the last finishedAt is earliest
#   if the earliest message is "No more retries left" then
#     get the n-1th message from that group
#   else
#     return the last message.
###


def parse_workflow_failures():
    failures = json.loads(
        json.loads(os.getenv("METAFLOW_ARGO_WORKFLOW_FAILURES", "[]"), strict=False),
        strict=False,
    )
    return [wf for wf in failures if wf.get("message")]


def group_failures_by_template(failures):
    groups = {}
    for failure in failures:
        groups.setdefault(failure["templateName"], []).append(failure)
    return groups


def sort_by_finished_at(items):
    return sorted(
        items, key=lambda x: datetime.strptime(x["finishedAt"], "%Y-%m-%dT%H:%M:%SZ")
    )


def find_earliest_last_finished_group(groups):
    return min(
        groups,
        key=lambda k: datetime.strptime(
            groups[k][-1]["finishedAt"], "%Y-%m-%dT%H:%M:%SZ"
        ),
    )


def determine_first_error():
    failures = parse_workflow_failures()
    if not failures:
        return None

    grouped_failures = group_failures_by_template(failures)
    for group in grouped_failures.values():
        group.sort(
            key=lambda x: datetime.strptime(x["finishedAt"], "%Y-%m-%dT%H:%M:%SZ")
        )

    earliest_group = grouped_failures[
        find_earliest_last_finished_group(grouped_failures)
    ]

    if earliest_group[-1]["message"] == "No more retries left":
        return earliest_group[-2]
    return earliest_group[-1]


if __name__ == "__main__":
    first_err = determine_first_error()
    print(json.dumps(first_err, indent=2))
